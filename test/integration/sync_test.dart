import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/core/multiaddr.dart';
import 'package:dart_libp2p/core/network/context.dart';
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/core/peer/addr_info.dart';
import 'package:dart_libp2p/core/peer/peer_id.dart';
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;
import 'package:dart_libp2p_kad_dht/dart_libp2p_kad_dht.dart' show IpfsDHTv2;
import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:dart_udx/dart_udx.dart' show UDX;
import 'package:dcid/dcid.dart' show CID;
import 'package:merkledag/merkledag.dart';
import 'package:test/test.dart';

import '../real_net_stack.dart' show Libp2pNode, createLibp2pNode;

const _protocol = defaultFetchProtocolId;
const _instance = 'sync-test';

typedef _Manager = MerkleCrdtGossipManager<Set<String>, GSet<String>>;

class _Net {
  final Libp2pNode node;
  final PubSub pubsub;
  final GossipSubRouter router;
  _Net(this.node, this.pubsub, this.router);

  PeerId get id => node.peerId;
  AddrInfo get addrInfo => AddrInfo(
      id,
      node.host.addrs
          .map((a) => MultiAddr(a.toString().replaceFirst('/ip4/0.0.0.0/', '/ip4/127.0.0.1/')))
          .toList());
}

class _Replica {
  final _Net net;
  final InMemoryBlockStore<GSet<String>, Set<String>> store;
  final P2PStreamDagFetcher<GSet<String>, Set<String>> fetcher;
  final GossipSubAnnouncer announcer;
  final _Manager manager;
  _Replica(this.net, this.store, this.fetcher, this.announcer, this.manager);

  Future<Set<String>> state() async => await manager.getState() ?? {};
}

void main() {
  final nets = <_Net>[];
  final replicas = <_Replica>[];
  final dhts = <IpfsDHTv2>[];

  Future<_Net> net() async {
    final node = await createLibp2pNode(
      udxInstance: UDX(),
      resourceManager: NullResourceManager(),
      connManager: p2p_conn_mgr.ConnectionManager(),
      hostEventBus: p2p_event_bus.BasicBus(),
    );
    final router = GossipSubRouter();
    final pubsub = PubSub(node.host, router);
    await pubsub.start();
    final n = _Net(node, pubsub, router);
    nets.add(n);
    return n;
  }

  _Replica replica(_Net n, WriterAuthorizer writers, {IpfsDHTv2? dht}) {
    final store = InMemoryBlockStore<GSet<String>, Set<String>>();
    final fetcher = P2PStreamDagFetcher<GSet<String>, Set<String>>(
      n.node.host, store, _protocol, GSet<String>.fromCanonicalBytes,
      dht: dht, fetchTimeout: const Duration(seconds: 3),
    );
    final announcer = GossipSubAnnouncer(n.node.host, n.pubsub, _instance, writers: writers);
    final manager = _Manager(
      dagFetcher: fetcher,
      announcer: announcer,
      retryDelay: const Duration(milliseconds: 200),
    );
    final r = _Replica(n, store, fetcher, announcer, manager);
    replicas.add(r);
    return r;
  }

  Future<void> until(FutureOr<bool> Function() condition, String what,
      {Duration timeout = const Duration(seconds: 20)}) async {
    final deadline = DateTime.now().add(timeout);
    while (!await condition()) {
      if (DateTime.now().isAfter(deadline)) fail('Timed out waiting for $what');
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
  }

  /// Connects every pair and waits for the GossipSub mesh to form.
  Future<void> mesh(List<_Replica> rs) async {
    for (var i = 0; i < rs.length; i++) {
      for (var j = i + 1; j < rs.length; j++) {
        await rs[i].net.node.host.connect(rs[j].net.addrInfo);
      }
    }
    final topic = rs.first.announcer.topic;
    for (final r in rs) {
      await until(
          () => rs.every((o) =>
              o == r || (r.net.router.mesh[topic]?.contains(o.net.id) ?? false)),
          'GossipSub mesh');
    }
  }

  tearDown(() async {
    for (final r in replicas) {
      await r.manager.close();
    }
    for (final d in dhts) {
      await d.close();
    }
    for (final n in nets) {
      await n.pubsub.stop();
      await n.node.host.close();
    }
    replicas.clear();
    dhts.clear();
    nets.clear();
  });

  test('replicates both ways without a DHT, and concurrent writes converge', () async {
    final na = await net(), nb = await net();
    final writers = WriterAuthorizer.only([na.id, nb.id]);
    final a = replica(na, writers), b = replica(nb, writers);
    await mesh([a, b]);

    await a.manager.applyLocalUpdate(GSet<String>.fromList(['from-a']));
    await until(() async => (await b.state()).contains('from-a'), 'B to get A\'s update');

    // Concurrent: both write before seeing the other's update.
    await Future.wait([
      a.manager.applyLocalUpdate(GSet<String>.fromList(['a2'])),
      b.manager.applyLocalUpdate(GSet<String>.fromList(['b2'])),
    ]);
    const all = {'from-a', 'a2', 'b2'};
    await until(() async => (await a.state()).containsAll(all), 'A to converge');
    await until(() async => (await b.state()).containsAll(all), 'B to converge');
  });

  test('a long history replicates block by block from the announcer', () async {
    final na = await net(), nb = await net();
    final writers = WriterAuthorizer.only([na.id, nb.id]);
    final a = replica(na, writers);
    for (var i = 0; i < 30; i++) {
      await a.manager.applyLocalUpdate(GSet<String>.fromList(['v$i']));
    }
    // B joins late and only ever hears the latest head.
    final b = replica(nb, writers);
    await mesh([a, b]);
    await a.manager.forceAnnounceCurrentHead();

    await until(() async => (await b.state()).length == 30, 'B to catch up');
    expect(b.manager.heads, a.manager.heads);
  });

  test('a block larger than one yamux frame replicates', () async {
    final na = await net(), nb = await net();
    final writers = WriterAuthorizer.only([na.id, nb.id]);
    final a = replica(na, writers), b = replica(nb, writers);
    await mesh([a, b]);

    final big = GSet<String>.fromList(
        [for (var i = 0; i < 20000; i++) 'element-${i.toString().padLeft(6, '0')}-${'x' * 32}']);
    expect(big.toCanonicalBytes().length, greaterThan(800 * 1024));
    await a.manager.applyLocalUpdate(big);

    await until(() async => (await b.state()).length == 20000, 'B to receive the large block');
  });

  test('heads from writers outside the allowlist are neither merged nor relayed', () async {
    final na = await net(), nb = await net(), nc = await net();
    // B only hears C through A: connect B-A and A-C, not B-C.
    final writers = WriterAuthorizer.only([na.id, nb.id]);
    final a = replica(na, writers), b = replica(nb, writers);
    final c = replica(nc, WriterAuthorizer.only([na.id, nb.id, nc.id]));
    await mesh([a, b]);
    await nc.node.host.connect(na.addrInfo);
    await until(() => nc.router.mesh[c.announcer.topic]?.contains(na.id) ?? false, 'C in mesh');

    final intruderHead = await c.manager.applyLocalUpdate(GSet<String>.fromList(['intruder']));
    await a.manager.applyLocalUpdate(GSet<String>.fromList(['allowed']));

    await until(() async => (await b.state()).contains('allowed'), 'B to get A\'s update');
    expect(await a.state(), isNot(contains('intruder')));
    expect(await b.state(), isNot(contains('intruder')));
    expect(await a.store.has(intruderHead), isFalse);
    expect(await b.store.has(intruderHead), isFalse);
  });

  test('a delete on one peer sticks on every peer, even after re-announcement', () async {
    final na = await net(), nb = await net();
    final writers = WriterAuthorizer.only([na.id, nb.id]);
    MerkleCrdtGossipManager<Map<String, dynamic>, DocumentCRDT> docs(_Net n) {
      final manager = MerkleCrdtGossipManager<Map<String, dynamic>, DocumentCRDT>(
        dagFetcher: P2PStreamDagFetcher<DocumentCRDT, Map<String, dynamic>>(
            n.node.host, InMemoryBlockStore(), _protocol, DocumentCRDT.fromBytes),
        announcer: GossipSubAnnouncer(n.node.host, n.pubsub, 'docs', writers: writers),
      );
      addTearDown(manager.close);
      return manager;
    }

    final a = docs(na), b = docs(nb);
    await na.node.host.connect(nb.addrInfo);
    await until(() => na.router.mesh['merkle-crdt/heads/2.0.0/docs']?.contains(nb.id) ?? false,
        'mesh');
    await until(() => nb.router.mesh['merkle-crdt/heads/2.0.0/docs']?.contains(na.id) ?? false,
        'mesh');

    await a.applyLocalUpdate(DocumentCRDT.empty().put('k', 'v', actorId: 'a'));
    await until(() async => (await b.getState())?['k'] == 'v', 'B to see k');

    await b.applyLocalUpdate((await b.getPayload())!.delete('k', actorId: 'b'));
    await until(() async => !((await a.getState()) ?? {}).containsKey('k'), 'A to see the delete');

    // A re-announces its heads; the delete still holds on both.
    await a.forceAnnounceCurrentHead();
    await b.forceAnnounceCurrentHead();
    await until(() async => _sameHeads(a.heads, b.heads), 'heads to agree');
    expect((await a.getState())!.containsKey('k'), isFalse);
    expect((await b.getState())!.containsKey('k'), isFalse);
  });

  test('close() stops serving blocks and processing announcements', () async {
    final na = await net(), nb = await net();
    final writers = WriterAuthorizer.only([na.id, nb.id]);
    final a = replica(na, writers), b = replica(nb, writers);
    await mesh([a, b]);
    final head = await a.manager.applyLocalUpdate(GSet<String>.fromList(['x']));
    await until(() async => (await b.state()).contains('x'), 'B to sync');

    await a.manager.close();
    replicas.remove(a);

    // A no longer serves the block it holds.
    expect(await a.store.has(head), isTrue);
    expect(await b.fetcher.nodeProtocol.fetch(head, na.id), isNull);

    // A ignores later announcements.
    await b.manager.applyLocalUpdate(GSet<String>.fromList(['after-close']));
    await until(() async => b.manager.heads.length == 1, 'B head');
    expect(await a.store.has(b.manager.heads.single), isFalse);
  });

  group('serving peer misbehaves', () {
    late _Net honest, bad;
    late _Replica victim;
    late MerkleNode<GSet<String>> real;

    setUp(() async {
      honest = await net();
      bad = await net();
      victim = replica(honest, WriterAuthorizer.only([honest.id]));
      await honest.node.host.connect(bad.addrInfo);
      real = MerkleNode.create(GSet<String>.fromList(['real']), <CID>{});
    });

    void serve(Future<void> Function(Uint8List request, dynamic stream) respond) {
      bad.node.host.setStreamHandler(_protocol, (stream, peer) async {
        final request = await FrameReader(stream).readFrame(maxLength: 1024);
        await respond(request, stream);
      });
    }

    test('forged content under a real CID is rejected and not stored', () async {
      serve((request, stream) async {
        final forged = MerkleNode(
            cid: real.cid, payload: GSet<String>.fromList(['forged']), children: <CID>{});
        await stream.write(encodeFrame(Uint8List.fromList([0x00, ...forged.toBytes()])));
        await stream.close();
      });
      victim.fetcher.notePeer(bad.id);
      expect(await victim.fetcher.get(real.cid), isNull);
      expect(await victim.store.has(real.cid), isFalse);
    });

    test('an oversized frame is rejected without reading it', () async {
      serve((request, stream) async {
        // Declares a 64 MiB frame.
        await stream.write(Uint8List.fromList([0x80, 0x80, 0x80, 0x20]));
      });
      final started = DateTime.now();
      expect(await victim.fetcher.nodeProtocol.fetch(real.cid, bad.id), isNull);
      expect(DateTime.now().difference(started), lessThan(const Duration(seconds: 2)));
    });

    test('a peer that never answers times out', () async {
      serve((request, stream) => Completer<void>().future);
      final started = DateTime.now();
      expect(await victim.fetcher.nodeProtocol.fetch(real.cid, bad.id), isNull);
      expect(DateTime.now().difference(started), lessThan(const Duration(seconds: 5)));
    });

    test('a client that never sends a request is dropped by the server', () async {
      await victim.store.put(real);
      final stream = await bad.node.host.newStream(honest.id, [_protocol], Context());
      // The server gives up after its timeout (3s here) and resets the stream.
      Uint8List? response;
      try {
        response = await stream.read().timeout(const Duration(seconds: 15));
      } on TimeoutException {
        fail('Server kept an idle stream open');
      } catch (_) {
        response = Uint8List(0); // Reset.
      }
      expect(response, isEmpty);
    });
  });
}

bool _sameHeads(Set<CID> a, Set<CID> b) => a.length == b.length && a.containsAll(b);
