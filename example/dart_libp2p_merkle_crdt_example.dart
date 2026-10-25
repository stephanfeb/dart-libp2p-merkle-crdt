// Two peers on this machine replicate a grow-only set.
//
//   dart run example/dart_libp2p_merkle_crdt_example.dart
import 'dart:async';

import 'package:dart_libp2p/config/config.dart' as p2p_config;
import 'package:dart_libp2p/core/crypto/ed25519.dart' as ed25519;
import 'package:dart_libp2p/core/host/host.dart';
import 'package:dart_libp2p/core/multiaddr.dart';
import 'package:dart_libp2p/core/peer/addr_info.dart';
import 'package:dart_libp2p/p2p/security/noise/noise_protocol.dart';
import 'package:dart_libp2p/p2p/transport/connection_manager.dart';
import 'package:dart_libp2p/p2p/transport/udx_transport.dart';
import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:dart_udx/dart_udx.dart';
import 'package:merkledag/merkledag.dart';

typedef Replica = MerkleCrdtGossipManager<Set<String>, GSet<String>>;

Future<Host> createHost() async {
  final keyPair = await ed25519.generateEd25519KeyPair();
  final connManager = ConnectionManager();
  final host = await p2p_config.Libp2p.new_([
    p2p_config.Libp2p.identity(keyPair),
    p2p_config.Libp2p.connManager(connManager),
    p2p_config.Libp2p.transport(UDXTransport(connManager: connManager, udxInstance: UDX())),
    p2p_config.Libp2p.security(await NoiseSecurity.create(keyPair)),
    p2p_config.Libp2p.listenAddrs([MultiAddr('/ip4/0.0.0.0/udp/0/udx')]),
  ]);
  await host.start();
  return host;
}

Replica createReplica(Host host, PubSub pubsub, WriterAuthorizer writers) {
  final fetcher = P2PStreamDagFetcher<GSet<String>, Set<String>>(
    host,
    InMemoryBlockStore(),
    defaultFetchProtocolId,
    GSet<String>.fromCanonicalBytes,
    // Pass `dht:` to also find blocks through DHT providers.
  );
  final announcer = GossipSubAnnouncer(host, pubsub, 'example-set', writers: writers);
  return Replica(dagFetcher: fetcher, announcer: announcer);
}

Future<void> main() async {
  final hostA = await createHost();
  final hostB = await createHost();
  final pubsubA = PubSub(hostA, GossipSubRouter());
  final pubsubB = PubSub(hostB, GossipSubRouter());
  await pubsubA.start();
  await pubsubB.start();

  // Only these two peers may write.
  final writers = WriterAuthorizer.only([hostA.id, hostB.id]);
  final a = createReplica(hostA, pubsubA, writers);
  final b = createReplica(hostB, pubsubB, writers);

  print('A listens on ${hostA.addrs}');
  await hostB.connect(AddrInfo(hostA.id, hostA.addrs));

  final received = Completer<void>();
  b.onStateChanged.listen((state) {
    print('B sees: $state');
    if (state != null && state.containsAll({'apple', 'pear'}) && !received.isCompleted) {
      received.complete();
    }
  });

  // Give GossipSub a moment to form its mesh before writing. GossipSub does
  // not replay missed messages; forceAnnounceCurrentHead() repeats the heads.
  await Future<void>.delayed(const Duration(seconds: 2));
  await a.applyLocalUpdate(GSet<String>.fromList(['apple']));
  await a.applyLocalUpdate(GSet<String>.fromList(['pear']));
  while (!received.isCompleted) {
    await received.future.timeout(const Duration(seconds: 5), onTimeout: a.forceAnnounceCurrentHead);
  }

  print('A heads: ${a.heads}');
  print('B heads: ${b.heads}');

  await a.close();
  await b.close();
  await pubsubA.stop();
  await pubsubB.stop();
  await hostA.close();
  await hostB.close();
}
