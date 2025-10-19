import 'dart:async';

import 'package:dcid/dcid.dart' show CID;
import 'package:dart_libp2p/dart_libp2p.dart';
import 'package:dart_libp2p/p2p/host/basic/basic_host.dart' show BasicHost;
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:dart_libp2p_kad_dht/dart_libp2p_kad_dht.dart' show DHTMode, DHTOptions, IpfsDHT, IpfsDHTv2;
import 'package:dart_libp2p_kad_dht/src/providers/provider_store.dart' show MemoryProviderStore;
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_udx/dart_udx.dart' show UDX;

import 'package:logging/logging.dart';
import 'package:test/test.dart';

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/p2p.dart'; // For Actor
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/liked_crdt.dart'; // Import LikedCrdtPayload

import '../../real_net_stack.dart' show Libp2pNode, createLibp2pNode;
import '../../test_utils.dart';

final _logger = Logger('LikedSyncTest');

class _LikedTestNode {
  late Libp2pNode nodeDetails;
  late Host host;
  late PeerId peerId;
  late PubSub pubsub;
  late IpfsDHTv2 dht;
  late Actor actor;
  late InMemoryBlockStore<LikedCrdtPayload, Set<CID>> blockStore;
  late P2PStreamDagFetcher<LikedCrdtPayload, Set<CID>> dagFetcher;
  late GossipSubAnnouncer announcer;
  late MerkleCrdtGossipManager<Set<CID>, LikedCrdtPayload> ownLikedManager;
  String likedCrdtTopic = '';

  _LikedTestNode();

  Future<void> startNodeServices() async {
    _logger.info('[$peerId] Starting PubSub and DHT...');
    await pubsub.start();
    await dht.start();
    _logger.info('[$peerId] PubSub and DHT started.');
  }

  static Future<_LikedTestNode> create(String nodeName) async {
    final helper = _LikedTestNode();

    final udxInstance = UDX();
    final resourceManager = NullResourceManager();
    final connManager = p2p_conn_mgr.ConnectionManager();
    final hostEventBus = p2p_event_bus.BasicBus();

    helper.nodeDetails = await createLibp2pNode(
        udxInstance: udxInstance,
        resourceManager: resourceManager,
        connManager: connManager,
        hostEventBus: hostEventBus,
        userAgentPrefix: nodeName,
    );
    helper.host = helper.nodeDetails.host;
    helper.peerId = helper.nodeDetails.peerId;
    helper.actor = await Actor.create();
    helper.likedCrdtTopic = 'crdt/liked/${helper.actor.did}';

    final router = GossipSubRouter();
    helper.pubsub = PubSub(helper.host, router);
    final providerStore = MemoryProviderStore();
    helper.dht = IpfsDHTv2(
      host: helper.host,
      providerStore: providerStore,
      options: const DHTOptions(mode: DHTMode.server),
    );

    helper.blockStore = InMemoryBlockStore<LikedCrdtPayload, Set<CID>>();
    helper.dagFetcher = P2PStreamDagFetcher<LikedCrdtPayload, Set<CID>>(
      helper.host,
      helper.blockStore,
      '/my-app/liked-crdt-dag-sync/1.0.0',
      LikedCrdtPayload.fromBytes,
      dht: helper.dht,
    );

    helper.announcer = GossipSubAnnouncer(
      helper.host,
      helper.pubsub,
      helper.likedCrdtTopic,
    );

    helper.ownLikedManager = MerkleCrdtGossipManager<Set<CID>, LikedCrdtPayload>(
      dagFetcher: helper.dagFetcher,
      announcer: helper.announcer,
      loggerName: '$nodeName-LikedManager-${helper.actor.did.substring(8, 14)}',
    );
    
    _logger.info('[$nodeName] _LikedTestNode created. Actor DID: ${helper.actor.did}, Liked CRDT Topic: ${helper.likedCrdtTopic}');
    return helper;
  }

  Future<void> stop() async {
    _logger.info('Stopping _LikedTestNode ${actor.did.substring(8,12)}...');
    await ownLikedManager.close();
    await dht.close();
    await pubsub.stop();
    await host.close();
    _logger.info('_LikedTestNode ${actor.did.substring(8,12)} stopped.');
  }
}

void main() {
  Logger.root.level = Level.INFO;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, STACK: ${record.stackTrace}');
  });

  group('Liked CRDT Synchronization Test', () {
    late _LikedTestNode nodeA; // The one whose "liked" list we observe
    late _LikedTestNode nodeB; // The observer
    
    // Example CID for a piece of content
    final contentCid = CID.fromString('bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi');

    setUpAll(() async {
      _logger.info('Setting up Liked CRDT synchronization test nodes...');
      nodeA = await _LikedTestNode.create('NodeA');
      nodeB = await _LikedTestNode.create('NodeB');

      await nodeA.startNodeServices();
      await nodeB.startNodeServices();

      _logger.info('Node A PeerId: ${nodeA.peerId.toBase58()}, Actor DID: ${nodeA.actor.did}');
      _logger.info('Node B PeerId: ${nodeB.peerId.toBase58()}, Actor DID: ${nodeB.actor.did}');
      _logger.info('Node A Liked CRDT Topic: ${nodeA.likedCrdtTopic}');

      await connectPeers(nodeA.host, nodeB.host);
      await Future.delayed(Duration(seconds: 2));

      _logger.info('Populating DHT routing tables and bootstrapping...');
      if (nodeB.host.addrs.isNotEmpty) {
        nodeA.host.peerStore.addrBook.addAddrs(nodeB.peerId, nodeB.host.addrs, AddressTTL.permanentAddrTTL);
        await nodeA.dht.routingTable.tryAddPeer(nodeB.peerId, queryPeer: true);
      }
      if (nodeA.host.addrs.isNotEmpty) {
        nodeB.host.peerStore.addrBook.addAddrs(nodeA.peerId, nodeA.host.addrs, AddressTTL.permanentAddrTTL);
        await nodeB.dht.routingTable.tryAddPeer(nodeA.peerId, queryPeer: true);
      }
      
      await Future.wait([
        nodeA.dht.bootstrap(),
        nodeB.dht.bootstrap(),
      ]).catchError((e,s) => _logger.warning('Error during DHT bootstrap: $e', e, s));
      
      _logger.info('DHT bootstrapping initiated. Waiting for DHT and GossipSub to stabilize...');
      await Future.delayed(Duration(seconds: 15));
      _logger.info('Finished waiting for network stabilization.');
    });

    tearDownAll(() async {
      _logger.info('Tearing down Liked CRDT synchronization test nodes...');
      await nodeA.stop();
      await nodeB.stop();
      _logger.info('Test nodes stopped.');
    });

    test('Node A liking content replicates to Node B', () async {
      final actorA = nodeA.actor;
      final managerA = nodeA.ownLikedManager;

      _logger.info('Setting up Node B to sync Node A\'s "liked" CRDT (${nodeA.likedCrdtTopic})');
      final blockStoreBForASync = InMemoryBlockStore<LikedCrdtPayload, Set<CID>>();
      final dagFetcherBForASync = P2PStreamDagFetcher<LikedCrdtPayload, Set<CID>>(
        nodeB.host,
        blockStoreBForASync,
        '/my-app/liked-crdt-dag-sync/1.0.0', 
        LikedCrdtPayload.fromBytes,
        dht: nodeB.dht,
      );
      final announcerBForASync = GossipSubAnnouncer(
        nodeB.host,
        nodeB.pubsub,
        nodeA.likedCrdtTopic, 
      );
      final managerBForASync = MerkleCrdtGossipManager<Set<CID>, LikedCrdtPayload>(
        dagFetcher: dagFetcherBForASync,
        announcer: announcerBForASync,
        loggerName: 'NodeB-SyncingA-Liked-${nodeA.actor.did.substring(8, 14)}',
      );

      final updateCompleter = Completer<Set<CID>?>();
      final subscriptionB = managerBForASync.onStateChanged.listen((likedSet) {
        _logger.info('Node B (syncing A): Received liked set for ${nodeA.actor.did}: ${likedSet?.map((c)=>c.toString())}');
        if (likedSet != null && likedSet.any((c) => c.toString() == contentCid.toString())) {
          if (!updateCompleter.isCompleted) {
            updateCompleter.complete(likedSet);
          }
        }
      });

      _logger.info('Node A (${actorA.did}) is liking content (${contentCid.toString()})...');
      await actorA.like(contentCid); 
      await managerA.applyLocalUpdate(actorA.likedCrdtPayload);
      _logger.info('Node A local "liked" update applied. Current liked set: ${actorA.likedItems.map((c)=>c.toString())}');

      try {
        _logger.info('Waiting for Node B to receive the "liked" update from Node A...');
        final receivedSetOnB = await updateCompleter.future.timeout(Duration(seconds: 45),
          onTimeout: () {
            _logger.warning('Timeout waiting for Node B to receive "liked" update.');
            return null;
          });

        expect(receivedSetOnB, isNotNull, reason: 'Node B should have received a "liked" update for Node A.');
        expect(receivedSetOnB?.any((c) => c.toString() == contentCid.toString()), isTrue, 
               reason: 'Node B\'s synced list for Node A should contain ${contentCid.toString()}');
        
        _logger.info('Test PASSED: "Liked" update successfully replicated from Node A to Node B.');

      } catch (e, s) {
        _logger.severe('Test FAILED: Error during "liked" CRDT replication or assertion.', e, s);
        fail('Liked CRDT replication test failed: $e');
      } finally {
        await subscriptionB.cancel();
        await managerBForASync.close();
      }
    }, timeout: Timeout(Duration(seconds: 90)));
  });
}
