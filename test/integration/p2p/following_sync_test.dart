import 'dart:async';

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
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/following_crdt.dart'; // Import FollowingCrdtPayload

import '../../real_net_stack.dart' show Libp2pNode, createLibp2pNode;
import '../../test_utils.dart';

final _logger = Logger('FollowingSyncTest');

class _FollowingTestNode {
  late Libp2pNode nodeDetails;
  late Host host;
  late PeerId peerId;
  late PubSub pubsub;
  late IpfsDHTv2 dht;
  late Actor actor;
  late InMemoryBlockStore<FollowingCrdtPayload, Set<String>> blockStore;
  late P2PStreamDagFetcher<FollowingCrdtPayload, Set<String>> dagFetcher;
  late GossipSubAnnouncer announcer;
  // This manager is for the node's OWN "following" CRDT that it broadcasts
  late MerkleCrdtGossipManager<Set<String>, FollowingCrdtPayload> ownFollowingManager;
  String followingCrdtTopic = '';

  _FollowingTestNode();

  Future<void> startNodeServices() async {
    _logger.info('[$peerId] Starting PubSub and DHT...');
    await pubsub.start();
    await dht.start();
    _logger.info('[$peerId] PubSub and DHT started.');
  }

  static Future<_FollowingTestNode> create(String nodeName) async {
    final helper = _FollowingTestNode();

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
    helper.followingCrdtTopic = 'crdt/following/${helper.actor.did}';

    final router = GossipSubRouter();
    helper.pubsub = PubSub(helper.host, router);
    final providerStore = MemoryProviderStore();
    helper.dht = IpfsDHTv2(
      host: helper.host,
      providerStore: providerStore,
      options: const DHTOptions(mode: DHTMode.server),
    );

    helper.blockStore = InMemoryBlockStore<FollowingCrdtPayload, Set<String>>();
    helper.dagFetcher = P2PStreamDagFetcher<FollowingCrdtPayload, Set<String>>(
      helper.host,
      helper.blockStore,
      '/my-app/following-crdt-dag-sync/1.0.0',
      FollowingCrdtPayload.fromBytes,
      dht: helper.dht,
    );

    helper.announcer = GossipSubAnnouncer(
      helper.host,
      helper.pubsub,
      helper.followingCrdtTopic,
    );

    helper.ownFollowingManager = MerkleCrdtGossipManager<Set<String>, FollowingCrdtPayload>(
      dagFetcher: helper.dagFetcher,
      announcer: helper.announcer,
      loggerName: '$nodeName-FollowingManager-${helper.actor.did.substring(8, 14)}',
    );
    
    _logger.info('[$nodeName] _FollowingTestNode created. Actor DID: ${helper.actor.did}, Following CRDT Topic: ${helper.followingCrdtTopic}');
    return helper;
  }

  Future<void> stop() async {
    _logger.info('Stopping _FollowingTestNode ${actor.did.substring(8,12)}...');
    await ownFollowingManager.close();
    await dht.close();
    await pubsub.stop();
    await host.close();
    _logger.info('_FollowingTestNode ${actor.did.substring(8,12)} stopped.');
  }
}

void main() {
  Logger.root.level = Level.INFO; // Adjust as needed
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, STACK: ${record.stackTrace}');
  });

  group('Following CRDT Synchronization Test', () {
    late _FollowingTestNode nodeA; // The one whose "following" list we observe
    late _FollowingTestNode nodeB; // The observer
    late _FollowingTestNode nodeC; // The one being followed by nodeA

    setUpAll(() async {
      _logger.info('Setting up Following CRDT synchronization test nodes...');
      nodeA = await _FollowingTestNode.create('NodeA');
      nodeB = await _FollowingTestNode.create('NodeB');
      nodeC = await _FollowingTestNode.create('NodeC'); // Node C is just for a DID to follow

      await nodeA.startNodeServices();
      await nodeB.startNodeServices();
      // Node C doesn't need full services if it's just a target DID, but starting them is harmless.
      await nodeC.startNodeServices();


      _logger.info('Node A PeerId: ${nodeA.peerId.toBase58()}, Actor DID: ${nodeA.actor.did}');
      _logger.info('Node B PeerId: ${nodeB.peerId.toBase58()}, Actor DID: ${nodeB.actor.did}');
      _logger.info('Node C PeerId: ${nodeC.peerId.toBase58()}, Actor DID: ${nodeC.actor.did}');
      _logger.info('Node A Following CRDT Topic: ${nodeA.followingCrdtTopic}');


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
      _logger.info('Tearing down Following CRDT synchronization test nodes...');
      await nodeA.stop();
      await nodeB.stop();
      await nodeC.stop();
      _logger.info('Test nodes stopped.');
    });

    test('Node A following Node C replicates to Node B', () async {
      final actorA = nodeA.actor;
      final managerA = nodeA.ownFollowingManager;
      final didToFollow = nodeC.actor.did;

      // Node B sets up a manager to sync Node A's "following" CRDT
      _logger.info('Setting up Node B to sync Node A\'s "following" CRDT (${nodeA.followingCrdtTopic})');
      final blockStoreBForASync = InMemoryBlockStore<FollowingCrdtPayload, Set<String>>();
      final dagFetcherBForASync = P2PStreamDagFetcher<FollowingCrdtPayload, Set<String>>(
        nodeB.host,
        blockStoreBForASync,
        '/my-app/following-crdt-dag-sync/1.0.0', // Must match Node A's fetcher protocol
        FollowingCrdtPayload.fromBytes,
        dht: nodeB.dht,
      );
      final announcerBForASync = GossipSubAnnouncer(
        nodeB.host,
        nodeB.pubsub,
        nodeA.followingCrdtTopic, // Node B subscribes to Node A's "following" topic
      );
      final managerBForASync = MerkleCrdtGossipManager<Set<String>, FollowingCrdtPayload>(
        dagFetcher: dagFetcherBForASync,
        announcer: announcerBForASync,
        loggerName: 'NodeB-SyncingA-Following-${nodeA.actor.did.substring(8, 14)}',
      );

      final updateCompleter = Completer<Set<String>?>();
      final subscriptionB = managerBForASync.onStateChanged.listen((followingSet) {
        _logger.info('Node B (syncing A): Received following set for ${nodeA.actor.did}: $followingSet');
        if (followingSet != null && followingSet.contains(didToFollow)) {
          if (!updateCompleter.isCompleted) {
            updateCompleter.complete(followingSet);
          }
        }
      });

      _logger.info('Node A (${actorA.did}) is following Node C (${didToFollow})...');
      await actorA.follow(didToFollow); // This updates actorA._followingCrdt
      // Now, apply this updated CRDT payload to Node A's manager to broadcast it
      await managerA.applyLocalUpdate(actorA.followingCrdtPayload);
      _logger.info('Node A local "following" update applied. Current following set: ${actorA.following}');

      try {
        _logger.info('Waiting for Node B to receive the "following" update from Node A...');
        final receivedSetOnB = await updateCompleter.future.timeout(Duration(seconds: 45),
          onTimeout: () {
            _logger.warning('Timeout waiting for Node B to receive "following" update.');
            return null;
          });

        expect(receivedSetOnB, isNotNull, reason: 'Node B should have received a "following" update for Node A.');
        expect(receivedSetOnB, contains(didToFollow), reason: 'Node B\'s synced list for Node A should contain ${didToFollow}');
        
        _logger.info('Test PASSED: "Following" update successfully replicated from Node A to Node B.');

      } catch (e, s) {
        _logger.severe('Test FAILED: Error during "following" CRDT replication or assertion.', e, s);
        fail('Following CRDT replication test failed: $e');
      } finally {
        await subscriptionB.cancel();
        await managerBForASync.close();
      }
    }, timeout: Timeout(Duration(seconds: 90)));
  });
}
