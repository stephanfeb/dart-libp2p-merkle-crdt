import 'dart:async';

import 'package:dart_libp2p/dart_libp2p.dart';
import 'package:dart_libp2p/p2p/host/basic/basic_host.dart' show BasicHost; // Corrected BasicHost import
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:dart_libp2p_kad_dht/dart_libp2p_kad_dht.dart' show DHTMode, DHTOptions, IpfsDHT, IpfsDHTv2; // For IpfsDHT, DHTOptions, DHTMode
import 'package:dart_libp2p_kad_dht/src/providers/provider_store.dart' show MemoryProviderStore; // For MemoryProviderStore
// For NullResourceManager, ConnectionManager, BasicBus if not exported by dart_libp2p
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_udx/dart_udx.dart' show UDX; // For UDX

import 'package:logging/logging.dart';
import 'package:test/test.dart';

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/p2p.dart';

// Assuming real_net_stack.dart is in ../../ (relative to test/integration/p2p/)
import '../../real_net_stack.dart' show Libp2pNode, createLibp2pNode; 
import '../../test_utils.dart'; // For connectPeers, etc.

final _logger = Logger('ProfileSyncTest');

// Helper class to bundle a node's components for the test
class _ProfileNode {
  late Libp2pNode nodeDetails;
  late Host host;
  late PeerId peerId;
  late PubSub pubsub;
  late IpfsDHTv2 dht; // Added DHT instance
  late Actor actor;
  late InMemoryBlockStore<ProfileCrdtPayload, ProfileData> blockStore;
  late P2PStreamDagFetcher<ProfileCrdtPayload, ProfileData> dagFetcher;
  late GossipSubAnnouncer announcer;
  late MerkleCrdtGossipManager<ProfileData, ProfileCrdtPayload> manager;
  String profileCrdtId = ''; // Will be set after actor creation

  _ProfileNode();

  Future<void> startNodeServices() async {
    _logger.info('[$peerId] Starting PubSub and DHT...');
    await pubsub.start();
    await dht.start();
    _logger.info('[$peerId] PubSub and DHT started.');
  }

  static Future<_ProfileNode> create(String nodeName) async {
    final helper = _ProfileNode();

    // 1. Setup Libp2p Node using RealNetStack
    // RealNetStack components
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

    // 2. Create Actor
    helper.actor = await Actor.create();
    helper.profileCrdtId = 'profile-${helper.actor.did}';

    // 3. Setup PubSub
    final router = GossipSubRouter(); // Or use one from RealNetStack if provided
    helper.pubsub = PubSub(helper.host, router);
    // await helper.pubsub.start(); // Will be started by startNodeServices

    // 4. Setup DHT
    final providerStore = MemoryProviderStore();
    helper.dht = IpfsDHTv2(
      host: helper.host,
      providerStore: providerStore,
      options: const DHTOptions(mode: DHTMode.server), // Server mode for test nodes
    );
    // await helper.dht.start(); // Will be started by startNodeServices

    // 5. Setup MerkleCRDT components for Profile
    helper.blockStore = InMemoryBlockStore<ProfileCrdtPayload, ProfileData>();
    
    helper.dagFetcher = P2PStreamDagFetcher<ProfileCrdtPayload, ProfileData>(
      helper.host,
      helper.blockStore,
      '/my-app/profile-crdt-dag-sync/1.0.0', // Unique protocol ID
      ProfileCrdtPayload.fromBytes,
      dht: helper.dht, // Pass the created DHT instance
    );

    helper.announcer = GossipSubAnnouncer(
      helper.host,
      helper.pubsub,
      helper.profileCrdtId, // CRDT instance-specific topic
    );

    helper.manager = MerkleCrdtGossipManager<ProfileData, ProfileCrdtPayload>(
      dagFetcher: helper.dagFetcher,
      announcer: helper.announcer,
      loggerName: '$nodeName-ProfileManager-${helper.actor.did.substring(8, 14)}',
    );
    
    _logger.info('[$nodeName] ProfileNode created. Actor DID: ${helper.actor.did}, Profile CRDT ID: ${helper.profileCrdtId}');
    return helper;
  }

  Future<void> stop() async {
    _logger.info('Stopping ProfileNode ${actor.did.substring(8,12)}...');
    await manager.close();
    await dht.close(); // Close DHT
    await pubsub.stop();
    await host.close(); 
    _logger.info('ProfileNode ${actor.did.substring(8,12)} stopped.');
  }
}


void main() {
  Logger.root.level = Level.INFO; // Adjust as needed
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, STACK: ${record.stackTrace}');
  });

  group('Profile CRDT Synchronization Test', () {
    late _ProfileNode nodeA;
    late _ProfileNode nodeB;

    setUpAll(() async {
      _logger.info('Setting up Profile CRDT synchronization test nodes...');
      nodeA = await _ProfileNode.create('NodeA');
      nodeB = await _ProfileNode.create('NodeB');

      // Start node services (PubSub, DHT)
      await nodeA.startNodeServices();
      await nodeB.startNodeServices();

      _logger.info('Node A PeerId: ${nodeA.peerId.toBase58()}');
      _logger.info('Node B PeerId: ${nodeB.peerId.toBase58()}');
      _logger.info('Node A Actor DID: ${nodeA.actor.did}');
      _logger.info('Node B Actor DID: ${nodeB.actor.did}');
      _logger.info('Node A Profile CRDT ID: ${nodeA.profileCrdtId}');
      _logger.info('Node B Profile CRDT ID: ${nodeB.profileCrdtId}');

      // Connect nodes using the helper from test_utils.dart
      await connectPeers(nodeA.host, nodeB.host);
      await Future.delayed(Duration(seconds: 2)); // Brief pause after direct connection

      // Explicitly add peers to DHT routing tables and bootstrap
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
      // Increased delay for network stabilization (DHT + GossipSub)
      await Future.delayed(Duration(seconds: 15)); 
      _logger.info('Finished waiting for network stabilization.');
    });

    tearDownAll(() async {
      _logger.info('Tearing down Profile CRDT synchronization test nodes...');
      await nodeA.stop();
      await nodeB.stop();
      _logger.info('Test nodes stopped.');
    });

    test('Profile update from Node A replicates to Node B', () async {
      final managerA = nodeA.manager;
      
      // Node B needs to manage/sync Node A's profile CRDT.
      // For this test, we'll create a manager on Node B specifically for Node A's profileCrdtId.
      // This simulates Node B "following" or "discovering" Node A's profile.
      _logger.info('Setting up Node B to sync Node A\'s profile (${nodeA.profileCrdtId})');
      final blockStoreBForAProfile = InMemoryBlockStore<ProfileCrdtPayload, ProfileData>();
      
      // Use Node B's actual DHT instance
      final dhtBInstance = nodeB.dht; 

      final dagFetcherBForAProfile = P2PStreamDagFetcher<ProfileCrdtPayload, ProfileData>(
        nodeB.host,
        blockStoreBForAProfile,
        '/my-app/profile-crdt-dag-sync/1.0.0', // Must match Node A's fetcher protocol
        ProfileCrdtPayload.fromBytes,
        dht: dhtBInstance, // Pass Node B's DHT
      );
      final announcerBForAProfile = GossipSubAnnouncer(
        nodeB.host,
        nodeB.pubsub, // Node B's pubsub instance
        nodeA.profileCrdtId, // Subscribing to Node A's profile topic
      );
      final managerBForAProfile = MerkleCrdtGossipManager<ProfileData, ProfileCrdtPayload>(
        dagFetcher: dagFetcherBForAProfile,
        announcer: announcerBForAProfile,
        loggerName: 'NodeB-Syncing-${nodeA.actor.did.substring(8, 14)}',
      );


      final updateCompleter = Completer<ProfileData?>();
      final subscriptionB = managerBForAProfile.onStateChanged.listen((newProfileState) {
        _logger.info('Node B (syncing A): Received state update for ${nodeA.actor.did}: ${newProfileState?.name}');
        if (newProfileState?.name == 'Alice The Great') {
          if (!updateCompleter.isCompleted) {
            updateCompleter.complete(newProfileState);
          }
        }
      });

      _logger.info('Node A (${nodeA.actor.did}) applying local profile update...');
      var profilePayloadA = ProfileCrdtPayload.create(nodeA.actor.did, nodeA.actor.publicKeyJwk);
      profilePayloadA = profilePayloadA.set('name', 'Alice The Great');
      profilePayloadA = profilePayloadA.set('preferredUsername', 'alicetg');
      
      await managerA.applyLocalUpdate(profilePayloadA);
      _logger.info('Node A local profile update applied. Current state name: ${(await managerA.getState())?.name}');

      try {
        _logger.info('Waiting for Node B to receive the profile update from Node A...');
        final receivedStateOnB = await updateCompleter.future.timeout(Duration(seconds: 45), // Increased timeout
          onTimeout: () {
            _logger.warning('Timeout waiting for Node B to receive profile update.');
            return null;
          });

        expect(receivedStateOnB, isNotNull, reason: 'Node B should have received a profile update for Node A.');
        expect(receivedStateOnB?.id, equals(nodeA.actor.did));
        expect(receivedStateOnB?.name, equals('Alice The Great'));
        expect(receivedStateOnB?.preferredUsername, equals('alicetg'));
        
        _logger.info('Test PASSED: Profile update successfully replicated from Node A to Node B.');

      } catch (e, s) {
        _logger.severe('Test FAILED: Error during profile replication or assertion.', e, s);
        // Add diagnostic info if helpful
        fail('Profile replication test failed: $e');
      } finally {
        await subscriptionB.cancel();
        await managerBForAProfile.close(); // Clean up the temporary manager
      }
    }, timeout: Timeout(Duration(seconds: 90))); // Overall test timeout
  });
}
