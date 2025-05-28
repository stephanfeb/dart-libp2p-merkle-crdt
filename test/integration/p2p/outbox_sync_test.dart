import 'dart:async';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p; // Should export Host, PeerId, EvtPeerConnectednessChanged, EventBus (interface)

// Removed: import 'package:dart_libp2p/core.dart' as libp2p_core;
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHT, DHTOptions, DHTMode;
import 'package:dart_libp2p_kat_dht/src/providers/provider_store.dart' show MemoryProviderStore;
import 'dart:typed_data'; // Added for Uint8List

import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus; // Retain for hostEventBus in create()
// EventBus, EvtPeerConnectednessChanged should be available from 'package:dart_libp2p/dart_libp2p.dart'
import 'package:dart_udx/udx.dart' show UDX;
import 'package:dart_cid/dart_cid.dart' show CID;

import 'package:logging/logging.dart';
import 'package:test/test.dart';

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart'; // Main import for CRDT lib types
import 'package:dart_libp2p_merkle_crdt/src/p2p/p2p.dart'; // For Actor, OutboxCrdtPayload, OutboxData, Note, CreateActivity
import 'package:dart_libp2p_merkle_crdt/src/block_store.dart' as raw_block_store; // Alias our new InMemoryBlockStore for raw blocks

// Bitswap imports
import 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart' show Bitswap, IBlockStore, WantManager;
import 'dart:convert' show jsonEncode, utf8; // For serializing objects to store

import '../../real_net_stack.dart' show Libp2pNode, createLibp2pNode;
import '../../test_utils.dart'; // For connectPeers

final _logger = Logger('OutboxSyncTest');

// Helper class to bundle a node's components for the test
class _OutboxNode {
  late Libp2pNode nodeDetails;
  late libp2p.Host host;
  late libp2p.PeerId peerId;
  late PubSub pubsub;
  late IpfsDHT dht;
  late Actor actor;
  late Bitswap bitswap; // Added Bitswap instance
  late raw_block_store.InMemoryBlockStore blockStore; // Use aliased type for raw IPLD blocks
  
  // Specific to Outbox CRDT
  late InMemoryBlockStore<OutboxCrdtPayload, OutboxData> outboxCrdtBlockStore; // Use direct type from merkle_crdt_lib
  late P2PStreamDagFetcher<OutboxCrdtPayload, OutboxData> outboxDagFetcher;
  late GossipSubAnnouncer outboxAnnouncer;
  late MerkleCrdtGossipManager<OutboxData, OutboxCrdtPayload> outboxManager;
  String outboxCrdtId = ''; // Will be set after actor creation

  _OutboxNode();

  Future<void> startNodeServices() async {
    _logger.info('[$peerId] Starting PubSub, DHT, and Bitswap for OutboxNode...');
    await pubsub.start();
    await dht.start();
    // Bitswap is started by its constructor via BitswapNetworkService
    _logger.info('[$peerId] PubSub, DHT, and Bitswap services initialized/started for OutboxNode.');
  }

  static Future<_OutboxNode> create(String nodeName) async {
    final helper = _OutboxNode();

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
    helper.outboxCrdtId = 'outbox-${helper.actor.did}';

    final router = GossipSubRouter();
    helper.pubsub = PubSub(helper.host, router);

    final providerStore = MemoryProviderStore();
    helper.dht = IpfsDHT(
      host: helper.host,
      providerStore: providerStore,
      options: const DHTOptions(mode: DHTMode.server),
    );

    // Create general block store for IPLD objects
    helper.blockStore = raw_block_store.InMemoryBlockStore();

    // Setup Bitswap
    // Need to get the peer events stream from the host's event bus
    // Cast to the concrete BasicBus type to ensure the .on<T>() method is found.
    final p2pBasicEventBus = helper.host.eventBus as p2p_event_bus.BasicBus;
    final subscription = p2pBasicEventBus.subscribe(libp2p.EvtPeerConnectednessChanged);

    helper.bitswap = Bitswap(
      host: helper.host,
      blockStore: helper.blockStore, // Use the general block store
      peerEventsStream: subscription.stream,
    );

    // Setup MerkleCRDT components for Outbox
    helper.outboxCrdtBlockStore = InMemoryBlockStore<OutboxCrdtPayload, OutboxData>(); // Use direct type
    helper.outboxDagFetcher = P2PStreamDagFetcher<OutboxCrdtPayload, OutboxData>(
      helper.host,
      helper.outboxCrdtBlockStore, // Use the CRDT specific block store here
      '/my-app/outbox-crdt-dag-sync/1.0.0', // Unique protocol ID for outbox
      OutboxCrdtPayload.fromBytes,
      dht: helper.dht,
    );
    helper.outboxAnnouncer = GossipSubAnnouncer(
      helper.host,
      helper.pubsub,
      helper.outboxCrdtId, // CRDT instance-specific topic for outbox
    );
    helper.outboxManager = MerkleCrdtGossipManager<OutboxData, OutboxCrdtPayload>(
      dagFetcher: helper.outboxDagFetcher,
      announcer: helper.outboxAnnouncer,
      loggerName: '$nodeName-OutboxManager-${helper.actor.did.substring(8, 14)}',
    );
    
    _logger.info('[$nodeName] OutboxNode created. Actor DID: ${helper.actor.did}, Outbox CRDT ID: ${helper.outboxCrdtId}');
    return helper;
  }

  Future<void> stop() async {
    _logger.info('Stopping OutboxNode ${actor.did.substring(8,12)}...');
    await outboxManager.close();
    await bitswap.stop(); // Stop Bitswap
    await dht.close();
    await pubsub.stop();
    await host.close(); 
    _logger.info('OutboxNode ${actor.did.substring(8,12)} stopped.');
  }

  // Helper to publish a note to the outbox
  Future<({Note note, CreateActivity activity, CID activityCid})> publishNoteToOutbox(String content) async {
    final noteResult = await actor.createNote(content);
    final activityResult = await actor.createAndSignActivity(objectCid: noteResult.noteCid);
    
    // Store the Note and Activity blocks in the general blockStore for Bitswap
    final noteBytes = Uint8List.fromList(utf8.encode(jsonEncode(noteResult.note.toJson())));
    await blockStore.put(noteResult.noteCid, noteBytes);
    _logger.info('[${actor.did.substring(8,14)}] Stored Note ${noteResult.noteCid} in blockStore.');

    final activityBytes = Uint8List.fromList(utf8.encode(jsonEncode(activityResult.activity.toJson())));
    await blockStore.put(activityResult.activityCid, activityBytes);
     _logger.info('[${actor.did.substring(8,14)}] Stored Activity ${activityResult.activityCid} in blockStore.');

    // Fetch current state (OutboxData) to build upon for the outbox CRDT
    final currentOutboxData = await outboxManager.getState();
    
    OutboxCrdtPayload newPayload = OutboxCrdtPayload.create();
    if (currentOutboxData != null) {
        for (final cidInLoop in currentOutboxData.activityCids) { 
            newPayload = newPayload.append(cidInLoop); 
        }
    }
    newPayload = newPayload.append(activityResult.activityCid); 

    await outboxManager.applyLocalUpdate(newPayload);
    _logger.info('[${actor.did.substring(8,14)}] Published activity ${activityResult.activityCid} to outbox CRDT. New payload has ${newPayload.value.activityCids.length} CIDs.');
    return (note: noteResult.note, activity: activityResult.activity as CreateActivity, activityCid: activityResult.activityCid);
  }
}

void main() {
  Logger.root.level = Level.INFO;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, STACK: ${record.stackTrace}');
  });

  group('Outbox CRDT Synchronization Test', () {
    late _OutboxNode nodeA;
    late _OutboxNode nodeB;

    setUpAll(() async {
      _logger.info('Setting up Outbox CRDT synchronization test nodes...');
      nodeA = await _OutboxNode.create('NodeA');
      nodeB = await _OutboxNode.create('NodeB');

      await nodeA.startNodeServices();
      await nodeB.startNodeServices();

      _logger.info('Node A Outbox CRDT ID: ${nodeA.outboxCrdtId}');
      _logger.info('Node B Outbox CRDT ID: ${nodeB.outboxCrdtId}');

      await connectPeers(nodeA.host, nodeB.host);
      await Future.delayed(Duration(seconds: 2));

      _logger.info('Populating DHT routing tables and bootstrapping for Outbox test...');
      if (nodeB.host.addrs.isNotEmpty) {
        nodeA.host.peerStore.addrBook.addAddrs(nodeB.peerId, nodeB.host.addrs, libp2p.AddressTTL.permanentAddrTTL);
        await nodeA.dht.routingTable.tryAddPeer(nodeB.peerId, queryPeer: true);
      }
      if (nodeA.host.addrs.isNotEmpty) {
        nodeB.host.peerStore.addrBook.addAddrs(nodeA.peerId, nodeA.host.addrs, libp2p.AddressTTL.permanentAddrTTL);
        await nodeB.dht.routingTable.tryAddPeer(nodeA.peerId, queryPeer: true);
      }
      
      await Future.wait([
        nodeA.dht.bootstrap(),
        nodeB.dht.bootstrap(),
      ]).catchError((e,s) => _logger.warning('Error during Outbox DHT bootstrap: $e', e, s));
      
      _logger.info('Outbox DHT bootstrapping initiated. Waiting for DHT and GossipSub to stabilize...');
      await Future.delayed(Duration(seconds: 15)); 
      _logger.info('Finished waiting for Outbox network stabilization.');
    });

    tearDownAll(() async {
      _logger.info('Tearing down Outbox CRDT synchronization test nodes...');
      await nodeA.stop();
      await nodeB.stop();
      _logger.info('Outbox test nodes stopped.');
    });

    test('Activity published to Node A outbox replicates to Node B', () async {
      // Node B needs to manage/sync Node A's outbox CRDT.
      _logger.info('Setting up Node B to sync Node A\'s outbox (${nodeA.outboxCrdtId})');
      final crdtBlockStoreBForAOutbox = InMemoryBlockStore<OutboxCrdtPayload, OutboxData>(); // Use direct type
      
      final dagFetcherBForAOutbox = P2PStreamDagFetcher<OutboxCrdtPayload, OutboxData>(
        nodeB.host,
        crdtBlockStoreBForAOutbox, // Use the CRDT specific store
        '/my-app/outbox-crdt-dag-sync/1.0.0', // Must match Node A's fetcher protocol
        OutboxCrdtPayload.fromBytes,
        dht: nodeB.dht, // Use Node B's DHT
      );
      final announcerBForAOutbox = GossipSubAnnouncer(
        nodeB.host,
        nodeB.pubsub,
        nodeA.outboxCrdtId, // Subscribing to Node A's outbox topic
      );
      final managerBForAOutbox = MerkleCrdtGossipManager<OutboxData, OutboxCrdtPayload>(
        dagFetcher: dagFetcherBForAOutbox,
        announcer: announcerBForAOutbox,
        loggerName: 'NodeB-SyncingOutbox-${nodeA.actor.did.substring(8, 14)}',
      );

      final updateCompleter = Completer<OutboxData?>();
      
      _logger.info('Node A (${nodeA.actor.did}) publishing note to its outbox...');
      final publishResult = await nodeA.publishNoteToOutbox('Hello from Node A outbox!');
      final CID publishedActivityCid = publishResult.activityCid; // Initialize immediately
      _logger.info('Node A published activity ${publishedActivityCid}. Current outbox size: ${(await nodeA.outboxManager.getState())?.activityCids.length}');

      // Now that publishedActivityCid is known, set up the listener
      final subscriptionB = managerBForAOutbox.onStateChanged.listen((newOutboxState) {
        _logger.info('Node B (syncing A outbox): Received state update. Activity CIDs: ${newOutboxState?.activityCids.length}');
        if (newOutboxState != null && newOutboxState.activityCids.contains(publishedActivityCid)) {
          if (!updateCompleter.isCompleted) {
            updateCompleter.complete(newOutboxState);
          }
        }
      });

      // Check if the state was already received due to quick propagation
      // (manager might have processed it while we were setting up the listener)
      final initialCheckState = await managerBForAOutbox.getState();
      if (initialCheckState != null && initialCheckState.activityCids.contains(publishedActivityCid)) {
        if (!updateCompleter.isCompleted) {
          _logger.info('Node B (syncing A outbox): State already contained the CID before explicit wait.');
          updateCompleter.complete(initialCheckState);
        }
      }

      try {
        _logger.info('Waiting for Node B to receive the outbox update from Node A (or confirm it was already received)...');
        final receivedStateOnB = await updateCompleter.future.timeout(Duration(seconds: 45),
          onTimeout: () {
            _logger.warning('Timeout waiting for Node B to receive outbox update.');
            return null;
          });

        expect(receivedStateOnB, isNotNull, reason: 'Node B should have received an outbox update for Node A.');
        // The managerBForAOutbox is subscribed to nodeA.outboxCrdtId, so if data is received, it's for the correct CRDT.
        // The OutboxData itself doesn't store the top-level CRDT ID.
        expect(receivedStateOnB?.activityCids, contains(publishedActivityCid), reason: 'Node B outbox should contain the published activity CID.');
        
        _logger.info('Test PASSED: Outbox update successfully replicated from Node A to Node B.');

      } catch (e, s) {
        _logger.severe('Test FAILED: Error during outbox replication or assertion.', e, s);
        fail('Outbox replication test failed: $e');
      } finally {
        await subscriptionB.cancel();
        await managerBForAOutbox.close();
      }
    }, timeout: Timeout(Duration(seconds: 90)));
  });
}
