import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dart_libp2p_merkle_crdt/src/bitswap/bitswap.dart';
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHT, DHTOptions, DHTMode;
import 'package:dart_libp2p_kat_dht/src/providers/provider_store.dart' show MemoryProviderStore;
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_udx/udx.dart' show UDX;
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:logging/logging.dart';
import 'package:test/test.dart';
import 'package:cbor/cbor.dart';

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/p2p.dart'; // For Actor, Note, CreateActivity, etc.
import 'package:dart_libp2p_merkle_crdt/src/block_store.dart' as raw_block_store;

// Import Bitswap from the (assumed) local path or correct package reference
// This path might need adjustment based on your project structure or if bitswap is a dependency.

import '../../real_net_stack.dart' show Libp2pNode, createLibp2pNode;
import '../../test_utils.dart';

final _logger = Logger('OutboxSyncTest');

class _OutboxNode {
  late Libp2pNode nodeDetails;
  late libp2p.Host host;
  late libp2p.PeerId peerId;
  late PubSub pubsub;
  late IpfsDHT dht;
  late Actor actor;
  late Bitswap bitswap;
  late raw_block_store.InMemoryBlockStore blockStore; // General block store for raw IPLD blocks
  
  late InMemoryBlockStore<OutboxCrdtPayload, List<CID>> outboxCrdtBlockStore;
  late P2PStreamDagFetcher<OutboxCrdtPayload, List<CID>> outboxDagFetcher;
  late GossipSubAnnouncer outboxAnnouncer;
  late MerkleCrdtGossipManager<List<CID>, OutboxCrdtPayload> outboxManager;
  String outboxCrdtId = '';

  _OutboxNode();

  Future<void> startNodeServices() async {
    _logger.info('[$peerId] Starting PubSub, DHT, and Bitswap for OutboxNode...');
    await pubsub.start();
    await dht.start();
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

    helper.blockStore = raw_block_store.InMemoryBlockStore();

    final p2pBasicEventBus = helper.host.eventBus as p2p_event_bus.BasicBus;
    final subscription = p2pBasicEventBus.subscribe(libp2p.EvtPeerConnectednessChanged);

    helper.bitswap = Bitswap(
      host: helper.host,
      blockStore: helper.blockStore, // Bitswap uses the general block store
      peerEventsStream: subscription.stream,
      nodeNameForLogging: "$nodeName-BS", // Pass the nodeName (e.g., "NodeA-BS")
    );

    helper.outboxCrdtBlockStore = InMemoryBlockStore<OutboxCrdtPayload, List<CID>>();
    helper.outboxDagFetcher = P2PStreamDagFetcher<OutboxCrdtPayload, List<CID>>(
      helper.host,
      helper.outboxCrdtBlockStore,
      '/my-app/outbox-crdt-dag-sync/1.0.0',
      OutboxCrdtPayload.fromBytes,
      dht: helper.dht,
    );
    helper.outboxAnnouncer = GossipSubAnnouncer(
      helper.host,
      helper.pubsub,
      helper.outboxCrdtId,
    );
    helper.outboxManager = MerkleCrdtGossipManager<List<CID>, OutboxCrdtPayload>(
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
    await bitswap.stop();
    await dht.close();
    await pubsub.stop();
    await host.close(); 
    _logger.info('OutboxNode ${actor.did.substring(8,12)} stopped.');
  }

  Future<({Note note, CreateActivity activity, CID activityCid, CID noteCid})> publishNoteToOutbox(String content) async {
    final noteResult = await actor.createNote(content);
    final activityResult = await actor.createAndSignActivity(objectCid: noteResult.noteCid);
    
    final noteCborValue = noteResult.note.toCborMap();
    final noteBytesEncoded = cbor.encode(noteCborValue);
    final noteBytesForStore = noteBytesEncoded is Uint8List ? noteBytesEncoded : Uint8List.fromList(noteBytesEncoded);
    await blockStore.put(noteResult.noteCid, noteBytesForStore);
    _logger.info('[${actor.did.substring(8,14)}] Stored Note ${noteResult.noteCid} (CBOR) in blockStore for Bitswap.');

    final activityCborValue = (activityResult.activity as CreateActivity).toCborMap();
    final activityBytesEncoded = cbor.encode(activityCborValue);
    final activityBytesForStore = activityBytesEncoded is Uint8List ? activityBytesEncoded : Uint8List.fromList(activityBytesEncoded);
    await blockStore.put(activityResult.activityCid, activityBytesForStore);
    _logger.info('[${actor.did.substring(8,14)}] Stored Activity ${activityResult.activityCid} (CBOR) in blockStore for Bitswap.');

    final currentOutboxData = await outboxManager.getState();
    
    OutboxCrdtPayload newPayload = OutboxCrdtPayload.create();
    if (currentOutboxData != null) {
        for (final cidInLoop in currentOutboxData) { // currentOutboxData is List<CID>
            newPayload = newPayload.add(cidInLoop); 
        }
    }
    newPayload = newPayload.add(activityResult.activityCid); 

    await outboxManager.applyLocalUpdate(newPayload);
    _logger.info('[${actor.did.substring(8,14)}] Published activity ${activityResult.activityCid} to outbox CRDT. New payload has ${newPayload.value.length} CIDs.');
    return (note: noteResult.note, activity: activityResult.activity as CreateActivity, activityCid: activityResult.activityCid, noteCid: noteResult.noteCid);
  }
}

void main() {
  Logger.root.level = Level.INFO;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, STACK: ${record.stackTrace}');
  });

  group('Outbox CRDT Synchronization and Bitswap Fetch Test', () {
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
      _logger.info('Peers connected. Waiting 5 seconds for protocol exchange (Identify) to complete...');
      await Future.delayed(Duration(seconds: 5)); // Increased delay for protocol exchange

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

    test('Activity published to Node A outbox replicates to Node B, and content is fetched via Bitswap', () async {
      _logger.info('Setting up Node B to sync Node A\'s outbox (${nodeA.outboxCrdtId})');
      final crdtBlockStoreBForAOutbox = InMemoryBlockStore<OutboxCrdtPayload, List<CID>>();
      
      final dagFetcherBForAOutbox = P2PStreamDagFetcher<OutboxCrdtPayload, List<CID>>(
        nodeB.host,
        crdtBlockStoreBForAOutbox,
        '/my-app/outbox-crdt-dag-sync/1.0.0',
        OutboxCrdtPayload.fromBytes,
        dht: nodeB.dht,
      );
      final announcerBForAOutbox = GossipSubAnnouncer(
        nodeB.host,
        nodeB.pubsub,
        nodeA.outboxCrdtId,
      );
      final managerBForAOutbox = MerkleCrdtGossipManager<List<CID>, OutboxCrdtPayload>(
        dagFetcher: dagFetcherBForAOutbox,
        announcer: announcerBForAOutbox,
        loggerName: 'NodeB-SyncingOutbox-${nodeA.actor.did.substring(8, 14)}',
      );

      final updateCompleter = Completer<List<CID>?>();
      
      _logger.info('Node A (${nodeA.actor.did}) publishing note to its outbox...');
      final originalNoteContent = 'Hello from Node A outbox, to be fetched by Bitswap!';
      final publishResult = await nodeA.publishNoteToOutbox(originalNoteContent);
      final CID publishedActivityCid = publishResult.activityCid;
      final CID originalNoteCid = publishResult.noteCid; // Get the original Note's CID

      _logger.info('Node A published activity ${publishedActivityCid} (for note ${originalNoteCid}). Current outbox size: ${(await nodeA.outboxManager.getState())?.length}');

      final subscriptionB = managerBForAOutbox.onStateChanged.listen((newOutboxState_ListCid) { // Renamed to avoid clash
        _logger.info('Node B (syncing A outbox): Received state update. Activity CIDs: ${newOutboxState_ListCid?.length}');
        if (newOutboxState_ListCid != null && newOutboxState_ListCid.contains(publishedActivityCid)) {
          if (!updateCompleter.isCompleted) {
            updateCompleter.complete(newOutboxState_ListCid);
          }
        }
      });

      final initialCheckState = await managerBForAOutbox.getState();
      if (initialCheckState != null && initialCheckState.contains(publishedActivityCid)) {
        if (!updateCompleter.isCompleted) {
          _logger.info('Node B (syncing A outbox): State already contained the CID before explicit wait.');
          updateCompleter.complete(initialCheckState);
        }
      }

      try {
        _logger.info('Waiting for Node B to receive the outbox update from Node A...');
        final receivedStateOnB_ListCid = await updateCompleter.future.timeout(Duration(seconds: 45), // Renamed
          onTimeout: () {
            _logger.warning('Timeout waiting for Node B to receive outbox update.');
            return null;
          });

        expect(receivedStateOnB_ListCid, isNotNull, reason: 'Node B should have received an outbox update for Node A.');
        expect(receivedStateOnB_ListCid, contains(publishedActivityCid), reason: 'Node B outbox should contain the published activity CID.');
        
        _logger.info('Node B received CRDT update for activity $publishedActivityCid. Now fetching via Bitswap...');

        // 1. Node B fetches the CreateActivity block
        _logger.info('Node B: Attempting to fetch CreateActivity block $publishedActivityCid from Node A via Bitswap...');
        final activityBlockBytes = await nodeB.bitswap.getBlock(publishedActivityCid);
        expect(activityBlockBytes, isNotNull, reason: 'Node B should fetch CreateActivity block via Bitswap.');
        _logger.info('Node B: Successfully fetched CreateActivity block ($publishedActivityCid) bytes (length: ${activityBlockBytes?.length}).');

        // 2. Node B deserializes the CreateActivity
        final decodedActivityCbor = cbor.decode(activityBlockBytes!) as CborMap;
        final fetchedActivity = CreateActivity.fromCborMap(decodedActivityCbor);
        expect(fetchedActivity, isA<CreateActivity>());
        expect(fetchedActivity.actor, nodeA.actor.did, reason: 'Fetched activity actor DID should match Node A.');
        expect(fetchedActivity.object, originalNoteCid, reason: 'Fetched activity object CID should match original Note CID.');
        _logger.info('Node B: Successfully deserialized CreateActivity. Object (Note) CID: ${fetchedActivity.object}');

        // 3. Node B extracts the Note's CID and fetches the Note block
        final noteCidFromActivity = fetchedActivity.object;
        _logger.info('Node B: Attempting to fetch Note block $noteCidFromActivity from Node A via Bitswap...');
        final noteBlockBytes = await nodeB.bitswap.getBlock(noteCidFromActivity);
        expect(noteBlockBytes, isNotNull, reason: 'Node B should fetch Note block via Bitswap.');
        _logger.info('Node B: Successfully fetched Note block ($noteCidFromActivity) bytes (length: ${noteBlockBytes?.length}).');
        
        // 4. Node B deserializes the Note
        final decodedNoteCbor = cbor.decode(noteBlockBytes!) as CborMap;
        final fetchedNote = Note.fromCborMap(decodedNoteCbor);
        expect(fetchedNote, isA<Note>());
        _logger.info('Node B: Successfully deserialized Note.');

        // 5. Node B verifies the content of the Note
        expect(fetchedNote.content, originalNoteContent, reason: 'Fetched Note content should match the original content.');
        _logger.info('Node B: Successfully verified fetched Note content: "${fetchedNote.content}"');
        
        _logger.info('Test PASSED: Outbox update successfully replicated, and content fetched and verified via Bitswap.');

      } catch (e, s) {
        _logger.severe('Test FAILED: Error during outbox replication or Bitswap fetch.', e, s);
        fail('Outbox replication and Bitswap fetch test failed: $e');
      } finally {
        await subscriptionB.cancel();
        await managerBForAOutbox.close();
      }
    }, timeout: Timeout(Duration(seconds: 90)));
  });
}
