import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:dcid/dcid.dart';
import 'package:dart_libp2p/core/host/host.dart';
import 'package:dart_libp2p/core/peer/peer_id.dart';
import 'package:dart_libp2p/core/multiaddr.dart'; // Added import for Multiaddr
import 'package:dart_libp2p/core/peer/addr_info.dart';
import 'package:dart_libp2p/core/network/stream.dart';
import 'package:dart_libp2p/core/network/context.dart';
import 'package:dart_libp2p_kad_dht/dart_libp2p_kad_dht.dart';
import 'package:dart_libp2p_merkle_crdt/src/gossip_manager/block_store.dart';
import 'package:dart_libp2p_merkle_crdt/src/gossip_manager/p2p_stream_dag_fetcher.dart';
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';
import 'package:mockito/src/dummies.dart'; // For provideDummy

// Mocks
@GenerateMocks([
  Host,
  BlockStore,
  IpfsDHTv2,
  PeerId,
  AddrInfo,
  P2PStream, // Assuming P2PStream is the interface for MuxedStream used by the fetcher
  MerkleNode,
  Context,
])
import 'p2p_stream_dag_fetcher_test.mocks.dart';

// Simple CRDTPayload implementation for testing
class TestPayload implements CRDTPayload<String> {
  @override
  final String value;

  TestPayload(this.value);



  @override
  Uint8List toCanonicalBytes() => Uint8List.fromList(utf8.encode(value));

  static TestPayload fromCanonicalBytes(Uint8List bytes) =>
      TestPayload(utf8.decode(bytes));

  @override
  TestPayload merge(covariant TestPayload other) {
    // Simple merge: new value wins
    return TestPayload(other.value);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is TestPayload &&
          runtimeType == other.runtimeType &&
          value == other.value;

  @override
  int get hashCode => value.hashCode;
}

// Payload factory for TestPayload
TestPayload testPayloadFactory(Uint8List payloadBytes) {
  return TestPayload.fromCanonicalBytes(payloadBytes);
}

void main() {
  setUpAll(() {
    // Provide a dummy TestPayload for Mockito
    provideDummy<TestPayload>(TestPayload('dummy_payload_value'));
    // If TestPayload could be null, and that's a valid dummy:
    // provideDummy<TestPayload?>(null); 
    // Or more specific dummy for nullable types if needed:
    provideDummy<TestPayload?>(TestPayload('dummy_nullable_payload_value'));
  });

  group('P2PStreamDagFetcher', () {
    late MockHost mockHost;
    late MockBlockStore<TestPayload, String> mockBlockStore;
    late MockIpfsDHTv2 mockDht;
    late MockPeerId mockLocalPeerId;
    late MockPeerId mockRemotePeerId;
    late P2PStreamDagFetcher<TestPayload, String> fetcher;
    late P2PStreamDagFetcher<TestPayload, String> fetcherWithDht;

    const protocolId = '/test/dag-sync/1.0.0';
    // final testCid = CID.fromString('bafyreigvfiq23i3epm2nwngjdefgptrstcndnb3uwnmcgufm2gf2tgu5hq'); // This was CID of raw payload
    final testNodePayload = TestPayload('hello world');
    // Create an actual node to get its true CID for the test
    final actualTestMerkleNode = MerkleNode.create(testNodePayload, <CID>{});
    final testCid = actualTestMerkleNode.cid; // This is the CID of the dag-pb node structure
    late MockMerkleNode<TestPayload> mockMerkleNode;


    setUp(() {
      mockHost = MockHost();
      mockBlockStore = MockBlockStore<TestPayload, String>();
      mockDht = MockIpfsDHTv2();
      mockLocalPeerId = MockPeerId();
      mockRemotePeerId = MockPeerId();
      mockMerkleNode = MockMerkleNode<TestPayload>();

      when(mockHost.id).thenReturn(mockLocalPeerId);
      when(mockLocalPeerId.toBase58()).thenReturn('local_peer_id_b58');
      when(mockLocalPeerId.toBytes()).thenReturn(Uint8List.fromList(utf8.encode('local_peer_id_bytes')));
      
      when(mockRemotePeerId.toBase58()).thenReturn('remote_peer_id_b58');
      when(mockRemotePeerId.toBytes()).thenReturn(Uint8List.fromList(utf8.encode('remote_peer_id_bytes')));


      when(mockMerkleNode.cid).thenReturn(testCid); // Use the derived dag-pb CID
      when(mockMerkleNode.payload).thenReturn(testNodePayload);
      when(mockMerkleNode.children).thenReturn(<CID>{}); // Use the derived dag-pb CID

      // Default behavior for host.setStreamHandler
      when(mockHost.setStreamHandler(any, any)).thenReturn(null);
      // Default behavior for blockstore.get
      when(mockBlockStore.get(any)).thenAnswer((_) async => null);
      // Default behavior for blockstore.put
      when(mockBlockStore.put(any)).thenAnswer((_) async {});
      // Default behavior for dht.provide
      when(mockDht.provide(any, any)).thenAnswer((_) async {});


      fetcher = P2PStreamDagFetcher<TestPayload, String>(
        mockHost,
        mockBlockStore,
        protocolId,
        testPayloadFactory,
        // No DHT
      );

      fetcherWithDht = P2PStreamDagFetcher<TestPayload, String>(
        mockHost,
        mockBlockStore,
        protocolId,
        testPayloadFactory,
        dht: mockDht,
      );
    });

    group('constructor', () {
      test('should set stream handler on host for each instance created', () {
        // setUp creates two instances, so setStreamHandler is called twice.
        verify(mockHost.setStreamHandler(protocolId, any)).called(2);
      });
    });

    group('get(cid)', () {
      test('Node in BlockStore - should return node from blockstore without network calls', () async {
        when(mockBlockStore.get(testCid)).thenAnswer((_) async => mockMerkleNode);

        final result = await fetcher.get(testCid);

        expect(result, mockMerkleNode);
        verify(mockBlockStore.get(testCid)).called(1);
        verifyNever(mockDht.findProvidersAsync(any, any));
        verifyNever(mockHost.newStream(any, any, any));
      });

      test('Node not in BlockStore, DHT available, provider found - should fetch from provider', () async {
        final mockP2PStream = MockP2PStream();
        final mockAddrInfo = MockAddrInfo();
        
        when(mockAddrInfo.id).thenReturn(mockRemotePeerId);
        // Corrected to use a List for addrs and use MultiAddr constructor
        when(mockAddrInfo.addrs).thenReturn([MultiAddr('/ip4/127.0.0.1/tcp/1234')]); 

        final providersController = StreamController<AddrInfo>();
        when(mockDht.findProvidersAsync(testCid.toV1(), 3)).thenAnswer((_) => providersController.stream);
        
        when(mockHost.connect(mockAddrInfo)).thenAnswer((_) async {});
        when(mockHost.newStream(mockRemotePeerId, [protocolId], any)).thenAnswer((_) async => mockP2PStream);
        
        // Use proper merkledag serialization instead of custom JSON
        final serializedNode = actualTestMerkleNode.toBytes();
        final responseFromServer = 'NODE_DATA ${testCid.toString()} ${base64.encode(serializedNode)}';
        
        when(mockP2PStream.read()).thenAnswer((_) async => Uint8List.fromList(utf8.encode(responseFromServer)));
        when(mockP2PStream.write(any)).thenAnswer((_) async => {}); // Stub write
        when(mockP2PStream.isClosed).thenReturn(false); // Stub isClosed
        when(mockP2PStream.close()).thenAnswer((_) async {}); // Stub close
        when(mockP2PStream.id()).thenReturn('test_stream_id'); // Stub id

        // Start the get operation
        final getFuture = fetcherWithDht.get(testCid);

        // Emit provider immediately to simulate DHT finding a provider
        providersController.add(mockAddrInfo);
        providersController.close();

        final result = await getFuture;

        expect(result, isNotNull);
        expect(result!.cid, testCid);
        expect(result.payload.value, testNodePayload.value);

        verify(mockBlockStore.get(testCid)).called(1);
        verify(mockDht.findProvidersAsync(testCid.toV1(), 3)).called(1);
        verify(mockHost.connect(mockAddrInfo)).called(1);
        verify(mockHost.newStream(mockRemotePeerId, [protocolId], any)).called(1);
        verify(mockP2PStream.write(utf8.encode('REQUEST_NODE ${testCid.toString()}'))).called(1);
        verify(mockP2PStream.read()).called(1);
        verify(mockBlockStore.put(result)).called(1); // Verify it's put into blockstore
      });
      
      // TODO: Add more tests for get(cid):
      // - Node not in BlockStore, DHT available, no providers found
      // - Node not in BlockStore, DHT not available (should return null or throw)
      // - Node not in BlockStore, DHT available, provider fetch fails (network error, timeout, wrong data)
      // - Node not in BlockStore, DHT available, provider returns NODE_NOT_FOUND
      // - Node not in BlockStore, DHT available, provider returns malformed response
    });

    group('put(node)', () {
      test('DHT available - should put to blockstore and provide to DHT', () async {
        await fetcherWithDht.put(mockMerkleNode);

        verify(mockBlockStore.put(mockMerkleNode)).called(1);
        verify(mockDht.provide(testCid.toV1(), true)).called(1);
      });

      test('DHT not available - should only put to blockstore', () async {
        await fetcher.put(mockMerkleNode); // Uses fetcher without DHT

        verify(mockBlockStore.put(mockMerkleNode)).called(1);
        verifyNever(mockDht.provide(any, any));
      });

      test('DHT available, provide fails - should still complete and log (implicitly)', () async {
        // Ensure provide returns a Future that completes with an error
        when(mockDht.provide(any, any)).thenAnswer((_) => Future.error(Exception('DHT provide failed')));
        
        await expectLater(fetcherWithDht.put(mockMerkleNode), completes);
        verify(mockBlockStore.put(mockMerkleNode)).called(1);
        verify(mockDht.provide(testCid.toV1(), true)).called(1);
        // Verification of logging would go here.
      });
    });
    
    group('_handleIncomingStream', () {
        // TODO: Add tests for _handleIncomingStream:
        // - Request for existing node
        // - Request for non-existing node
        // - Request with invalid CID format
        // - Unknown message type
        // - Stream read/write errors
    });

    group('close', () {
      test('should complete (currently no specific action to verify apart from logging)', () async {
        await expectLater(fetcher.close(), completes);
        // Verification of logging or removeStreamHandler if it were used.
      });
    });
  });
}
