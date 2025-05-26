import 'dart:convert';

import 'package:dart_libp2p/core/host/host.dart';
import 'package:dart_libp2p/core/peer/peer_id.dart';
import 'package:dart_libp2p_merkle_crdt/src/gossip_manager/gossipsub_announcer.dart';
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';
import 'package:matcher/matcher.dart'; // For isA
import 'dart:async'; // For StreamController
import 'dart:typed_data'; // For Uint8List
import 'package:dart_libp2p_pubsub/src/pb/rpc.pb.dart' as rpc; // For rpc.Message

// Generate mocks for Host, PubSub, PeerId, and Subscription
@GenerateMocks([Host, PubSub, PeerId, Subscription])
import 'gossipsub_announcer_test.mocks.dart';

void main() {
  group('GossipSubAnnouncer', () {
    late MockHost mockHost;
    late MockPubSub mockPubSub;
    late PeerId actualHostPeerId;
    late PeerId actualSenderPeerId;
    late GossipSubAnnouncer announcer;
    late MockSubscription mockSubscription;
    late StreamController<dynamic> pubsubStreamController;

    const crdtInstanceId = 'test_crdt_instance';
    const testCid = 'QmTestCid123';
    // IMPORTANT: Replace these placeholders with actual Base58 strings
    const hostPeerIdString = '12D3KooWRpNLL8HCsuhwAXo6MABFW3GGHgUv77zqPV9RXavWqaCQ';
    const senderPeerIdString = '12D3KooWR3vHBGPmQFptY7mG4CdrXQ16PsVh7e1iKjxMfi5Zv4jF';
    final expectedTopic = 'merkledag-crdt/heads/1.0.0/$crdtInstanceId';

    setUp(() {
      mockHost = MockHost();
      mockPubSub = MockPubSub();
      
      // Create actual PeerId instances
      final hostIdBytes = Uint8List.fromList([0x00, 0x01, 0x01]);
      actualHostPeerId = PeerId.fromBytes(hostIdBytes);
      final senderIdBytes = Uint8List.fromList([0x00, 0x01, 0x02]);
      actualSenderPeerId = PeerId.fromBytes(senderIdBytes);

      mockSubscription = MockSubscription();
      pubsubStreamController = StreamController<dynamic>.broadcast(); 

      // Stub mockSubscription's stream first, using thenAnswer as required for Streams
      when(mockSubscription.stream).thenAnswer((_) => pubsubStreamController.stream); 

      // mockHost.id returns the actual PeerId instance
      when(mockHost.id).thenReturn(actualHostPeerId);

      // No longer need to mock toBytes() or toBase58() for mockHostPeerId/mockSenderPeerId
      // as we are using actual PeerId instances or their direct values.

      // Default stub for publish - reverting to any, any as parameters are nullable
      when(mockPubSub.publish(any, argThat(isA<Uint8List?>()))).thenAnswer((_) async {});
      // Default stub for subscribe
      when(mockPubSub.subscribe(any)).thenReturn(mockSubscription);
      // Default stub for unsubscribe
      when(mockPubSub.unsubscribe(any)).thenAnswer((_) async {});


      announcer = GossipSubAnnouncer(mockHost, mockPubSub, crdtInstanceId);
    });

    tearDown(() {
      pubsubStreamController.close();
    });

    group('broadcast', () {
      test('should publish the CID and sender PeerId to the correct topic', () async {
        final expectedPayload = {
          'cid': testCid,
          'senderPeerId': actualHostPeerId.toBase58(), // Use real PeerId's Base58 string
        };
        final expectedMessageBytes = utf8.encode(jsonEncode(expectedPayload));

        await announcer.broadcast(testCid);

        verify(mockPubSub.publish(expectedTopic, expectedMessageBytes)).called(1);
      });

      test('should log an error if publishing fails', () async {
        // Note: Testing logger output directly is often complex and might require
        // a custom LogRecord handler or checking for specific exceptions if the
        // method rethrows them. For now, we'll assume logger.severe is called.
        // This test primarily ensures that an error during publish doesn't crash the app
        // if the method catches it.

        final exception = Exception('Publishing failed');
        // Use specific matchers here too for consistency if the default stubbing was the issue
        when(mockPubSub.publish(any, argThat(isA<Uint8List?>()))).thenThrow(exception);

        // Expect no error to be rethrown from broadcast if it's caught internally
        await expectLater(announcer.broadcast(testCid), completes);
        
        // Verification of logging would typically go here if a logging test framework is set up.
        // For example, using `test współLog` or a similar package.
        // For now, we ensure it doesn't throw an unhandled exception.
      });
    });

    group('subscribe', () {
      test('should subscribe to the correct topic', () {
        announcer.subscribe(); // Call subscribe
        verify(mockPubSub.subscribe(expectedTopic)).called(1);
      });

      test('should yield CID from valid PubSubMessage', () async {
        final stream = announcer.subscribe();
        
        final validPayload = {'cid': testCid, 'senderPeerId': actualSenderPeerId.toBase58()};
        final payloadBytes = utf8.encode(jsonEncode(validPayload));
        final rpcMsg = rpc.Message()
          ..from = actualSenderPeerId.toBytes()
          ..data = payloadBytes
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final pubSubMessage = PubSubMessage(
          rpcMessage: rpcMsg,
          receivedFrom: actualSenderPeerId, // Use actual PeerId
        );

        // Add with a delay to ensure it's processed in a subsequent microtask
        Future.delayed(Duration.zero, () => pubsubStreamController.add(pubSubMessage));

        await expectLater(stream, emits(testCid));
      });

      test('should ignore self-originated messages', () async {
        final stream = announcer.subscribe();
        
        // Message whose original publisher is self (_host.id)
        final selfOriginatedPayload = {'cid': 'QmSelfCid', 'senderPeerId': actualHostPeerId.toBase58()};
        final selfPayloadBytes = utf8.encode(jsonEncode(selfOriginatedPayload));
        final rpcMsgSelf = rpc.Message()
          ..from = actualHostPeerId.toBytes() // Original publisher is self
          ..data = selfPayloadBytes
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final pubSubMessageSelf = PubSubMessage( // Renamed to avoid conflict
          rpcMessage: rpcMsgSelf,
          receivedFrom: actualHostPeerId, // Simulating it was received from self (loopback)
        );

        pubsubStreamController.add(pubSubMessageSelf);
        
        // Add another valid message (not self-originated)
        final validPayloadOther = {'cid': testCid, 'senderPeerId': actualSenderPeerId.toBase58()}; // Renamed
        final validPayloadBytesOther = utf8.encode(jsonEncode(validPayloadOther)); // Renamed
        final rpcMsgValid = rpc.Message()
          ..from = actualSenderPeerId.toBytes() // Original publisher is other peer
          ..data = validPayloadBytesOther
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final validPubSubMessage = PubSubMessage(
          rpcMessage: rpcMsgValid,
          receivedFrom: actualSenderPeerId, // Use actual PeerId
        );
        Future.delayed(Duration.zero, () => pubsubStreamController.add(validPubSubMessage));
        
        // We expect only the testCid from the non-self-originated message
        await expectLater(stream, emits(testCid));
        // To be more robust, ensure no other events before the expected one,
        // or that the 'QmSelfCid' is not emitted. This can be tricky with async emits.
        // One way is to collect events over a short period.
        final collected = await stream.take(1).toList(); // Take the first emitted item
        expect(collected, equals([testCid]));
        expect(collected, isNot(contains('QmSelfCid')));
      });

      test('should log warning and not yield for malformed JSON message', () async {
        final stream = announcer.subscribe();
        final malformedPayloadBytes = utf8.encode("this is not json");
        final rpcMsgMalformed = rpc.Message()
          ..from = actualSenderPeerId.toBytes()
          ..data = malformedPayloadBytes
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final pubSubMessageMalformed = PubSubMessage( // Renamed
          rpcMessage: rpcMsgMalformed,
          receivedFrom: actualSenderPeerId, // Use actual PeerId
        );

        pubsubStreamController.add(pubSubMessageMalformed);
        
        // Add a valid message to ensure the stream can still process
        final validPayloadForMalformedTest = {'cid': testCid, 'senderPeerId': actualSenderPeerId.toBase58()}; // Renamed
        final validPayloadBytesForMalformedTest = utf8.encode(jsonEncode(validPayloadForMalformedTest)); // Renamed
        final rpcMsgValidAfterMalformed = rpc.Message()
          ..from = actualSenderPeerId.toBytes()
          ..data = validPayloadBytesForMalformedTest
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final validPubSubMessageAfterMalformed = PubSubMessage( // Renamed
          rpcMessage: rpcMsgValidAfterMalformed,
          receivedFrom: actualSenderPeerId, // Use actual PeerId
        );
        Future.delayed(Duration.zero, () => pubsubStreamController.add(validPubSubMessageAfterMalformed));

        // Expect only the CID from the valid message
        await expectLater(stream, emits(testCid));
        // Verification of logging would go here.
      });

      test('should log warning and not yield if JSON does not contain CID', () async {
        final stream = announcer.subscribe();
        final noCidPayload = {'some_other_field': 'value', 'senderPeerId': actualSenderPeerId.toBase58()};
        final noCidPayloadBytes = utf8.encode(jsonEncode(noCidPayload));
        final rpcMsgNoCid = rpc.Message()
          ..from = actualSenderPeerId.toBytes()
          ..data = noCidPayloadBytes
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final pubSubMessageNoCid = PubSubMessage( // Renamed
          rpcMessage: rpcMsgNoCid,
          receivedFrom: actualSenderPeerId, // Use actual PeerId
        );

        pubsubStreamController.add(pubSubMessageNoCid);

        final validPayloadForNoCidTest = {'cid': testCid, 'senderPeerId': actualSenderPeerId.toBase58()}; // Renamed
        final validPayloadBytesForNoCidTest = utf8.encode(jsonEncode(validPayloadForNoCidTest)); // Renamed
        final rpcMsgValidAfterNoCid = rpc.Message()
          ..from = actualSenderPeerId.toBytes()
          ..data = validPayloadBytesForNoCidTest
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final validPubSubMessageAfterNoCid = PubSubMessage( // Renamed
          rpcMessage: rpcMsgValidAfterNoCid,
          receivedFrom: actualSenderPeerId, // Use actual PeerId
        );
        Future.delayed(Duration.zero, () => pubsubStreamController.add(validPubSubMessageAfterNoCid));
        
        await expectLater(stream, emits(testCid));
        // Verification of logging.
      });

      test('should log warning and filter out non-PubSubMessage items from raw stream', () async {
        final stream = announcer.subscribe();
        
        pubsubStreamController.add("not a pubsub message object"); // Add invalid data type

        final validPayloadForInvalidItemTest = {'cid': testCid, 'senderPeerId': actualSenderPeerId.toBase58()}; // Renamed
        final validPayloadBytesForInvalidItemTest = utf8.encode(jsonEncode(validPayloadForInvalidItemTest)); // Renamed
        final rpcMsgValidAfterInvalid = rpc.Message()
          ..from = actualSenderPeerId.toBytes()
          ..data = validPayloadBytesForInvalidItemTest
          ..seqno = Uint8List(0)
          ..topic = expectedTopic;
        final validPubSubMessageAfterInvalid = PubSubMessage( // Renamed
          rpcMessage: rpcMsgValidAfterInvalid,
          receivedFrom: actualSenderPeerId, // Use actual PeerId
        );
        Future.delayed(Duration.zero, () => pubsubStreamController.add(validPubSubMessageAfterInvalid));

        await expectLater(stream, emits(testCid));
        // Verification of logging.
      });

       test('should return Stream.error if _pubsub.subscribe throws', () {
        final exception = Exception('Subscription failed');
        when(mockPubSub.subscribe(expectedTopic)).thenThrow(exception);

        final stream = announcer.subscribe();

        expect(stream, emitsError(isA<Exception>()));
      });

    });

    group('close', () {
      test('should unsubscribe from the topic', () async {
        // Call subscribe first to ensure there's a subscription to close
        announcer.subscribe(); 
        await announcer.close();
        verify(mockPubSub.unsubscribe(expectedTopic)).called(1);
      });

      test('should not throw if unsubscribe fails', () async {
        when(mockPubSub.unsubscribe(any)).thenThrow(Exception('Unsubscribe failed'));
        
        announcer.subscribe(); // Ensure subscription exists
        await expectLater(announcer.close(), completes);
        // Verification of logging for the error would go here.
      });
    });
  });
}
