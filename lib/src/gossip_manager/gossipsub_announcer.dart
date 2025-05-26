// lib/src/gossip_manager/gossipsub_announcer.dart
import 'dart:async';
import 'dart:convert'; // For jsonEncode, jsonDecode, utf8

import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart' show PubSub, PubSubMessage, Subscription;
import 'package:merkledag/merkledag.dart' show Broadcaster;
import 'package:logging/logging.dart';

final _logger = Logger('GossipSubAnnouncer');

class GossipSubAnnouncer implements Broadcaster {
  final Host _host;
  final PubSub _pubsub;
  final String _crdtInstanceTopic; // Topic specific to this CRDT instance

  GossipSubAnnouncer(this._host, this._pubsub, String crdtInstanceId)
      : _crdtInstanceTopic = 'merkledag-crdt/heads/1.0.0/$crdtInstanceId';

  @override
  Future<void> broadcast(String cidString) async {
    final announcement = {
      'cid': cidString,
      'senderPeerId': _host.id.toBase58(), // Include sender's PeerId
    };
    final messageBytes = utf8.encode(jsonEncode(announcement));

    try {
      _logger.info('Broadcasting CID $cidString on topic $_crdtInstanceTopic');
      await _pubsub.publish(_crdtInstanceTopic, messageBytes);
      _logger.fine('Successfully broadcasted CID $cidString');
    } catch (e, s) {
      _logger.severe('Error broadcasting CID $cidString on topic $_crdtInstanceTopic: $e', e, s);
      // Depending on policy, might rethrow or handle
    }
  }

  @override
  Stream<String> subscribe() {
    _logger.info('Subscribing to Merkle-CRDT head announcements on topic: $_crdtInstanceTopic');
    
    late Subscription subscription;
    try {
      subscription = _pubsub.subscribe(_crdtInstanceTopic);
    } catch (e, s) {
      _logger.severe('Error subscribing to topic $_crdtInstanceTopic: $e', e, s);
      return Stream.error(e, s); // Return an error stream
    }

    Stream<String> _processMessage(PubSubMessage message) async* {
      // Don't process messages from self if they somehow get through pubsub loopback
      if (message.from == _host.id) {
        _logger.finer('Ignoring self-originated head announcement from ${message.from.toBase58()}');
        return; // Stop processing this message, yields an empty stream for this event
      }

      try {
        final messageString = utf8.decode(message.data);
        final announcement = jsonDecode(messageString) as Map<String, dynamic>;
        
        final cidString = announcement['cid'] as String?;
        final senderPeerIdString = announcement['senderPeerId'] as String?; // Optional

        if (cidString == null) {
          _logger.warning('Received announcement on $_crdtInstanceTopic without a CID from ${message.from.toBase58()}');
          return; // Stop processing this message, yields an empty stream for this event
        }

        // Optional: Log sender if available in payload, though message.from is more reliable
        if (senderPeerIdString != null) {
              _logger.fine('Received CID $cidString from sender $senderPeerIdString (via payload) on topic $_crdtInstanceTopic. Actual sender (message.from): ${message.from.toBase58()}');
        } else {
              _logger.fine('Received CID $cidString from sender ${message.from.toBase58()} on topic $_crdtInstanceTopic (payload did not contain senderPeerId)');
        }
        
        yield cidString; // Emit the CID string
      } catch (e, s) {
        _logger.warning('Error processing received message on topic $_crdtInstanceTopic from ${message.from.toBase58()}: $e', e, s);
        // Do not yield anything if there's an error processing the message, yields an empty stream for this event
      }
    }

    // Explicitly cast the stream to ensure the type is correctly understood by asyncExpand
    final Stream<PubSubMessage> typedStream = subscription.stream as Stream<PubSubMessage>;
    return typedStream.asyncExpand<String>(_processMessage);
  }

  Future<void> close() async {
    // PubSub.unsubscribe is not strictly necessary if the PubSub instance itself is being stopped/disposed elsewhere.
    // However, if this announcer instance has a shorter lifecycle than the PubSub service,
    // then unsubscribing is good practice.
    try {
      _logger.info('Unsubscribing from topic $_crdtInstanceTopic');
      // Note: _pubsub.unsubscribe returns Future<void>
      await _pubsub.unsubscribe(_crdtInstanceTopic); 
    } catch (e, s) {
      _logger.warning('Error unsubscribing from topic $_crdtInstanceTopic during close: $e', e, s);
      // Continue closing even if unsubscribe fails
    }
    _logger.info('GossipSubAnnouncer closed for topic $_crdtInstanceTopic.');
  }
}
