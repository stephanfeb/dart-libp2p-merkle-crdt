// lib/src/gossip_manager/gossipsub_announcer.dart
import 'dart:async';
import 'dart:convert'; // For jsonEncode, jsonDecode, utf8

import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart' show PubSub, PubSubMessage, Subscription;
import 'package:merkledag/merkledag.dart' show Broadcaster;
import 'package:logging/logging.dart';

// Assuming the top-level _logger is intended for this class.
// If not, uncomment and use:
// static final _logger = Logger('GossipSubAnnouncer');

class GossipSubAnnouncer implements Broadcaster {
  static final _logger = Logger('GossipSubAnnouncer'); // Ensure logger is specific to this class or correctly scoped
  final Host _host;
  final PubSub _pubsub;
  final String _crdtInstanceTopic; // Topic specific to this CRDT instance

  GossipSubAnnouncer(this._host, this._pubsub, String crdtInstanceId)
      : _crdtInstanceTopic = 'merkledag-crdt/heads/1.0.0/$crdtInstanceId' {
    _logger.fine('GossipSubAnnouncer: Constructor started. CRDT Instance ID: $crdtInstanceId, Topic: $_crdtInstanceTopic');
    _logger.info('GossipSubAnnouncer: Initialized for topic $_crdtInstanceTopic');
  }

  @override
  Future<void> broadcast(String cidString) async {
    _logger.info('broadcast: Broadcasting CID $cidString on topic $_crdtInstanceTopic');
    final announcement = {
      'cid': cidString,
      'senderPeerId': _host.id.toBase58(), // Include sender's PeerId
    };
    final messageBytes = utf8.encode(jsonEncode(announcement));
    _logger.finer('broadcast: Encoded message for CID $cidString: ${announcement.toString()} (Bytes length: ${messageBytes.length})');

    try {
      await _pubsub.publish(_crdtInstanceTopic, messageBytes);
      _logger.fine('broadcast: Successfully broadcasted CID $cidString on topic $_crdtInstanceTopic');
    } catch (e, s) {
      _logger.severe('broadcast: Error broadcasting CID $cidString on topic $_crdtInstanceTopic: $e', e, s);
      // Depending on policy, might rethrow or handle
    }
  }

  @override
  Stream<String> subscribe() {
    _logger.info('subscribe: Subscribing to Merkle-CRDT head announcements on topic: $_crdtInstanceTopic');
    
    late Subscription subscription;
    try {
      subscription = _pubsub.subscribe(_crdtInstanceTopic);
      _logger.fine('subscribe: Successfully initiated subscription to topic $_crdtInstanceTopic.');
    } catch (e, s) {
      _logger.severe('subscribe: Error subscribing to topic $_crdtInstanceTopic: $e', e, s);
      return Stream.error(e, s); // Return an error stream
    }

    Stream<String> _processMessage(PubSubMessage message) async* {
      _logger.finer('_processMessage: Received PubSubMessage from ${message.from.toBase58()} on topic $_crdtInstanceTopic. Data length: ${message.data.length}');
      // Don't process messages from self if they somehow get through pubsub loopback
      if (message.from == _host.id) {
        _logger.finer('_processMessage: Ignoring self-originated head announcement from ${message.from.toBase58()} on topic $_crdtInstanceTopic.');
        return; // Stop processing this message, yields an empty stream for this event
      }

      try {
        final messageString = utf8.decode(message.data);
        _logger.finer('_processMessage: Decoded message string: $messageString');
        final announcement = jsonDecode(messageString) as Map<String, dynamic>;
        _logger.finer('_processMessage: Parsed announcement: $announcement');
        
        final cidString = announcement['cid'] as String?;
        final senderPeerIdString = announcement['senderPeerId'] as String?; // Optional

        if (cidString == null) {
          _logger.warning('_processMessage: Received announcement on $_crdtInstanceTopic without a CID from ${message.from.toBase58()}. Message data: $messageString');
          return; // Stop processing this message, yields an empty stream for this event
        }

        // Optional: Log sender if available in payload, though message.from is more reliable
        if (senderPeerIdString != null) {
              _logger.fine('_processMessage: Received CID $cidString from sender $senderPeerIdString (via payload) on topic $_crdtInstanceTopic. Actual sender (message.from): ${message.from.toBase58()}');
        } else {
              _logger.fine('_processMessage: Received CID $cidString from sender ${message.from.toBase58()} on topic $_crdtInstanceTopic (payload did not contain senderPeerId)');
        }
        
        yield cidString; // Emit the CID string
        _logger.finer('_processMessage: Yielded CID $cidString for topic $_crdtInstanceTopic.');
      } catch (e, s) {
        _logger.warning('_processMessage: Error processing received message on topic $_crdtInstanceTopic from ${message.from.toBase58()}: $e. Message data (UTF-8 decoded, if possible): ${utf8.decode(message.data, allowMalformed: true)}', e, s);
        // Do not yield anything if there's an error processing the message, yields an empty stream for this event
      }
    }

    // Safely transform the dynamic stream to Stream<PubSubMessage>
    // and then process valid messages.
    return subscription.stream.transform(
      StreamTransformer<dynamic, PubSubMessage>.fromHandlers(
        handleData: (data, sink) {
          if (data is PubSubMessage) {
            _logger.finer('GossipSubAnnouncer: StreamTransformer received PubSubMessage for topic $_crdtInstanceTopic.');
            sink.add(data);
          } else {
            _logger.warning('GossipSubAnnouncer: StreamTransformer received unexpected data type on PubSub stream for topic $_crdtInstanceTopic: ${data.runtimeType}. Expected PubSubMessage. Data: ${data.toString()}');
            // Optionally, if strict error handling is needed:
            // sink.addError(TypeError()); 
          }
        },
        handleError: (error, stackTrace, sink) {
          _logger.severe('GossipSubAnnouncer: StreamTransformer error in PubSub stream for topic $_crdtInstanceTopic: $error', error, stackTrace);
          sink.addError(error, stackTrace);
        },
        handleDone: (sink) {
          _logger.fine('GossipSubAnnouncer: StreamTransformer PubSub stream for topic $_crdtInstanceTopic is done.');
          sink.close();
        },
      ),
    ).asyncExpand<String>(_processMessage);
  }

  Future<void> close() async {
    _logger.info('close: Closing GossipSubAnnouncer for topic $_crdtInstanceTopic...');
    // PubSub.unsubscribe is not strictly necessary if the PubSub instance itself is being stopped/disposed elsewhere.
    // However, if this announcer instance has a shorter lifecycle than the PubSub service,
    // then unsubscribing is good practice.
    try {
      _logger.fine('close: Attempting to unsubscribe from topic $_crdtInstanceTopic.');
      // Note: _pubsub.unsubscribe returns Future<void>
      await _pubsub.unsubscribe(_crdtInstanceTopic); 
      _logger.info('close: Successfully unsubscribed from topic $_crdtInstanceTopic.');
    } catch (e, s) {
      _logger.warning('close: Error unsubscribing from topic $_crdtInstanceTopic during close: $e', e, s);
      // Continue closing even if unsubscribe fails
    }
    _logger.info('close: GossipSubAnnouncer closed for topic $_crdtInstanceTopic.');
  }
}
