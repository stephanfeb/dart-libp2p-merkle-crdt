// lib/src/gossip_manager/gossipsub_announcer.dart
import 'dart:async';
import 'dart:convert'; // For jsonEncode, jsonDecode, utf8
import 'dart:math' as math; // For math.min

import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart' show PubSub, PubSubMessage, Subscription;
import 'package:merkledag/merkledag.dart' show Broadcaster;
import 'package:logging/logging.dart';

class GossipSubAnnouncer implements Broadcaster {
  static final _logger = Logger('GossipSubAnnouncer');
  final Host _host;
  final PubSub _pubsub;
  final String _crdtInstanceTopic;

  GossipSubAnnouncer(this._host, this._pubsub, String crdtInstanceId, {String? topic})
      : _crdtInstanceTopic = topic ?? 'merkledag-crdt/heads/1.0.0/$crdtInstanceId' {
    _logger.fine('GossipSubAnnouncer: Constructor started. CRDT Instance ID: $crdtInstanceId, Topic: $_crdtInstanceTopic');
    _logger.info('GossipSubAnnouncer: Initialized for topic $_crdtInstanceTopic');
  }

  @override
  Future<void> broadcast(String cidString) async {
    _logger.info('broadcast: Broadcasting CID $cidString on topic $_crdtInstanceTopic');
    final announcement = {
      'cid': cidString,
      'senderPeerId': _host.id.toBase58(),
    };
    final messageBytes = utf8.encode(jsonEncode(announcement));
    _logger.finer('broadcast: Encoded message for CID $cidString: ${announcement.toString()} (Bytes length: ${messageBytes.length})');

    try {
      await _pubsub.publish(_crdtInstanceTopic, messageBytes);
      _logger.fine('broadcast: Successfully broadcasted CID $cidString on topic $_crdtInstanceTopic');
    } catch (e, s) {
      _logger.severe('broadcast: Error broadcasting CID $cidString on topic $_crdtInstanceTopic: $e', e, s);
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
      return Stream.error(e, s);
    }

    Stream<String> _processMessage(PubSubMessage message) async* {
      final hostIdStr = _host.id.toBase58();
      final messageFromStr = message.from.toBase58();
      final hostIdShort = hostIdStr.substring(0, math.min(hostIdStr.length, 6));
      final messageFromShort = messageFromStr.substring(0, math.min(messageFromStr.length, 6));

      _logger.info('[GossipSubAnnouncer] Node $hostIdShort: _processMessage: Received PubSubMessage from $messageFromShort on topic $_crdtInstanceTopic (subscribed). Data length: ${message.data.length}. (isMine: ${message.from == _host.id})');
      _logger.finer('_processMessage: Received PubSubMessage from $messageFromStr on topic $_crdtInstanceTopic. Data length: ${message.data.length}');
      
      if (message.from == _host.id) {
        _logger.finer('_processMessage: Ignoring self-originated head announcement from ${message.from.toBase58()} on topic $_crdtInstanceTopic.');
        return;
      }

      try {
        final messageString = utf8.decode(message.data);
        _logger.finer('_processMessage: Decoded message string: $messageString');
        final announcement = jsonDecode(messageString) as Map<String, dynamic>;
        _logger.finer('_processMessage: Parsed announcement: $announcement');
        
        final cidString = announcement['cid'] as String?;
        final senderPeerIdString = announcement['senderPeerId'] as String?;

        if (cidString == null) {
          _logger.warning('_processMessage: Received announcement on $_crdtInstanceTopic without a CID from ${message.from.toBase58()}. Message data: $messageString');
          return;
        }

        if (senderPeerIdString != null) {
              _logger.fine('_processMessage: Received CID $cidString from sender $senderPeerIdString (via payload) on topic $_crdtInstanceTopic. Actual sender (message.from): ${message.from.toBase58()}');
        } else {
              _logger.fine('_processMessage: Received CID $cidString from sender ${message.from.toBase58()} on topic $_crdtInstanceTopic (payload did not contain senderPeerId)');
        }
        
        yield cidString;
        _logger.finer('_processMessage: Yielded CID $cidString for topic $_crdtInstanceTopic.');
      } catch (e, s) {
        _logger.warning('_processMessage: Error processing received message on topic $_crdtInstanceTopic from ${message.from.toBase58()}: $e. Message data (UTF-8 decoded, if possible): ${utf8.decode(message.data, allowMalformed: true)}', e, s);
      }
    }

    return subscription.stream.transform(
      StreamTransformer<dynamic, PubSubMessage>.fromHandlers(
        handleData: (data, sink) {
          if (data is PubSubMessage) {
            _logger.finer('GossipSubAnnouncer: StreamTransformer received PubSubMessage for topic $_crdtInstanceTopic.');
            sink.add(data);
          } else {
            _logger.warning('GossipSubAnnouncer: StreamTransformer received unexpected data type on PubSub stream for topic $_crdtInstanceTopic: ${data.runtimeType}. Expected PubSubMessage. Data: ${data.toString()}');
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
    try {
      _logger.fine('close: Attempting to unsubscribe from topic $_crdtInstanceTopic.');
      await _pubsub.unsubscribe(_crdtInstanceTopic); 
      _logger.info('close: Successfully unsubscribed from topic $_crdtInstanceTopic.');
    } catch (e, s) {
      _logger.warning('close: Error unsubscribing from topic $_crdtInstanceTopic during close: $e', e, s);
    }
    _logger.info('close: GossipSubAnnouncer closed for topic $_crdtInstanceTopic.');
  }
}
