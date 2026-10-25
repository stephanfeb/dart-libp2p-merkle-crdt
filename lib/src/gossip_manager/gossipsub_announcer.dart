// lib/src/gossip_manager/gossipsub_announcer.dart
import 'dart:async';
import 'dart:typed_data';

import 'package:dcid/dcid.dart' show CID;
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart';
import 'package:merkledag/merkledag.dart' show Broadcaster;
import 'package:logging/logging.dart';

import 'writer_authorizer.dart';

final _logger = Logger('GossipSubAnnouncer');

/// A head CID announced by [from], the authenticated author of the message.
class HeadAnnouncement {
  final CID cid;
  final PeerId from;

  const HeadAnnouncement(this.cid, this.from);

  @override
  String toString() => 'HeadAnnouncement($cid from ${from.toBase58()})';
}

/// Announces and receives head CIDs over a GossipSub topic.
///
/// Each message is the raw bytes of one CID. A topic validator accepts only
/// well-formed CIDs authored by peers that [writers] allows, so other
/// announcements are neither delivered nor relayed. GossipSub must sign
/// messages (StrictSign, the default) for the author to be authenticated.
class GossipSubAnnouncer implements Broadcaster {
  final Host _host;
  final PubSub _pubsub;
  final String _topic;
  final WriterAuthorizer _writers;

  final StreamController<HeadAnnouncement> _announcements =
      StreamController<HeadAnnouncement>.broadcast();
  Subscription? _subscription;
  StreamSubscription<dynamic>? _listener;
  bool _validatorRegistered = false;
  bool _closed = false;

  GossipSubAnnouncer(
    this._host,
    this._pubsub,
    String crdtInstanceId, {
    String? topic,
    required WriterAuthorizer writers,
  })  : _topic = topic ?? 'merkle-crdt/heads/2.0.0/$crdtInstanceId',
        _writers = writers;

  /// The GossipSub topic heads are announced on.
  String get topic => _topic;

  /// Announcements from allowed writers other than this host. Joins the
  /// topic on first use.
  Stream<HeadAnnouncement> get announcements {
    _join();
    return _announcements.stream;
  }

  void _join() {
    if (_closed || _subscription != null) return;
    try {
      _pubsub.registerTopicValidator(_topic, _validate);
      _validatorRegistered = true;
    } on StateError catch (e) {
      // Another component already validates this topic; _accept still
      // filters what is delivered here.
      _logger.warning('Topic $_topic already has a validator: $e');
    }
    final subscription = _pubsub.subscribe(_topic);
    _subscription = subscription;
    _listener = subscription.stream.listen((message) {
      if (message is! PubSubMessage) return;
      final announcement = _accept(message);
      if (announcement != null && !_announcements.isClosed) {
        _announcements.add(announcement);
      }
    });
  }

  ValidationResult _validate(PeerId receivedFrom, PubSubMessage message) {
    if (message.from == _host.id) return ValidationResult.accept;
    if (_parse(message.data) == null) return ValidationResult.reject;
    // Valid but unwanted: drop it without penalising the peer that relayed it.
    return _writers.isAllowed(message.from)
        ? ValidationResult.accept
        : ValidationResult.ignore;
  }

  HeadAnnouncement? _accept(PubSubMessage message) {
    final from = message.from;
    if (from == _host.id || !_writers.isAllowed(from)) return null;
    final cid = _parse(message.data);
    return cid == null ? null : HeadAnnouncement(cid, from);
  }

  static CID? _parse(List<int> data) {
    if (data.isEmpty || data.length > 128) return null;
    try {
      return CID.fromBytes(Uint8List.fromList(data));
    } catch (_) {
      return null;
    }
  }

  /// Announces [cidString] as a head. Failures are logged, not thrown: a
  /// later announcement or [MerkleCrdtGossipManager.forceAnnounceCurrentHead]
  /// recovers.
  @override
  Future<void> broadcast(String cidString) async {
    if (_closed) return;
    _join();
    try {
      await _pubsub.publish(_topic, CID.fromString(cidString).toBytes());
    } catch (e) {
      _logger.warning('Announcing $cidString on $_topic failed: $e');
    }
  }

  /// The CIDs of [announcements], as merkledag's [Broadcaster] expects.
  @override
  Stream<String> subscribe() => announcements.map((a) => a.cid.toString());

  /// Leaves the topic and stops delivering announcements.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    if (_validatorRegistered) _pubsub.unregisterTopicValidator(_topic);
    await _listener?.cancel();
    if (_subscription != null) {
      try {
        await _pubsub.unsubscribe(_topic);
      } catch (e) {
        _logger.fine('Unsubscribing from $_topic failed: $e');
      }
    }
    await _announcements.close();
  }
}
