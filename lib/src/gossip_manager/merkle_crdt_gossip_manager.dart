// lib/src/gossip_manager/merkle_crdt_gossip_manager.dart
import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';

import 'package:collection/collection.dart' show ListEquality;
import 'package:dcid/dcid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleCRDT, CRDTPayload;
import 'package:logging/logging.dart';

import 'p2p_stream_dag_fetcher.dart';
import 'gossipsub_announcer.dart';

/// Replicates one Merkle-CRDT between peers.
///
/// Local updates are added to the DAG and announced through the
/// [GossipSubAnnouncer]. Each announced head goes through one path: the
/// announcer has already checked the author against its writers; the
/// fetcher asks the announcing peer for the block first, verifies it and
/// stores it; then the CRDT merges it, within its traversal bounds.
/// Announcements of a head already being merged are coalesced, and a head
/// that cannot be fetched is retried with backoff.
///
/// The manager owns [dagFetcher] and [announcer]: [close] closes both.
class MerkleCrdtGossipManager<V, P extends CRDTPayload<V>> {
  final Logger _logger;
  final P2PStreamDagFetcher<P, V> _dagFetcher;
  final GossipSubAnnouncer _announcer;
  late final MerkleCRDT<V, P> _merkleCrdt;

  /// Attempts after the first for a head that cannot be fetched.
  final int maxRetries;

  /// Delay before the first retry; doubles with each further retry.
  final Duration retryDelay;

  final StreamController<V?> _stateController = StreamController<V?>.broadcast();
  final StreamController<String> _cidController = StreamController<String>.broadcast();
  late final StreamSubscription<HeadAnnouncement> _announcementSubscription;

  /// Heads being merged or waiting for a retry.
  final Set<CID> _inFlight = {};

  /// Recently merged (or found already included) heads, so that repeated
  /// announcements of them cost nothing.
  final LinkedHashSet<CID> _settled = LinkedHashSet<CID>();
  static const int _maxSettled = 1024;

  final Set<Timer> _retryTimers = {};
  V? _cachedState;
  Uint8List? _cachedStateBytes;
  Set<CID> _lastHeads = {};
  bool _closed = false;

  /// Stream of CRDT state updates (logical value V). Emits the new state whenever it changes.
  Stream<V?> get onStateChanged => _stateController.stream;

  /// Stream of new head CIDs for this CRDT (after local or remote updates).
  Stream<String> get onCidChanged => _cidController.stream;

  /// The current heads. More than one means concurrent updates that the next
  /// local update will fold together.
  Set<CID> get heads => _merkleCrdt.roots;

  /// One of the current heads, or null before the first update.
  String? get latestKnownCid {
    final roots = _merkleCrdt.roots;
    return roots.isNotEmpty ? roots.first.toString() : null;
  }

  /// The logical value as of the last notification.
  V? get currentState => _cachedState;

  MerkleCrdtGossipManager({
    required P2PStreamDagFetcher<P, V> dagFetcher,
    required GossipSubAnnouncer announcer,
    String? loggerName,
    int maxTraversalNodes = 10000,
    int maxMissingNodes = 8,
    this.maxRetries = 3,
    this.retryDelay = const Duration(seconds: 2),
  })  : _dagFetcher = dagFetcher,
        _announcer = announcer,
        _logger = Logger(loggerName ?? 'MerkleCrdtGossipManager') {
    _merkleCrdt = MerkleCRDT<V, P>(
      dagSyncer: _dagFetcher,
      broadcaster: _announcer,
      subscribe: false,
      maxTraversalNodes: maxTraversalNodes,
      maxMissingNodes: maxMissingNodes,
    );
    _announcementSubscription = _announcer.announcements.listen(_onAnnouncement);
  }

  void _onAnnouncement(HeadAnnouncement announcement) {
    if (_closed) return;
    _dagFetcher.notePeer(announcement.from);
    final cid = announcement.cid;
    if (_settled.contains(cid) || !_inFlight.add(cid)) return;
    unawaited(_ingest(cid, 0));
  }

  Future<void> _ingest(CID cid, int attempt) async {
    var retrying = false;
    try {
      final changed = await _merkleCrdt.merge(cid);
      if (_closed) return;
      if (changed || await _dagFetcher.blockStore.has(cid)) {
        _settle(cid);
        if (changed) await _notifyChange();
        return;
      }
      if (attempt < maxRetries) {
        retrying = true;
        late final Timer timer;
        timer = Timer(retryDelay * (1 << attempt), () {
          _retryTimers.remove(timer);
          if (!_closed) unawaited(_ingest(cid, attempt + 1));
        });
        _retryTimers.add(timer);
      } else {
        _logger.warning('Could not fetch announced head $cid after ${attempt + 1} attempts');
      }
    } catch (e, s) {
      _logger.warning('Merging announced head $cid failed', e, s);
    } finally {
      if (!retrying) _inFlight.remove(cid);
    }
  }

  void _settle(CID cid) {
    _settled.remove(cid);
    _settled.add(cid);
    if (_settled.length > _maxSettled) _settled.remove(_settled.first);
  }

  /// Emits on [onStateChanged] and [onCidChanged] if the state or heads
  /// changed since the last notification.
  Future<void> _notifyChange() async {
    final P? state = await _merkleCrdt.getState();
    if (_closed) return;
    final bytes = state?.toCanonicalBytes();
    if (!const ListEquality<int>().equals(bytes, _cachedStateBytes)) {
      _cachedStateBytes = bytes;
      _cachedState = state?.value;
      _stateController.add(_cachedState);
    }
    final heads = _merkleCrdt.roots;
    if (!_sameSet(heads, _lastHeads)) {
      _lastHeads = heads;
      final cid = latestKnownCid;
      if (cid != null) _cidController.add(cid);
    }
  }

  static bool _sameSet(Set<CID> a, Set<CID> b) => a.length == b.length && a.containsAll(b);

  /// Applies a local update to the CRDT and announces the new head.
  /// The payload `P` should be the CRDT-specific payload type (e.g., GSet, Counter).
  Future<CID> applyLocalUpdate(P payload) async {
    if (_closed) throw StateError('MerkleCrdtGossipManager is closed');
    final cid = await _merkleCrdt.add(payload);
    _settle(cid);
    await _notifyChange();
    return cid;
  }

  /// Retrieves the current merged logical value of the CRDT.
  Future<V?> getState() async => (await _merkleCrdt.getState())?.value;

  /// The current merged payload. Build the next update from it (for example
  /// `(await getPayload())!.delete(key, actorId: me)`) so the update carries
  /// the causal history it replaces.
  Future<P?> getPayload() => _merkleCrdt.getState();

  /// Re-reads the state and notifies listeners if it changed.
  Future<void> refreshState() => _notifyChange();

  /// Announces every current head again, and re-provides them to the DHT if
  /// one is configured. Useful after connectivity returns, since GossipSub
  /// does not replay messages to peers that were offline.
  Future<void> forceAnnounceCurrentHead() async {
    for (final head in _merkleCrdt.roots) {
      await _announcer.broadcast(head.toString());
      await _dagFetcher.dhtProvider.provide(head);
    }
  }

  /// Stops syncing: no more announcements are processed, pending retries are
  /// cancelled, the topic is left and blocks are no longer served.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    for (final timer in _retryTimers) {
      timer.cancel();
    }
    _retryTimers.clear();
    await _announcementSubscription.cancel();
    await _merkleCrdt.close();
    await _announcer.close();
    await _dagFetcher.close();
    await _stateController.close();
    await _cidController.close();
  }
}
