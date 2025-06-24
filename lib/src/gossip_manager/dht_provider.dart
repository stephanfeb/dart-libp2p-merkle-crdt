// lib/src/gossip_manager/dht_provider.dart
import 'dart:async';

import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHT;
import 'package:logging/logging.dart';

final _logger = Logger('DHTProvider');

/// Handles DHT operations for content discovery and providing.
/// Encapsulates all DHT-related functionality with proper error handling.
class DHTProvider {
  final IpfsDHT? _dht;

  DHTProvider(this._dht) {
    _logger.info('DHTProvider: Initialized with DHT ${_dht != null ? 'available' : 'unavailable'}');
  }

  /// Whether DHT is available for operations
  bool get isAvailable => _dht != null;

  /// Public getter for the DHT instance (for backward compatibility)
  IpfsDHT? get dht => _dht;

  /// Finds providers for a given CID
  /// Returns a stream of provider addresses
  Stream<AddrInfo> findProviders(CID cid, {int maxProviders = 20, Duration timeout = const Duration(seconds: 10)}) {
    if (_dht == null) {
      _logger.warning('findProviders: DHT not available for CID $cid');
      return Stream.empty();
    }

    _logger.info('findProviders: Finding providers for CID $cid (max: $maxProviders, timeout: ${timeout.inSeconds}s)');

    return _dht!.findProvidersAsync(cid.toV1(), maxProviders).timeout(
      timeout,
      onTimeout: (EventSink<AddrInfo> sink) {
        _logger.warning('findProviders: Timeout finding providers for $cid after ${timeout.inSeconds}s');
        sink.close();
      },
    ).handleError((error, stackTrace) {
      _logger.warning('findProviders: Error finding providers for $cid: $error', error, stackTrace);
    });
  }

  /// Provides a CID to the DHT network
  /// Announces that this node has the content for the given CID
  Future<void> provide(CID cid) async {
    if (_dht == null) {
      _logger.warning('provide: DHT not available, cannot provide CID $cid');
      return;
    }

    _logger.info('provide: Providing CID $cid to DHT');
    
    try {
      await _dht!.provide(cid.toV1(), true);
      _logger.fine('provide: Successfully provided CID $cid to DHT');
    } catch (e, s) {
      _logger.warning('provide: Failed to provide CID $cid to DHT: $e', e, s);
      // Don't rethrow - providing is best-effort
    }
  }

  /// Provides multiple CIDs in batch
  /// More efficient than individual provide calls
  Future<void> provideBatch(Iterable<CID> cids) async {
    if (_dht == null) {
      _logger.warning('provideBatch: DHT not available, cannot provide ${cids.length} CIDs');
      return;
    }

    _logger.info('provideBatch: Providing ${cids.length} CIDs to DHT');
    
    final futures = cids.map((cid) => provide(cid));
    await Future.wait(futures, eagerError: false);
    
    _logger.fine('provideBatch: Completed batch provide for ${cids.length} CIDs');
  }

  /// Gets DHT statistics for monitoring
  Map<String, dynamic> getStats() {
    return {
      'dht_available': isAvailable,
      'dht_type': _dht?.runtimeType.toString() ?? 'none',
    };
  }

  /// Closes the DHT provider
  Future<void> close() async {
    _logger.info('close: Closing DHTProvider');
    // DHT cleanup is typically handled by the host
    _logger.info('DHTProvider closed');
  }
}
