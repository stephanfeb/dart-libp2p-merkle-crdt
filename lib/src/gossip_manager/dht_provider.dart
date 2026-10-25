// lib/src/gossip_manager/dht_provider.dart
import 'dart:async';

import 'package:dcid/dcid.dart' show CID;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:dart_libp2p_kad_dht/dart_libp2p_kad_dht.dart' show IpfsDHTv2;
import 'package:logging/logging.dart';

final _logger = Logger('DHTProvider');

/// Optional DHT content routing: announces held blocks and finds providers.
/// Every operation is a no-op when no DHT is configured.
class DHTProvider {
  final IpfsDHTv2? _dht;

  DHTProvider(this._dht);

  /// Whether DHT is available for operations
  bool get isAvailable => _dht != null;

  /// The DHT instance, if any.
  IpfsDHTv2? get dht => _dht;

  /// Finds up to [maxProviders] providers for [cid], giving up after [timeout].
  Stream<AddrInfo> findProviders(CID cid,
      {int maxProviders = 20, Duration timeout = const Duration(seconds: 10)}) {
    final dht = _dht;
    if (dht == null) return const Stream.empty();
    return dht
        .findProvidersAsync(cid.toV1(), maxProviders)
        .timeout(timeout, onTimeout: (sink) => sink.close())
        .handleError((Object e) => _logger.fine('findProviders($cid) failed: $e'));
  }

  /// Announces that this node holds [cid]. Best-effort: failures are logged.
  Future<void> provide(CID cid) async {
    final dht = _dht;
    if (dht == null) return;
    try {
      await dht.provide(cid.toV1(), true);
    } catch (e) {
      _logger.fine('provide($cid) failed: $e');
    }
  }

  /// Gets DHT statistics for monitoring
  Map<String, dynamic> getStats() {
    return {
      'dht_available': isAvailable,
      'dht_type': _dht?.runtimeType.toString() ?? 'none',
    };
  }
}
