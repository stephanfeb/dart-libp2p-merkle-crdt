// lib/src/gossip_manager/writer_authorizer.dart
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;

/// Decides which peers may publish heads for a CRDT.
///
/// Checked against the author of each head announcement (`message.from`),
/// which GossipSub authenticates when messages are signed (StrictSign, the
/// default). Announcements from other peers are dropped before anything is
/// fetched, and are not relayed. The content behind an accepted head is
/// covered by hash verification, so authorising the head authorises its DAG.
abstract class WriterAuthorizer {
  const WriterAuthorizer();

  /// Accepts heads from [writers] only.
  factory WriterAuthorizer.only(Iterable<PeerId> writers) = PeerIdAllowlist;

  /// Accepts heads from any peer. Only for open CRDTs where any participant
  /// may write.
  const factory WriterAuthorizer.allowAll() = _AllowAll;

  /// Whether [peer] may publish heads.
  bool isAllowed(PeerId peer);
}

/// A fixed set of peers allowed to write.
class PeerIdAllowlist extends WriterAuthorizer {
  final Set<String> _writers;

  PeerIdAllowlist(Iterable<PeerId> writers)
      : _writers = {for (final w in writers) w.toBase58()};

  @override
  bool isAllowed(PeerId peer) => _writers.contains(peer.toBase58());
}

class _AllowAll extends WriterAuthorizer {
  const _AllowAll();

  @override
  bool isAllowed(PeerId peer) => true;
}
