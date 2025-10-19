import 'dart:async'; // For Completer
import 'dart:typed_data'; // For Uint8List

import './proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart'; 

// Represents an entry in a wantlist
class WantlistEntry {
  final CID cid; 
  final int priority;
  final pb.Message_Wantlist_WantType wantType;
  final bool sendDontHave;
  final Set<String> peersSentTo; // Peers to whom this want has been sent
  final Completer<Uint8List> completer; // For notifying when block is received

  WantlistEntry({
    required this.cid, 
    this.priority = 1,
    this.wantType = pb.Message_Wantlist_WantType.Block,
    this.sendDontHave = false,
  }) : peersSentTo = {}, completer = Completer<Uint8List>();

  @override
  String toString() {
    return 'WantlistEntry(cid: $cid, priority: $priority, type: $wantType, sendDontHave: $sendDontHave, peersSentTo: ${peersSentTo.length}, hasCompleter: ${completer != null})';
  }
}

class WantManager {
  // Blocks our local node wants from the network
  // Key: CID as String
  final Map<String, WantlistEntry> _localWantlist = {};

  // Blocks remote peers want from our local node
  // Key: PeerId (String), Value: Map<String (CID), WantlistEntry>
  final Map<String, Map<String, WantlistEntry>> _peerWantlists = {};

  WantManager() {
    print('// BSW_LOG: WantManager instance created.');
    print('WantManager instance created.');
  }

  // --- Local Wantlist Management ---

  // Add a block to the local wantlist
  // Returns the entry. If it already existed with the same wantType, returns the existing entry.
  WantlistEntry addLocalWant(CID cid, { 
    int priority = 1,
    pb.Message_Wantlist_WantType wantType = pb.Message_Wantlist_WantType.Block,
    bool sendDontHave = false,
  }) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: addLocalWant for $cidStr. Priority: $priority, Type: $wantType, SendDontHave: $sendDontHave');
    if (_localWantlist.containsKey(cidStr) && _localWantlist[cidStr]!.wantType == wantType) {
      // Already wanting with same type, return existing entry
      print('// BSW_LOG: WantManager: Local want for $cidStr already exists with same type. Returning existing entry.');
      print('Local want for $cidStr already exists with same type. Returning existing entry.');
      return _localWantlist[cidStr]!;
    }
    // If it exists but with a different wantType, or doesn't exist, create/overwrite.
    final entry = WantlistEntry(
      cid: cid, 
      priority: priority,
      wantType: wantType,
      sendDontHave: sendDontHave,
    );
    _localWantlist[cidStr] = entry;
    print('// BSW_LOG: WantManager: Added/Updated local wantlist for: $cidStr. Entry: $entry');
    print('Added/Updated local wantlist for: $cidStr');
    return entry;
  }

  Future<Uint8List> whenBlockReceived(CID cid) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: whenBlockReceived called for $cidStr.');
    final entry = _localWantlist[cidStr];
    if (entry != null) {
      print('// BSW_LOG: WantManager: Found entry for $cidStr in local wantlist. Returning completer future.');
      return entry.completer.future;
    }
    // If not currently wanting, return a future that will complete with an error.
    // This indicates a programming error if getBlock is called without wantBlocks ensuring the want exists.
    print('// BSW_LOG: WantManager: Block $cidStr was NOT in the local wantlist when whenBlockReceived was called. Returning Future.error.');
    return Future.error(StateError('Block $cidStr was not in the local wantlist when whenBlockReceived was called.'));
  }

  // Call this when a local want is sent to a specific peer
  void recordWantSentToPeer(CID cid, String peerId) { 
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: recordWantSentToPeer for CID $cidStr to Peer $peerId.');
    _localWantlist[cidStr]?.peersSentTo.add(peerId);
  }

  Set<String> getPeersSentWantForCid(CID cid) { // Changed Cid to CID
    final cidStr = cid.toString();
    final peers = _localWantlist[cidStr]?.peersSentTo ?? {};
    print('// BSW_LOG: WantManager: getPeersSentWantForCid for $cidStr. Peers: $peers');
    return Set.unmodifiable(peers);
  }

  // Remove a block from the local wantlist (e.g., block received or cancelled by local client)
  WantlistEntry? removeLocalWant(CID cid) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: removeLocalWant for $cidStr.');
    final removed = _localWantlist.remove(cidStr);
    if (removed != null) {
      print('// BSW_LOG: WantManager: Removed $cidStr from local wantlist.');
      print('Removed from local wantlist: ${cid.toString()}');
    } else {
      print('// BSW_LOG: WantManager: removeLocalWant: $cidStr was not in local wantlist.');
    }
    return removed;
  }

  bool isWantingLocally(CID cid) {
    final cidStr = cid.toString();
    final isWanting = _localWantlist.containsKey(cidStr);
    print('// BSW_LOG: WantManager: isWantingLocally for $cidStr? $isWanting');
    return isWanting;
  }

  List<WantlistEntry> getLocalWants() {
    final wants = _localWantlist.values.toList();
    print('// BSW_LOG: WantManager: getLocalWants. Count: ${wants.length}');
    return wants;
  }

  // --- Peer Wantlist Management ---

  // Add/update a want from a remote peer
  void addPeerWant(String peerId, CID cid, {
    int priority = 1,
    pb.Message_Wantlist_WantType wantType = pb.Message_Wantlist_WantType.Block,
    bool sendDontHave = false,
  }) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: addPeerWant from Peer $peerId for CID $cidStr. Priority: $priority, Type: $wantType, SendDontHave: $sendDontHave');
    _peerWantlists.putIfAbsent(peerId, () => {});
    _peerWantlists[peerId]![cidStr] = WantlistEntry(
      cid: cid,
      priority: priority,
      wantType: wantType,
      sendDontHave: sendDontHave,
    );
    print('Peer $peerId added/updated want for $cidStr');
  }

  // Remove a peer's want (e.g., peer sent a cancel, or we sent the block)
  WantlistEntry? removePeerWant(String peerId, CID cid) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: removePeerWant from Peer $peerId for CID $cidStr.');
    final peerWants = _peerWantlists[peerId];
    if (peerWants != null) {
      final removed = peerWants.remove(cidStr);
      if (removed != null) {
        print('// BSW_LOG: WantManager: Peer $peerId removed want for $cidStr.');
        print('Peer $peerId removed want for ${cid.toString()}');
      }
      if (peerWants.isEmpty) {
        _peerWantlists.remove(peerId);
        print('// BSW_LOG: WantManager: Cleared all wants for peer $peerId as their wantlist is now empty.');
      }
      return removed;
    }
    return null;
  }

  Map<String, WantlistEntry> getWantsForPeer(String peerId) {
    final wants = _peerWantlists[peerId] ?? {};
    print('// BSW_LOG: WantManager: getWantsForPeer $peerId. Count: ${wants.length}');
    return Map.unmodifiable(wants);
  }

  // Clear all wants for a specific peer
  void clearPeerWantlist(String peerId) {
    print('// BSW_LOG: WantManager: clearPeerWantlist for $peerId.');
    if (_peerWantlists.containsKey(peerId)) {
      _peerWantlists.remove(peerId);
      print('// BSW_LOG: WantManager: Cleared all wants for peer $peerId.');
      print('Cleared all wants for peer $peerId.');
    } else {
      print('// BSW_LOG: WantManager: clearPeerWantlist: No wants found for peer $peerId to clear.');
    }
  }

  // --- Notifications ---

  // Called when a block is received that we were wanting
  void blockReceived(CID cid, List<int> data) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: blockReceived for CID $cidStr. Data length: ${data.length}');
    final entry = _localWantlist[cidStr];
    if (entry != null) {
      print('// BSW_LOG: WantManager: Block $cidStr received was in local wantlist.');
      print('Block received that was in local wantlist: $cid');
      if (!entry.completer.isCompleted) {
        entry.completer.complete(Uint8List.fromList(data));
        print('// BSW_LOG: WantManager: Completed future for $cidStr.');
      } else {
        print('// BSW_LOG: WantManager: Future for $cidStr was already completed.');
      }
      // It's important to remove the want *after* completing the future,
      // to avoid a race condition where whenBlockReceived might be called after removal but before completion.
      removeLocalWant(cid); 
      // TODO: Send CANCEL messages to peers we asked for this block (Bitswap class should handle this after block is stored)
    } else {
      print('// BSW_LOG: WantManager: blockReceived for $cidStr, but it was not in the local wantlist (perhaps already removed or unsolicited).');
    }
  }

  // Called when we receive a HAVE from a peer for a block we want
  void receivedHave(String peerId, CID cid) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: receivedHave from Peer $peerId for CID $cidStr.');
    if (isWantingLocally(cid)) {
      print('// BSW_LOG: WantManager: Peer $peerId HAS block we want: $cidStr');
      print('Peer $peerId HAS block we want: $cid');
      // TODO: Potentially prioritize fetching from this peer
    } else {
      print('// BSW_LOG: WantManager: Received HAVE for $cidStr from $peerId, but not currently wanting it locally.');
    }
  }

  // Called when we receive a DONT_HAVE from a peer for a block we want
  void receivedDontHave(String peerId, CID cid) {
    final cidStr = cid.toString();
    print('// BSW_LOG: WantManager: receivedDontHave from Peer $peerId for CID $cidStr.');
    if (isWantingLocally(cid)) {
      print('// BSW_LOG: WantManager: Peer $peerId DOES NOT HAVE block we want: $cidStr');
      print('Peer $peerId DOES NOT HAVE block we want: $cid');
      // TODO: Deprioritize or stop asking this peer for this block
    } else {
      print('// BSW_LOG: WantManager: Received DONT_HAVE for $cidStr from $peerId, but not currently wanting it locally.');
    }
  }
}
