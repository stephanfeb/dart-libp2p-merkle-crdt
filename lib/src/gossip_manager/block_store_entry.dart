// lib/src/gossip_manager/block_store_entry.dart
import 'package:dcid/dcid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;
import 'tombstone_metadata.dart';

/// Represents an entry in the BlockStore, combining a MerkleNode with metadata
/// and tombstone status. This wrapper provides a unified view of both active
/// and soft-deleted blocks.
class BlockStoreEntry<P extends CRDTPayload<V>, V> {
  /// The CID of this block
  final CID cid;
  
  /// The actual MerkleNode data, null if tombstoned
  final MerkleNode<P>? node;
  
  /// Whether this block is tombstoned (soft-deleted)
  final bool isTombstoned;
  
  /// Tombstone metadata, present only if isTombstoned is true
  final TombstoneMetadata? tombstoneMetadata;
  
  /// When this entry was first created in the store
  final DateTime createdAt;
  
  /// When this entry was last modified (including tombstone operations)
  final DateTime lastModified;
  
  /// The collection this block belongs to (optional)
  final String? collection;
  
  /// Custom metadata associated with this block
  final Map<String, dynamic> metadata;

  const BlockStoreEntry({
    required this.cid,
    required this.node,
    required this.isTombstoned,
    this.tombstoneMetadata,
    required this.createdAt,
    required this.lastModified,
    this.collection,
    this.metadata = const {},
  });

  /// Creates a new active (non-tombstoned) entry
  factory BlockStoreEntry.active({
    required MerkleNode<P> node,
    DateTime? createdAt,
    String? collection,
    Map<String, dynamic>? metadata,
  }) {
    final now = createdAt ?? DateTime.now();
    return BlockStoreEntry<P, V>(
      cid: node.cid,
      node: node,
      isTombstoned: false,
      tombstoneMetadata: null,
      createdAt: now,
      lastModified: now,
      collection: collection,
      metadata: metadata ?? const {},
    );
  }

  /// Creates a tombstoned entry from an existing active entry
  factory BlockStoreEntry.tombstoned({
    required BlockStoreEntry<P, V> originalEntry,
    required TombstoneMetadata tombstoneMetadata,
  }) {
    return BlockStoreEntry<P, V>(
      cid: originalEntry.cid,
      node: null, // Node is null for tombstoned entries
      isTombstoned: true,
      tombstoneMetadata: tombstoneMetadata,
      createdAt: originalEntry.createdAt,
      lastModified: DateTime.now(),
      collection: originalEntry.collection,
      metadata: originalEntry.metadata,
    );
  }

  /// Creates an entry that has been undeleted (restored from tombstone)
  factory BlockStoreEntry.undeleted({
    required BlockStoreEntry<P, V> tombstonedEntry,
    required MerkleNode<P> restoredNode,
  }) {
    if (!tombstonedEntry.isTombstoned) {
      throw ArgumentError('Cannot undelete an entry that is not tombstoned');
    }
    
    return BlockStoreEntry<P, V>(
      cid: tombstonedEntry.cid,
      node: restoredNode,
      isTombstoned: false,
      tombstoneMetadata: null,
      createdAt: tombstonedEntry.createdAt,
      lastModified: DateTime.now(),
      collection: tombstonedEntry.collection,
      metadata: tombstonedEntry.metadata,
    );
  }

  /// Returns true if this entry has valid node data (not tombstoned)
  bool get hasData => node != null && !isTombstoned;

  /// Returns the CID as a string
  String get cidString => cid.toString();

  /// Creates a copy of this entry with updated fields
  BlockStoreEntry<P, V> copyWith({
    CID? cid,
    MerkleNode<P>? node,
    bool? isTombstoned,
    TombstoneMetadata? tombstoneMetadata,
    DateTime? createdAt,
    DateTime? lastModified,
    String? collection,
    Map<String, dynamic>? metadata,
  }) {
    return BlockStoreEntry<P, V>(
      cid: cid ?? this.cid,
      node: node ?? this.node,
      isTombstoned: isTombstoned ?? this.isTombstoned,
      tombstoneMetadata: tombstoneMetadata ?? this.tombstoneMetadata,
      createdAt: createdAt ?? this.createdAt,
      lastModified: lastModified ?? this.lastModified,
      collection: collection ?? this.collection,
      metadata: metadata ?? this.metadata,
    );
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is BlockStoreEntry<P, V> &&
          runtimeType == other.runtimeType &&
          cid == other.cid &&
          node == other.node &&
          isTombstoned == other.isTombstoned &&
          tombstoneMetadata == other.tombstoneMetadata &&
          createdAt == other.createdAt &&
          lastModified == other.lastModified &&
          collection == other.collection &&
          _mapEquals(metadata, other.metadata);

  @override
  int get hashCode => Object.hash(
        cid,
        node,
        isTombstoned,
        tombstoneMetadata,
        createdAt,
        lastModified,
        collection,
        metadata,
      );

  @override
  String toString() {
    return 'BlockStoreEntry{cid: $cid, isTombstoned: $isTombstoned, '
        'hasData: $hasData, collection: $collection, '
        'createdAt: $createdAt, lastModified: $lastModified}';
  }

  /// Helper method for deep equality comparison of maps
  bool _mapEquals(Map<String, dynamic> a, Map<String, dynamic> b) {
    if (a.length != b.length) return false;
    for (final key in a.keys) {
      if (!b.containsKey(key) || a[key] != b[key]) return false;
    }
    return true;
  }
}
