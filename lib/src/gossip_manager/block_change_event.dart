// lib/src/gossip_manager/block_change_event.dart
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;

/// Represents the type of change that occurred to a block
enum BlockChangeType {
  /// A new block was inserted into the store
  inserted,
  
  /// An existing block was updated (replaced with new content)
  updated,
  
  /// A block was soft-deleted (tombstoned)
  deleted,
  
  /// A tombstoned block was restored (undeleted)
  restored,
  
  /// Block metadata was updated
  metadataUpdated,
  
  /// Block was moved to a different collection
  collectionChanged,
}

/// Represents a change event that occurred in the BlockStore
class BlockChangeEvent<P extends CRDTPayload<V>, V> {
  /// The type of change that occurred
  final BlockChangeType type;
  
  /// The CID of the block that changed
  final CID cid;
  
  /// The current node data (null for deleted blocks)
  final MerkleNode<P>? node;
  
  /// The previous node data (for updates, null for inserts)
  final MerkleNode<P>? previousNode;
  
  /// When the change occurred
  final DateTime timestamp;
  
  /// The collection the block belongs to (null if not in a collection)
  final String? collection;
  
  /// The previous collection (for collection changes)
  final String? previousCollection;
  
  /// Current metadata associated with the block
  final Map<String, dynamic> metadata;
  
  /// Previous metadata (for metadata updates)
  final Map<String, dynamic>? previousMetadata;
  
  /// Additional context about the change (e.g., reason for deletion)
  final Map<String, dynamic> context;

  const BlockChangeEvent({
    required this.type,
    required this.cid,
    this.node,
    this.previousNode,
    required this.timestamp,
    this.collection,
    this.previousCollection,
    this.metadata = const {},
    this.previousMetadata,
    this.context = const {},
  });

  /// Creates an insert event
  factory BlockChangeEvent.inserted({
    required CID cid,
    required MerkleNode<P> node,
    String? collection,
    Map<String, dynamic> metadata = const {},
    Map<String, dynamic> context = const {},
  }) {
    return BlockChangeEvent<P, V>(
      type: BlockChangeType.inserted,
      cid: cid,
      node: node,
      timestamp: DateTime.now(),
      collection: collection,
      metadata: metadata,
      context: context,
    );
  }

  /// Creates an update event
  factory BlockChangeEvent.updated({
    required CID cid,
    required MerkleNode<P> node,
    MerkleNode<P>? previousNode,
    String? collection,
    Map<String, dynamic> metadata = const {},
    Map<String, dynamic> context = const {},
  }) {
    return BlockChangeEvent<P, V>(
      type: BlockChangeType.updated,
      cid: cid,
      node: node,
      previousNode: previousNode,
      timestamp: DateTime.now(),
      collection: collection,
      metadata: metadata,
      context: context,
    );
  }

  /// Creates a delete event
  factory BlockChangeEvent.deleted({
    required CID cid,
    MerkleNode<P>? previousNode,
    String? collection,
    Map<String, dynamic> metadata = const {},
    String? reason,
    String? deletedBy,
    Map<String, dynamic> context = const {},
  }) {
    final deleteContext = Map<String, dynamic>.from(context);
    if (reason != null) deleteContext['reason'] = reason;
    if (deletedBy != null) deleteContext['deletedBy'] = deletedBy;
    
    return BlockChangeEvent<P, V>(
      type: BlockChangeType.deleted,
      cid: cid,
      previousNode: previousNode,
      timestamp: DateTime.now(),
      collection: collection,
      metadata: metadata,
      context: deleteContext,
    );
  }

  /// Creates a restore event
  factory BlockChangeEvent.restored({
    required CID cid,
    required MerkleNode<P> node,
    String? collection,
    Map<String, dynamic> metadata = const {},
    Map<String, dynamic> context = const {},
  }) {
    return BlockChangeEvent<P, V>(
      type: BlockChangeType.restored,
      cid: cid,
      node: node,
      timestamp: DateTime.now(),
      collection: collection,
      metadata: metadata,
      context: context,
    );
  }

  /// Creates a metadata update event
  factory BlockChangeEvent.metadataUpdated({
    required CID cid,
    MerkleNode<P>? node,
    String? collection,
    required Map<String, dynamic> metadata,
    Map<String, dynamic>? previousMetadata,
    Map<String, dynamic> context = const {},
  }) {
    return BlockChangeEvent<P, V>(
      type: BlockChangeType.metadataUpdated,
      cid: cid,
      node: node,
      timestamp: DateTime.now(),
      collection: collection,
      metadata: metadata,
      previousMetadata: previousMetadata,
      context: context,
    );
  }

  /// Creates a collection change event
  factory BlockChangeEvent.collectionChanged({
    required CID cid,
    MerkleNode<P>? node,
    String? collection,
    String? previousCollection,
    Map<String, dynamic> metadata = const {},
    Map<String, dynamic> context = const {},
  }) {
    return BlockChangeEvent<P, V>(
      type: BlockChangeType.collectionChanged,
      cid: cid,
      node: node,
      timestamp: DateTime.now(),
      collection: collection,
      previousCollection: previousCollection,
      metadata: metadata,
      context: context,
    );
  }

  @override
  String toString() {
    return 'BlockChangeEvent{type: $type, cid: $cid, collection: $collection, timestamp: $timestamp}';
  }

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    return other is BlockChangeEvent<P, V> &&
        other.type == type &&
        other.cid == cid &&
        other.timestamp == timestamp;
  }

  @override
  int get hashCode {
    return type.hashCode ^ cid.hashCode ^ timestamp.hashCode;
  }
}
