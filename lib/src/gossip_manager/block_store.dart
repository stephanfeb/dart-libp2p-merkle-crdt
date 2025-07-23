// lib/src/gossip_manager/block_store.dart
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;
import 'package:logging/logging.dart';

import 'block_store_entry.dart';
import 'tombstone_metadata.dart';
import 'block_store_exceptions.dart';
import 'field_extractor.dart';
import 'block_query.dart';
import 'pagination.dart';
import 'block_change_event.dart';
import 'change_subscription.dart';

/// Enhanced BlockStore interface with soft-delete functionality, batch operations,
/// and iteration support for efficient P2P storage implementation.
abstract class BlockStore<P extends CRDTPayload<V>, V> {
  /// Gets a block by CID. Returns null if not found.
  /// By default, tombstoned blocks are excluded unless includeTombstoned is true.
  Future<MerkleNode<P>?> get(CID cid, {bool includeTombstoned = false});

  /// Stores a block in the store
  Future<void> put(MerkleNode<P> node);

  /// Soft-deletes a block by marking it as tombstoned while preserving the entry
  /// for merkle-DAG integrity. The block becomes unavailable for normal get() calls
  /// but can still be retrieved with includeTombstoned=true.
  Future<void> softDelete(CID cid, {String? reason, String? deletedBy});

  /// Restores a tombstoned block, making it available for normal operations again.
  /// Throws BlockNotTombstonedException if the block is not currently tombstoned.
  /// Throws BlockNotFoundException if the block doesn't exist at all.
  Future<void> undelete(CID cid);

  /// Checks if a block is currently tombstoned (soft-deleted)
  Future<bool> isTombstoned(CID cid);

  /// Batch get operation for multiple CIDs
  /// Returns a map where missing blocks have null values
  Future<Map<CID, MerkleNode<P>?>> getAll(List<CID> cids, {bool includeTombstoned = false});

  /// Batch put operation for multiple blocks
  Future<void> putAll(Map<CID, MerkleNode<P>> entries);

  /// Gets all CIDs in the store
  /// By default, excludes tombstoned blocks unless includeTombstoned is true
  Future<List<CID>> getAllCids({bool includeTombstoned = false});

  /// Streams all entries in the store with full metadata
  /// By default, excludes tombstoned blocks unless includeTombstoned is true
  Stream<BlockStoreEntry<P, V>> streamAll({bool includeTombstoned = false});

  /// Streams all CIDs in the store
  /// By default, excludes tombstoned blocks unless includeTombstoned is true
  Stream<CID> streamCids({bool includeTombstoned = false});

  /// Gets a full BlockStoreEntry with metadata for a CID
  /// Returns null if not found
  Future<BlockStoreEntry<P, V>?> getEntry(CID cid, {bool includeTombstoned = false});

  /// Optional: for stores that need initialization
  Future<void> open();

  /// Optional: for stores that need cleanup
  Future<void> close();

  // Query System Methods

  /// Creates a query builder for this BlockStore using the provided field extractor.
  /// The field extractor determines how fields are extracted from payloads for querying.
  BlockQuery<P, V> query(FieldExtractor<P, V> fieldExtractor);

  /// Finds entries matching the given query conditions.
  /// Returns a list of matching BlockStoreEntry objects.
  Future<List<BlockStoreEntry<P, V>>> findWhere(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  );

  /// Finds the first entry matching the given query conditions.
  /// Returns null if no matching entry is found.
  Future<BlockStoreEntry<P, V>?> findOneWhere(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  );

  /// Counts the number of entries matching the given query conditions.
  Future<int> count(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  );

  /// Checks if any entries match the given query conditions.
  Future<bool> exists(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  );

  /// Finds entries with pagination support.
  /// Returns a paginated result with entries and metadata.
  Future<PageResult<BlockStoreEntry<P, V>>> findPage(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
    PageRequest pageRequest,
  );

  // Collection Management Methods

  /// Creates a new collection. If the collection already exists, this is a no-op.
  Future<void> createCollection(String name);

  /// Drops a collection and all its entries. This performs soft-delete on all entries.
  Future<void> dropCollection(String name);

  /// Lists all collection names in the store.
  Future<List<String>> listCollections();

  /// Finds all entries in a specific collection.
  Future<List<BlockStoreEntry<P, V>>> findInCollection(String collection, {bool includeTombstoned = false});

  /// Stores a block in a specific collection.
  Future<void> putInCollection(String collection, MerkleNode<P> node, {Map<String, dynamic>? metadata});

  /// Queries entries within a specific collection.
  Future<List<BlockStoreEntry<P, V>>> findInCollectionWhere(
    String collection,
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  );

  /// Counts entries in a specific collection.
  Future<int> countInCollection(String collection, {bool includeTombstoned = false});

  // Metadata Management Methods

  /// Sets metadata for a specific block. Merges with existing metadata.
  Future<void> putMetadata(CID cid, Map<String, dynamic> metadata);

  /// Gets metadata for a specific block.
  Future<Map<String, dynamic>?> getMetadata(CID cid);

  /// Deletes specific metadata keys for a block.
  Future<void> deleteMetadata(CID cid, List<String> keys);

  /// Clears all metadata for a block.
  Future<void> clearMetadata(CID cid);

  /// Updates a block's collection assignment.
  Future<void> moveToCollection(CID cid, String? newCollection);

  // Change Notification Methods

  /// Watches for changes to blocks in the store.
  /// Returns a stream of change events that can be filtered.
  /// If no filter is provided, all changes are emitted.
  Stream<BlockChangeEvent<P, V>> watchChanges([ChangeFilter<P, V>? filter]);

  /// Watches for changes to blocks in a specific collection.
  /// Returns a stream of change events for the specified collection.
  Stream<BlockChangeEvent<P, V>> watchCollection(String collection);

  /// Creates a subscription to change events with optional filtering.
  /// Returns a ChangeSubscription that can be managed independently.
  ChangeSubscription<P, V> subscribe({String? id, ChangeFilter<P, V>? filter});

  /// Removes a subscription by ID.
  /// Returns true if the subscription was found and removed, false otherwise.
  Future<bool> unsubscribe(String subscriptionId);

  /// Gets the number of active change subscriptions.
  int get subscriptionCount;
}

/// In-memory implementation of BlockStore with full soft-delete functionality
class InMemoryBlockStore<P extends CRDTPayload<V>, V> implements BlockStore<P, V> {
  final Map<String, BlockStoreEntry<P, V>> _store = {};
  final Logger _logger = Logger('InMemoryBlockStore');
  final ChangeSubscriptionManager<P, V> _subscriptionManager = ChangeSubscriptionManager<P, V>();

  @override
  Future<MerkleNode<P>?> get(CID cid, {bool includeTombstoned = false}) async {
    final entry = _store[cid.toString()];
    if (entry == null) return null;
    
    if (entry.isTombstoned && !includeTombstoned) {
      return null;
    }
    
    return entry.node;
  }

  @override
  Future<void> put(MerkleNode<P> node) async {
    final cidString = node.cid.toString();
    final existingEntry = _store[cidString];
    
    if (existingEntry != null) {
      // Update existing entry, preserving creation time, collection, and metadata
      final previousNode = existingEntry.node;
      _store[cidString] = existingEntry.copyWith(
        node: node,
        isTombstoned: false,
        tombstoneMetadata: null,
        lastModified: DateTime.now(),
      );
      
      // Emit update event
      final event = BlockChangeEvent<P, V>.updated(
        cid: node.cid,
        node: node,
        previousNode: previousNode,
        collection: existingEntry.collection,
        metadata: existingEntry.metadata,
      );
      _emitChangeEvent(event);
    } else {
      // Create new entry
      final newEntry = BlockStoreEntry<P, V>.active(node: node, collection: 'shopping-lists'); //FIXME !
      _store[cidString] = newEntry;
      
      // Emit insert event
      final event = BlockChangeEvent<P, V>.inserted(
        cid: node.cid,
        node: node,
        collection: newEntry.collection,
        metadata: newEntry.metadata,
      );
      _emitChangeEvent(event);
    }
    
    _logger.fine('Put block ${node.cid}');
  }

  @override
  Future<void> softDelete(CID cid, {String? reason, String? deletedBy}) async {
    final cidString = cid.toString();
    final entry = _store[cidString];
    
    if (entry == null) {
      throw BlockNotFoundException(cidString);
    }
    
    if (entry.isTombstoned) {
      throw InvalidOperationException('Block $cidString is already tombstoned');
    }
    
    final tombstoneMetadata = TombstoneMetadata(
      deletedAt: DateTime.now(),
      deletedBy: deletedBy,
      reason: reason,
    );
    
    _store[cidString] = BlockStoreEntry<P, V>.tombstoned(
      originalEntry: entry,
      tombstoneMetadata: tombstoneMetadata,
    );
    
    // Emit delete event
    final event = BlockChangeEvent<P, V>.deleted(
      cid: cid,
      previousNode: entry.node,
      collection: entry.collection,
      metadata: entry.metadata,
      reason: reason,
      deletedBy: deletedBy,
    );
    _emitChangeEvent(event);
    
    _logger.info('Soft deleted block $cidString (reason: $reason, deletedBy: $deletedBy)');
  }

  @override
  Future<void> undelete(CID cid) async {
    final cidString = cid.toString();
    final entry = _store[cidString];
    
    if (entry == null) {
      throw BlockNotFoundException(cidString);
    }
    
    if (!entry.isTombstoned) {
      throw BlockNotTombstonedException(cidString);
    }
    
    // For undelete to work, we need to restore the original node data
    // In a real implementation, this might require fetching from a backup or cache
    // For now, we'll throw an exception as the node data is null in tombstoned entries
    throw InvalidOperationException(
      'Cannot undelete block $cidString: original node data not available in tombstoned entry. '
      'This requires external restoration of the MerkleNode data.'
    );
  }

  @override
  Future<bool> isTombstoned(CID cid) async {
    final entry = _store[cid.toString()];
    return entry?.isTombstoned ?? false;
  }

  @override
  Future<Map<CID, MerkleNode<P>?>> getAll(List<CID> cids, {bool includeTombstoned = false}) async {
    final result = <CID, MerkleNode<P>?>{};
    
    for (final cid in cids) {
      result[cid] = await get(cid, includeTombstoned: includeTombstoned);
    }
    
    return result;
  }

  @override
  Future<void> putAll(Map<CID, MerkleNode<P>> entries) async {
    for (final entry in entries.entries) {
      await put(entry.value);
    }
  }

  @override
  Future<List<CID>> getAllCids({bool includeTombstoned = false}) async {
    final cids = <CID>[];
    
    for (final entry in _store.values) {
      if (!entry.isTombstoned || includeTombstoned) {
        cids.add(entry.cid);
      }
    }
    
    return cids;
  }

  @override
  Stream<BlockStoreEntry<P, V>> streamAll({bool includeTombstoned = false}) async* {
    for (final entry in _store.values) {
      if (!entry.isTombstoned || includeTombstoned) {
        yield entry;
      }
    }
  }

  @override
  Stream<CID> streamCids({bool includeTombstoned = false}) async* {
    for (final entry in _store.values) {
      if (!entry.isTombstoned || includeTombstoned) {
        yield entry.cid;
      }
    }
  }

  @override
  Future<BlockStoreEntry<P, V>?> getEntry(CID cid, {bool includeTombstoned = false}) async {
    final entry = _store[cid.toString()];
    if (entry == null) return null;
    
    if (entry.isTombstoned && !includeTombstoned) {
      return null;
    }
    
    return entry;
  }

  @override
  Future<void> open() async {
    _logger.info('InMemoryBlockStore opened');
  }

  @override
  Future<void> close() async {
    _store.clear();
    _logger.info('InMemoryBlockStore closed');
  }

  // Query System Implementation

  @override
  BlockQuery<P, V> query(FieldExtractor<P, V> fieldExtractor) {
    return BlockQueryImpl<P, V>(
      entryStream: streamAll(),
      fieldExtractor: fieldExtractor,
    );
  }

  @override
  Future<List<BlockStoreEntry<P, V>>> findWhere(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  ) async {
    final query = queryBuilder(this.query(fieldExtractor));
    return await query.execute();
  }

  @override
  Future<BlockStoreEntry<P, V>?> findOneWhere(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  ) async {
    final query = queryBuilder(this.query(fieldExtractor));
    return await query.executeFirst();
  }

  @override
  Future<int> count(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  ) async {
    final query = queryBuilder(this.query(fieldExtractor));
    return await query.executeCount();
  }

  @override
  Future<bool> exists(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  ) async {
    final query = queryBuilder(this.query(fieldExtractor));
    return await query.executeExists();
  }

  @override
  Future<PageResult<BlockStoreEntry<P, V>>> findPage(
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
    PageRequest pageRequest,
  ) async {
    // For pagination, we need to get all results first, then paginate
    // In a real database implementation, this would be more efficient
    final query = queryBuilder(this.query(fieldExtractor));
    final allResults = await query.execute();
    
    return PageResult.fromList(allResults, pageRequest);
  }

  // Collection Management Implementation

  @override
  Future<void> createCollection(String name) async {
    // In-memory implementation doesn't need explicit collection creation
    // Collections are created implicitly when blocks are added to them
    _logger.fine('Collection "$name" created (implicit)');
  }

  @override
  Future<void> dropCollection(String name) async {
    final entriesToDelete = _store.values
        .where((entry) => entry.collection == name && !entry.isTombstoned)
        .toList();
    
    for (final entry in entriesToDelete) {
      await softDelete(entry.cid, reason: 'Collection "$name" dropped');
    }
    
    _logger.info('Dropped collection "$name" (${entriesToDelete.length} entries soft-deleted)');
  }

  @override
  Future<List<String>> listCollections() async {
    final collections = <String>{};
    
    for (final entry in _store.values) {
      if (entry.collection != null && !entry.isTombstoned) {
        collections.add(entry.collection!);
      }
    }
    
    return collections.toList()..sort();
  }

  @override
  Future<List<BlockStoreEntry<P, V>>> findInCollection(String collection, {bool includeTombstoned = false}) async {
    final results = <BlockStoreEntry<P, V>>[];
    
    for (final entry in _store.values) {
      if (entry.collection == collection) {
        if (!entry.isTombstoned || includeTombstoned) {
          results.add(entry);
        }
      }
    }
    
    return results;
  }

  @override
  Future<void> putInCollection(String collection, MerkleNode<P> node, {Map<String, dynamic>? metadata}) async {
    final cidString = node.cid.toString();
    final existingEntry = _store[cidString];
    
    if (existingEntry != null) {
      // Update existing entry with new collection and metadata
      final updatedMetadata = Map<String, dynamic>.from(existingEntry.metadata);
      if (metadata != null) {
        updatedMetadata.addAll(metadata);
      }
      
      final previousNode = existingEntry.node;
      _store[cidString] = existingEntry.copyWith(
        node: node,
        collection: collection,
        metadata: updatedMetadata,
        lastModified: DateTime.now(),
        isTombstoned: false,
        tombstoneMetadata: null,
      );
      
      // Emit update event
      final event = BlockChangeEvent<P, V>.updated(
        cid: node.cid,
        node: node,
        previousNode: previousNode,
        collection: collection,
        metadata: updatedMetadata,
      );
      _emitChangeEvent(event);
    } else {
      // Create new entry in collection
      final newEntry = BlockStoreEntry<P, V>.active(
        node: node,
        collection: collection,
        metadata: metadata,
      );
      _store[cidString] = newEntry;
      
      // Emit insert event
      final event = BlockChangeEvent<P, V>.inserted(
        cid: node.cid,
        node: node,
        collection: collection,
        metadata: newEntry.metadata,
      );
      _emitChangeEvent(event);
    }
    
    _logger.fine('Put block ${node.cid} in collection "$collection"');
  }

  @override
  Future<List<BlockStoreEntry<P, V>>> findInCollectionWhere(
    String collection,
    FieldExtractor<P, V> fieldExtractor,
    BlockQuery<P, V> Function(BlockQuery<P, V> query) queryBuilder,
  ) async {
    // Create a filtered stream for the collection
    final collectionStream = streamAll().where((entry) => entry.collection == collection);
    
    final query = queryBuilder(BlockQueryImpl<P, V>(
      entryStream: collectionStream,
      fieldExtractor: fieldExtractor,
    ));
    
    return await query.execute();
  }

  @override
  Future<int> countInCollection(String collection, {bool includeTombstoned = false}) async {
    int count = 0;
    
    for (final entry in _store.values) {
      if (entry.collection == collection) {
        if (!entry.isTombstoned || includeTombstoned) {
          count++;
        }
      }
    }
    
    return count;
  }

  // Metadata Management Implementation

  @override
  Future<void> putMetadata(CID cid, Map<String, dynamic> metadata) async {
    final cidString = cid.toString();
    final entry = _store[cidString];
    
    if (entry == null) {
      throw BlockNotFoundException(cidString);
    }
    
    final previousMetadata = Map<String, dynamic>.from(entry.metadata);
    final updatedMetadata = Map<String, dynamic>.from(entry.metadata);
    updatedMetadata.addAll(metadata);
    
    _store[cidString] = entry.copyWith(
      metadata: updatedMetadata,
      lastModified: DateTime.now(),
    );
    
    // Emit metadata update event
    final event = BlockChangeEvent<P, V>.metadataUpdated(
      cid: cid,
      node: entry.node,
      collection: entry.collection,
      metadata: updatedMetadata,
      previousMetadata: previousMetadata,
    );
    _emitChangeEvent(event);
    
    _logger.fine('Updated metadata for block $cidString');
  }

  @override
  Future<Map<String, dynamic>?> getMetadata(CID cid) async {
    final entry = _store[cid.toString()];
    return entry?.metadata;
  }

  @override
  Future<void> deleteMetadata(CID cid, List<String> keys) async {
    final cidString = cid.toString();
    final entry = _store[cidString];
    
    if (entry == null) {
      throw BlockNotFoundException(cidString);
    }
    
    final updatedMetadata = Map<String, dynamic>.from(entry.metadata);
    for (final key in keys) {
      updatedMetadata.remove(key);
    }
    
    _store[cidString] = entry.copyWith(
      metadata: updatedMetadata,
      lastModified: DateTime.now(),
    );
    
    _logger.fine('Deleted metadata keys $keys for block $cidString');
  }

  @override
  Future<void> clearMetadata(CID cid) async {
    final cidString = cid.toString();
    final entry = _store[cidString];
    
    if (entry == null) {
      throw BlockNotFoundException(cidString);
    }
    
    _store[cidString] = entry.copyWith(
      metadata: const {},
      lastModified: DateTime.now(),
    );
    
    _logger.fine('Cleared all metadata for block $cidString');
  }

  @override
  Future<void> moveToCollection(CID cid, String? newCollection) async {
    final cidString = cid.toString();
    final entry = _store[cidString];
    
    if (entry == null) {
      throw BlockNotFoundException(cidString);
    }
    
    final previousCollection = entry.collection;
    _store[cidString] = entry.copyWith(
      collection: newCollection,
      lastModified: DateTime.now(),
    );
    
    // Emit collection change event
    final event = BlockChangeEvent<P, V>.collectionChanged(
      cid: cid,
      node: entry.node,
      collection: newCollection,
      previousCollection: previousCollection,
      metadata: entry.metadata,
    );
    _emitChangeEvent(event);
    
    _logger.fine('Moved block $cidString to collection "$newCollection"');
  }

  /// Additional utility methods for debugging and testing
  
  /// Gets the total number of entries (including tombstoned)
  int get totalEntryCount => _store.length;
  
  /// Gets the number of active (non-tombstoned) entries
  int get activeEntryCount => _store.values.where((e) => !e.isTombstoned).length;
  
  /// Gets the number of tombstoned entries
  int get tombstonedEntryCount => _store.values.where((e) => e.isTombstoned).length;

  /// Gets the number of entries in a specific collection
  int getCollectionEntryCount(String collection, {bool includeTombstoned = false}) {
    return _store.values
        .where((entry) => entry.collection == collection)
        .where((entry) => !entry.isTombstoned || includeTombstoned)
        .length;
  }

  // Change Notification Implementation

  @override
  Stream<BlockChangeEvent<P, V>> watchChanges([ChangeFilter<P, V>? filter]) {
    final subscription = _subscriptionManager.subscribe(filter: filter);
    return subscription.stream;
  }

  @override
  Stream<BlockChangeEvent<P, V>> watchCollection(String collection) {
    final filter = CollectionFilter<P, V>(collection);
    return watchChanges(filter);
  }

  @override
  ChangeSubscription<P, V> subscribe({String? id, ChangeFilter<P, V>? filter}) {
    return _subscriptionManager.subscribe(id: id, filter: filter);
  }

  @override
  Future<bool> unsubscribe(String subscriptionId) async {
    return await _subscriptionManager.unsubscribe(subscriptionId);
  }

  @override
  int get subscriptionCount => _subscriptionManager.subscriptionCount;

  /// Emits a change event to all subscribers
  void _emitChangeEvent(BlockChangeEvent<P, V> event) {
    _subscriptionManager.emit(event);
    _logger.fine('Emitted change event: $event');
  }
}
