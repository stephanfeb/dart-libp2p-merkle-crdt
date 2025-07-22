# Last-Mile Implementation: Architectural Split

## Overview

This document defines the architectural boundary between the **libp2p-merkle-crdt** library and the **CryptoPeer framework** for implementing the final storage layer that applications will use. The enhanced BlockStore (Phases 1-4) provides the foundation, but the question remains: where should the high-level document-oriented APIs live?

## Architectural Decision

After completing Phases 1-4 of the BlockStore refactor, we have a clear separation of concerns for the remaining implementation:

### 🔧 **libp2p-merkle-crdt Responsibilities**
**Focus**: Low-level P2P/CRDT primitives and storage infrastructure

### 🏗️ **CryptoPeer Framework Responsibilities**  
**Focus**: High-level application APIs and developer convenience

## Component Ownership

### ✅ **IN libp2p-merkle-crdt** (This Repository)

#### **Enhanced BlockStore** (✅ COMPLETED - Phases 1-4)
```dart
// Already implemented and tested
abstract class BlockStore<P extends CRDTPayload<V>, V> {
  // Soft-delete operations
  Future<void> softDelete(CID cid, {String? reason, String? deletedBy});
  Future<bool> isTombstoned(CID cid);
  
  // Query system with pluggable field extractors
  Future<List<BlockStoreEntry<P, V>>> findWhere(/*...*/);
  BlockQuery<P, V> query(FieldExtractor<P, V> fieldExtractor);
  
  // Collection and metadata management
  Future<void> putInCollection(String collection, MerkleNode<P> node);
  Future<void> putMetadata(CID cid, Map<String, dynamic> metadata);
  
  // Change notifications and reactive features
  Stream<BlockChangeEvent<P, V>> watchChanges([ChangeFilter<P, V>? filter]);
  ChangeSubscription<P, V> subscribe({String? id, ChangeFilter<P, V>? filter});
}
```

#### **DocumentCRDT Implementation** (📋 TODO - Phase 6.1)
```dart
// NEW: Document-oriented CRDT payload type
class DocumentCRDT implements CRDTPayload<Map<String, dynamic>> {
  final Map<String, DocumentEntry> _entries;
  final VectorClock _clock;
  
  // Core CRDT operations
  DocumentCRDT put(String key, dynamic value, {String? actorId});
  DocumentCRDT delete(String key, {String? actorId});
  DocumentCRDT merge(covariant DocumentCRDT other);
  
  // Conflict resolution strategies
  DocumentCRDT withConflictResolver(ConflictResolver resolver);
  
  // Document-specific operations
  DocumentCRDT patch(Map<String, dynamic> updates);
  DocumentCRDT deepMerge(Map<String, dynamic> updates);
  
  @override
  Map<String, dynamic> get value => _computeCurrentState();
  
  @override
  Uint8List toCanonicalBytes() => _serializeToBytes();
}

// Supporting classes
class DocumentEntry {
  final dynamic value;
  final VectorClock timestamp;
  final bool isDeleted;
  final String actorId;
}

enum ConflictResolutionStrategy {
  lastWriterWins,
  firstWriterWins,
  merge,
  custom
}
```

#### **Document Field Extractor** (📋 TODO - Phase 6.1)
```dart
// NEW: Field extractor for document payloads
class DocumentFieldExtractor extends FieldExtractor<DocumentCRDT, Map<String, dynamic>> {
  @override
  dynamic extractField(MerkleNode<DocumentCRDT> node, String fieldPath) {
    // Support dot notation: "user.profile.name"
    // Support array indexing: "items[0].title"
    // Support metadata access: "_metadata.createdAt"
  }
  
  @override
  List<String> getSupportedFields() {
    // Dynamic field discovery from document structure
  }
}
```

**Rationale**: These are fundamental CRDT and storage primitives that other frameworks could reuse.

### 🏗️ **IN CryptoPeer Framework** (External Repository)

#### **P2PStorage Interface** (📋 TODO - Phase 6.2)
```dart
// High-level document database interface
abstract class P2PStorage {
  // Collection management
  P2PCollection collection(String name);
  Future<List<String>> listCollections();
  Future<void> dropCollection(String name);
  
  // Document operations (MongoDB-style)
  Future<Map<String, dynamic>?> findDocument(String collection, String id);
  Future<List<Map<String, dynamic>>> findDocuments(String collection, {
    Map<String, dynamic>? filter,
    Map<String, dynamic>? sort,
    int? limit,
    int? skip,
  });
  
  Future<void> insertDocument(String collection, String id, Map<String, dynamic> document);
  Future<void> updateDocument(String collection, String id, Map<String, dynamic> updates);
  Future<void> replaceDocument(String collection, String id, Map<String, dynamic> document);
  Future<void> deleteDocument(String collection, String id);
  
  // Reactive queries
  Stream<DocumentChangeEvent> watchCollection(String collection);
  Stream<DocumentChangeEvent> watchDocument(String collection, String id);
  
  // Aggregation and advanced queries
  Future<List<Map<String, dynamic>>> aggregate(String collection, List<AggregationStage> pipeline);
  Future<int> countDocuments(String collection, {Map<String, dynamic>? filter});
}
```

#### **P2PStorageImpl Implementation** (📋 TODO - Phase 6.2)
```dart
// Implementation using enhanced BlockStore + DocumentCRDT
class P2PStorageImpl implements P2PStorage {
  final BlockStore<DocumentCRDT, Map<String, dynamic>> _blockStore;
  final DocumentFieldExtractor _fieldExtractor;
  final String _actorId;
  
  P2PStorageImpl({
    required BlockStore<DocumentCRDT, Map<String, dynamic>> blockStore,
    required String actorId,
  }) : _blockStore = blockStore,
       _fieldExtractor = DocumentFieldExtractor(),
       _actorId = actorId;
  
  @override
  Future<void> insertDocument(String collection, String id, Map<String, dynamic> document) async {
    // Create DocumentCRDT with the document data
    final crdt = DocumentCRDT.fromMap(document, actorId: _actorId);
    final node = MerkleNode<DocumentCRDT>.create(crdt, <CID>{});
    
    // Store in collection with metadata
    await _blockStore.putInCollection(collection, node, metadata: {
      'documentId': id,
      'createdAt': DateTime.now().toIso8601String(),
      'createdBy': _actorId,
    });
  }
  
  @override
  Future<Map<String, dynamic>?> findDocument(String collection, String id) async {
    final results = await _blockStore.findInCollectionWhere(
      collection,
      _fieldExtractor,
      (query) => query.where('_metadata.documentId', id),
    );
    
    return results.isNotEmpty ? results.first.node?.payload.value : null;
  }
  
  @override
  Stream<DocumentChangeEvent> watchCollection(String collection) {
    return _blockStore.watchCollection(collection)
        .map((event) => DocumentChangeEvent.fromBlockChangeEvent(event));
  }
  
  // ... other implementations
}
```

#### **P2PCollection Helper** (📋 TODO - Phase 6.2)
```dart
// Convenient collection-scoped operations
class P2PCollection {
  final P2PStorage _storage;
  final String _name;
  
  P2PCollection(this._storage, this._name);
  
  // Scoped operations
  Future<Map<String, dynamic>?> findOne(String id) => _storage.findDocument(_name, id);
  Future<void> insertOne(String id, Map<String, dynamic> document) => _storage.insertDocument(_name, id, document);
  Future<void> updateOne(String id, Map<String, dynamic> updates) => _storage.updateDocument(_name, id, updates);
  Future<void> deleteOne(String id) => _storage.deleteDocument(_name, id);
  
  // Reactive operations
  Stream<DocumentChangeEvent> watch() => _storage.watchCollection(_name);
  Stream<DocumentChangeEvent> watchDocument(String id) => _storage.watchDocument(_name, id);
  
  // Query builder
  P2PQuery where(String field, dynamic value) => P2PQuery(_storage, _name).where(field, value);
}
```

#### **Framework Integration** (📋 TODO - Phase 6.3)
```dart
// Integration with CryptoPeer's dependency injection
class P2PApplication {
  late final P2PStorage storage;
  
  Future<void> initialize() async {
    // Create enhanced BlockStore
    final blockStore = InMemoryBlockStore<DocumentCRDT, Map<String, dynamic>>();
    await blockStore.open();
    
    // Create P2PStorage implementation
    storage = P2PStorageImpl(
      blockStore: blockStore,
      actorId: await _generateActorId(),
    );
    
    // Register with DI container
    GetIt.instance.registerSingleton<P2PStorage>(storage);
  }
}
```

**Rationale**: These are application-layer conveniences that provide familiar APIs to developers while being opinionated about document structure, validation, and usage patterns.

## Benefits of This Split

### 🎯 **Separation of Concerns**
- **libp2p-merkle-crdt**: Focuses on P2P networking, CRDT semantics, and storage primitives
- **CryptoPeer**: Focuses on developer experience, application patterns, and convenience APIs

### 🔄 **Reusability**
- Other frameworks can use `DocumentCRDT` without pulling in CryptoPeer-specific APIs
- The enhanced BlockStore can support multiple high-level storage abstractions

### 🛠️ **Independent Evolution**
- Core CRDT/storage logic can evolve independently of application APIs
- CryptoPeer can iterate on developer experience without affecting core library
- Different applications can have different opinions about document structure

### 🧪 **Testing Strategy**
- CRDT merge semantics can be tested in isolation
- Application-level APIs can be tested with mock BlockStore implementations
- Integration tests can verify the full stack

### 📦 **Deployment Flexibility**
- Applications can choose their storage backend (in-memory, SQLite, etc.) independently
- Different applications can use different conflict resolution strategies
- Framework can provide multiple storage implementations

## Implementation Timeline

### **Phase 6.1: DocumentCRDT** (libp2p-merkle-crdt)
**Estimated Effort**: 3-4 days  
**Dependencies**: Phases 1-4 ✅ (completed)

**Tasks**:
- [ ] Implement DocumentCRDT with proper merge semantics
- [ ] Add conflict resolution strategies (LWW, custom, etc.)
- [ ] Create DocumentFieldExtractor for querying
- [ ] Add comprehensive CRDT tests
- [ ] Document CRDT behavior and guarantees

### **Phase 6.2: P2PStorageImpl** (CryptoPeer Framework)
**Estimated Effort**: 4-5 days  
**Dependencies**: Phase 6.1

**Tasks**:
- [ ] Implement P2PStorage interface
- [ ] Create P2PStorageImpl using enhanced BlockStore + DocumentCRDT
- [ ] Add P2PCollection helper class
- [ ] Implement change streams and reactive queries
- [ ] Add comprehensive integration tests

### **Phase 6.3: Framework Integration** (CryptoPeer Framework)
**Estimated Effort**: 2-3 days  
**Dependencies**: Phase 6.2

**Tasks**:
- [ ] Update P2PApplication to use P2PStorage
- [ ] Update dependency injection configuration
- [ ] Create migration guide for existing applications
- [ ] Update documentation and examples

## Migration Path

### **For Existing CryptoPeer Applications**
1. **Phase 6.1 completion**: No changes needed (internal to libp2p-merkle-crdt)
2. **Phase 6.2 completion**: Update to use new P2PStorage APIs
3. **Phase 6.3 completion**: Update application initialization code

### **Backward Compatibility**
- Existing BlockStore usage continues to work unchanged
- New DocumentCRDT is additive, doesn't break existing CRDT types
- Migration can be gradual, collection by collection

## Success Criteria

### **Phase 6.1 Success Criteria**
- [ ] DocumentCRDT handles complex document operations correctly
- [ ] Merge semantics maintain CRDT properties (associative, commutative, idempotent)
- [ ] Conflict resolution strategies work as expected
- [ ] Performance is acceptable for typical document sizes
- [ ] All existing tests continue to pass

### **Phase 6.2 Success Criteria**
- [ ] P2PStorage provides intuitive MongoDB-like developer experience
- [ ] Change streams work reliably for real-time updates
- [ ] Query performance is acceptable for typical use cases
- [ ] Integration with enhanced BlockStore is seamless
- [ ] Comprehensive test coverage for all operations

### **Overall Success Criteria**
- [ ] CryptoPeer applications can easily adopt document-oriented storage
- [ ] Developer experience is significantly improved over raw BlockStore usage
- [ ] Performance meets production requirements
- [ ] Documentation enables easy adoption
- [ ] Migration path is clear and tested

## Risk Assessment

### **Low Risk**
- **DocumentCRDT Implementation**: Well-understood CRDT patterns, building on solid foundation
- **API Design**: Following proven patterns from MongoDB and similar systems

### **Medium Risk**
- **Performance**: Document operations may be slower than raw BlockStore operations
  - **Mitigation**: Performance testing and optimization, caching strategies
- **Memory Usage**: Document storage may use more memory than optimized formats
  - **Mitigation**: Configurable serialization, lazy loading

### **Mitigation Strategies**
- **Incremental Implementation**: Build and test each component independently
- **Performance Benchmarking**: Establish baselines and monitor throughout development
- **Comprehensive Testing**: Unit tests for CRDT logic, integration tests for full stack
- **Documentation**: Clear examples and migration guides

## Next Steps

1. **Immediate**: Begin Phase 6.1 (DocumentCRDT) implementation in libp2p-merkle-crdt
2. **Parallel**: Design P2PStorage API in CryptoPeer framework repository
3. **Integration**: Coordinate between repositories for seamless integration
4. **Testing**: Establish testing strategy across both repositories

This architectural split provides a clean separation of concerns while enabling powerful document-oriented storage for CryptoPeer applications built on the solid foundation of the enhanced BlockStore.

---

**Document Version**: 1.0  
**Last Updated**: 2025-07-15  
**Next Review**: After Phase 6.1 completion
