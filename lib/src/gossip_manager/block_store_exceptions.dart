// lib/src/gossip_manager/block_store_exceptions.dart

/// Exception thrown when a requested block is not found in the store
class BlockNotFoundException implements Exception {
  /// The CID of the block that was not found
  final String cid;
  
  /// Optional additional message
  final String? message;

  const BlockNotFoundException(this.cid, [this.message]);

  @override
  String toString() {
    final msg = message != null ? ': $message' : '';
    return 'BlockNotFoundException: Block with CID $cid not found$msg';
  }
}

/// Exception thrown when an invalid operation is attempted on the BlockStore
class InvalidOperationException implements Exception {
  /// Description of the invalid operation
  final String message;
  
  /// Optional underlying cause
  final Object? cause;

  const InvalidOperationException(this.message, [this.cause]);

  @override
  String toString() {
    final causeStr = cause != null ? ' (caused by: $cause)' : '';
    return 'InvalidOperationException: $message$causeStr';
  }
}

/// Exception thrown when attempting to operate on a tombstoned block inappropriately
class TombstonedBlockException extends InvalidOperationException {
  /// The CID of the tombstoned block
  final String cid;

  const TombstonedBlockException(this.cid, String message) 
      : super('Block $cid is tombstoned: $message');
}

/// Exception thrown when attempting to undelete a block that is not tombstoned
class BlockNotTombstonedException extends InvalidOperationException {
  /// The CID of the block that is not tombstoned
  final String cid;

  const BlockNotTombstonedException(this.cid) 
      : super('Block $cid is not tombstoned and cannot be undeleted');
}
