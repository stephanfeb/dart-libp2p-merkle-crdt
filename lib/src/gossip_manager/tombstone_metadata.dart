// lib/src/gossip_manager/tombstone_metadata.dart
import 'dart:convert';

/// Metadata associated with a tombstoned (soft-deleted) block.
/// Contains information about when, why, and by whom the block was deleted.
class TombstoneMetadata {
  /// When the block was deleted
  final DateTime deletedAt;
  
  /// Optional identifier of who/what deleted the block
  final String? deletedBy;
  
  /// Optional reason for deletion
  final String? reason;

  const TombstoneMetadata({
    required this.deletedAt,
    this.deletedBy,
    this.reason,
  });

  /// Creates TombstoneMetadata from a JSON map
  factory TombstoneMetadata.fromJson(Map<String, dynamic> json) {
    return TombstoneMetadata(
      deletedAt: DateTime.parse(json['deletedAt'] as String),
      deletedBy: json['deletedBy'] as String?,
      reason: json['reason'] as String?,
    );
  }

  /// Converts TombstoneMetadata to a JSON map
  Map<String, dynamic> toJson() {
    return {
      'deletedAt': deletedAt.toIso8601String(),
      if (deletedBy != null) 'deletedBy': deletedBy,
      if (reason != null) 'reason': reason,
    };
  }

  /// Creates TombstoneMetadata from JSON string
  factory TombstoneMetadata.fromJsonString(String jsonString) {
    return TombstoneMetadata.fromJson(json.decode(jsonString) as Map<String, dynamic>);
  }

  /// Converts TombstoneMetadata to JSON string
  String toJsonString() {
    return json.encode(toJson());
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is TombstoneMetadata &&
          runtimeType == other.runtimeType &&
          deletedAt == other.deletedAt &&
          deletedBy == other.deletedBy &&
          reason == other.reason;

  @override
  int get hashCode => Object.hash(deletedAt, deletedBy, reason);

  @override
  String toString() {
    return 'TombstoneMetadata{deletedAt: $deletedAt, deletedBy: $deletedBy, reason: $reason}';
  }
}
