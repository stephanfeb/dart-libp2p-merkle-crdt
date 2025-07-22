// lib/src/gossip_manager/field_extractor.dart
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;

/// Interface for extracting queryable fields from different payload formats.
/// This maintains payload agnosticism by allowing pluggable field extraction
/// strategies for different CRDT payload types.
abstract class FieldExtractor<P extends CRDTPayload<V>, V> {
  /// Extracts a field value from a MerkleNode using a field path.
  /// Field paths use dot notation (e.g., "user.name", "metadata.timestamp").
  /// Returns null if the field doesn't exist or cannot be extracted.
  dynamic extractField(MerkleNode<P> node, String fieldPath);

  /// Returns a list of all field paths that this extractor can handle.
  /// This is used for query optimization and validation.
  List<String> getSupportedFields();

  /// Returns true if this extractor can handle the given field path.
  bool supportsField(String fieldPath) {
    return getSupportedFields().contains(fieldPath);
  }

  /// Extracts multiple fields at once for efficiency.
  /// Returns a map of fieldPath -> value for all requested fields.
  Map<String, dynamic> extractFields(MerkleNode<P> node, List<String> fieldPaths) {
    final result = <String, dynamic>{};
    for (final fieldPath in fieldPaths) {
      result[fieldPath] = extractField(node, fieldPath);
    }
    return result;
  }
}

/// Base implementation providing common functionality for field extractors.
abstract class BaseFieldExtractor<P extends CRDTPayload<V>, V> implements FieldExtractor<P, V> {
  /// Cache of supported fields for performance
  List<String>? _supportedFieldsCache;

  @override
  List<String> getSupportedFields() {
    return _supportedFieldsCache ??= computeSupportedFields();
  }

  /// Subclasses implement this to define their supported fields.
  List<String> computeSupportedFields();

  @override
  bool supportsField(String fieldPath) {
    return getSupportedFields().contains(fieldPath);
  }

  @override
  Map<String, dynamic> extractFields(MerkleNode<P> node, List<String> fieldPaths) {
    final result = <String, dynamic>{};
    for (final fieldPath in fieldPaths) {
      result[fieldPath] = extractField(node, fieldPath);
    }
    return result;
  }

  /// Helper method to safely navigate nested field paths.
  /// Returns null if any part of the path is missing.
  dynamic navigateFieldPath(dynamic data, String fieldPath) {
    if (data == null) return null;
    
    final parts = fieldPath.split('.');
    dynamic current = data;
    
    for (final part in parts) {
      if (current == null) return null;
      
      if (current is Map) {
        current = current[part];
      } else if (current is List && _isNumeric(part)) {
        final index = int.tryParse(part);
        if (index != null && index >= 0 && index < current.length) {
          current = current[index];
        } else {
          return null;
        }
      } else {
        // Try to access as a property using reflection or other means
        current = _extractProperty(current, part);
      }
    }
    
    return current;
  }

  /// Helper to check if a string represents a numeric index.
  bool _isNumeric(String str) {
    return int.tryParse(str) != null;
  }

  /// Helper to extract a property from an object.
  /// This is a placeholder - in a real implementation, you might use
  /// reflection or other mechanisms depending on your payload structure.
  dynamic _extractProperty(dynamic object, String propertyName) {
    // This would need to be implemented based on your specific payload types
    // For now, return null to indicate the property couldn't be accessed
    return null;
  }
}

/// Field extractor for JSON-based payloads.
/// Handles nested JSON structures with dot notation field paths.
class JsonFieldExtractor<P extends CRDTPayload<V>, V> extends BaseFieldExtractor<P, V> {
  /// Function to convert a payload to JSON-like structure.
  /// This allows flexibility in how different payload types expose their data.
  final Map<String, dynamic> Function(P payload) payloadToJson;

  JsonFieldExtractor({required this.payloadToJson});

  @override
  dynamic extractField(MerkleNode<P> node, String fieldPath) {
    try {
      final jsonData = payloadToJson(node.payload);
      return navigateFieldPath(jsonData, fieldPath);
    } catch (e) {
      return null;
    }
  }

  @override
  List<String> computeSupportedFields() {
    // For JSON extractors, we can't know all possible fields ahead of time
    // This would typically be populated based on schema or discovered dynamically
    return [
      'id',
      'type',
      'timestamp',
      'author',
      'content',
      'metadata',
      'metadata.created',
      'metadata.updated',
      'metadata.version',
    ];
  }
}

/// Field extractor for binary payloads with a known schema.
/// This is useful for structured binary data formats.
class BinaryFieldExtractor<P extends CRDTPayload<V>, V> extends BaseFieldExtractor<P, V> {
  /// Schema definition for binary field extraction
  final Map<String, BinaryFieldDefinition> schema;

  BinaryFieldExtractor({required this.schema});

  @override
  dynamic extractField(MerkleNode<P> node, String fieldPath) {
    final fieldDef = schema[fieldPath];
    if (fieldDef == null) return null;

    try {
      // This would extract from binary payload based on field definition
      // Implementation depends on your specific binary format
      return fieldDef.extract(node.payload);
    } catch (e) {
      return null;
    }
  }

  @override
  List<String> computeSupportedFields() {
    return schema.keys.toList();
  }
}

/// Definition for a binary field extraction.
class BinaryFieldDefinition {
  final String name;
  final Type type;
  final dynamic Function(dynamic payload) extract;

  const BinaryFieldDefinition({
    required this.name,
    required this.type,
    required this.extract,
  });
}

/// Custom field extractor base class for user-defined extraction logic.
/// Users can extend this to implement domain-specific field extraction.
abstract class CustomFieldExtractor<P extends CRDTPayload<V>, V> extends BaseFieldExtractor<P, V> {
  /// Subclasses implement this to provide custom extraction logic.
  @override
  dynamic extractField(MerkleNode<P> node, String fieldPath);

  /// Subclasses implement this to define their supported fields.
  @override
  List<String> computeSupportedFields();
}

/// Composite field extractor that delegates to multiple extractors.
/// This allows combining different extraction strategies.
class CompositeFieldExtractor<P extends CRDTPayload<V>, V> implements FieldExtractor<P, V> {
  final List<FieldExtractor<P, V>> extractors;

  CompositeFieldExtractor(this.extractors);

  @override
  dynamic extractField(MerkleNode<P> node, String fieldPath) {
    for (final extractor in extractors) {
      if (extractor.supportsField(fieldPath)) {
        final result = extractor.extractField(node, fieldPath);
        if (result != null) return result;
      }
    }
    return null;
  }

  @override
  List<String> getSupportedFields() {
    final allFields = <String>{};
    for (final extractor in extractors) {
      allFields.addAll(extractor.getSupportedFields());
    }
    return allFields.toList();
  }

  @override
  bool supportsField(String fieldPath) {
    return extractors.any((extractor) => extractor.supportsField(fieldPath));
  }

  @override
  Map<String, dynamic> extractFields(MerkleNode<P> node, List<String> fieldPaths) {
    final result = <String, dynamic>{};
    for (final fieldPath in fieldPaths) {
      result[fieldPath] = extractField(node, fieldPath);
    }
    return result;
  }
}
