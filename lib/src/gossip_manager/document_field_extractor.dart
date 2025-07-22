import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode;

import '../p2p/crdt/document_crdt.dart';
import '../p2p/crdt/vector_clock.dart';
import 'field_extractor.dart';

/// Field extractor for DocumentCRDT payloads.
/// Supports dot notation field paths, array indexing, and metadata access.
class DocumentFieldExtractor extends BaseFieldExtractor<DocumentCRDT, Map<String, dynamic>> {
  /// Cache for dynamically discovered fields
  final Set<String> _discoveredFields = <String>{};

  @override
  dynamic extractField(MerkleNode<DocumentCRDT> node, String fieldPath) {
    try {
      // Handle metadata fields
      if (fieldPath.startsWith('_metadata.')) {
        return _extractMetadataField(node, fieldPath);
      }

      // Handle CRDT internal fields
      if (fieldPath.startsWith('_crdt.')) {
        return _extractCrdtField(node, fieldPath);
      }

      // Extract from document value
      final documentValue = node.payload.value;
      final result = navigateFieldPath(documentValue, fieldPath);
      
      // Cache discovered fields for future queries
      if (result != null) {
        _discoveredFields.add(fieldPath);
      }
      
      return result;
    } catch (e) {
      return null;
    }
  }

  /// Extracts metadata fields from the MerkleNode.
  dynamic _extractMetadataField(MerkleNode<DocumentCRDT> node, String fieldPath) {
    final metadataPath = fieldPath.substring('_metadata.'.length);
    
    // Common metadata fields that might be available
    switch (metadataPath) {
      case 'cid':
        return node.cid?.toString();
      case 'size':
        return node.payload.toCanonicalBytes().length;
      default:
        // Try to extract from node metadata if available
        // This would depend on how metadata is stored in MerkleNode
        return null;
    }
  }

  /// Extracts CRDT-specific fields from the DocumentCRDT.
  dynamic _extractCrdtField(MerkleNode<DocumentCRDT> node, String fieldPath) {
    final crdtPath = fieldPath.substring('_crdt.'.length);
    final crdt = node.payload;
    
    switch (crdtPath) {
      case 'clock':
        return crdt.clock.toMap();
      case 'strategy':
        return crdt.strategy.name;
      case 'keys':
        return crdt.keys.toList();
      case 'allKeys':
        return crdt.allKeys.toList();
      case 'length':
        return crdt.length;
      case 'isEmpty':
        return crdt.isEmpty;
      default:
        // Handle clock-specific fields
        if (crdtPath.startsWith('clock.')) {
          final clockPath = crdtPath.substring('clock.'.length);
          return _extractClockField(crdt.clock, clockPath);
        }
        
        // Handle entry-specific fields
        if (crdtPath.startsWith('entry.')) {
          final entryPath = crdtPath.substring('entry.'.length);
          return _extractEntryField(crdt, entryPath);
        }
        
        return null;
    }
  }

  /// Extracts fields from the vector clock.
  dynamic _extractClockField(VectorClock clock, String fieldPath) {
    switch (fieldPath) {
      case 'actors':
        return clock.actors.toList();
      case 'length':
        return clock.length;
      case 'isEmpty':
        return clock.isEmpty;
      default:
        // Handle actor-specific timestamps: clock.actor1, clock.actor2, etc.
        return clock.getTime(fieldPath);
    }
  }

  /// Extracts fields from document entries.
  dynamic _extractEntryField(DocumentCRDT crdt, String fieldPath) {
    final parts = fieldPath.split('.');
    if (parts.length < 2) return null;
    
    final key = parts[0];
    final entryFieldPath = parts.sublist(1).join('.');
    final entry = crdt.getEntry(key);
    
    if (entry == null) return null;
    
    switch (entryFieldPath) {
      case 'value':
        return entry.value;
      case 'timestamp':
        return entry.timestamp.toMap();
      case 'isDeleted':
        return entry.isDeleted;
      case 'actorId':
        return entry.actorId;
      default:
        // Handle nested timestamp fields
        if (entryFieldPath.startsWith('timestamp.')) {
          final timestampPath = entryFieldPath.substring('timestamp.'.length);
          return _extractClockField(entry.timestamp, timestampPath);
        }
        
        // Handle nested value fields
        if (entryFieldPath.startsWith('value.')) {
          final valuePath = entryFieldPath.substring('value.'.length);
          return navigateFieldPath(entry.value, valuePath);
        }
        
        return null;
    }
  }

  @override
  List<String> computeSupportedFields() {
    // Base document fields that are commonly supported
    final baseFields = [
      // Common document fields
      'id',
      'type',
      'title',
      'content',
      'description',
      'name',
      'value',
      'status',
      'category',
      'tags',
      'author',
      'createdAt',
      'updatedAt',
      'version',
      
      // Nested object fields (examples)
      'user.id',
      'user.name',
      'user.email',
      'profile.name',
      'profile.avatar',
      'settings.theme',
      'settings.language',
      'metadata.created',
      'metadata.updated',
      'metadata.version',
      
      // Array access examples
      'tags[0]',
      'items[0].id',
      'items[0].name',
      'comments[0].text',
      'comments[0].author',
      
      // Metadata fields
      '_metadata.cid',
      '_metadata.links',
      '_metadata.linkCount',
      '_metadata.size',
      
      // CRDT fields
      '_crdt.clock',
      '_crdt.strategy',
      '_crdt.keys',
      '_crdt.allKeys',
      '_crdt.length',
      '_crdt.isEmpty',
      '_crdt.clock.actors',
      '_crdt.clock.length',
      '_crdt.clock.isEmpty',
    ];
    
    // Add dynamically discovered fields
    final allFields = <String>{...baseFields, ..._discoveredFields};
    return allFields.toList()..sort();
  }

  @override
  bool supportsField(String fieldPath) {
    // Always support metadata and CRDT fields
    if (fieldPath.startsWith('_metadata.') || fieldPath.startsWith('_crdt.')) {
      return true;
    }
    
    // Check if it's in our supported fields list
    if (getSupportedFields().contains(fieldPath)) {
      return true;
    }
    
    // Support any field path that looks valid (contains only valid characters)
    return _isValidFieldPath(fieldPath);
  }

  /// Checks if a field path has valid syntax.
  bool _isValidFieldPath(String fieldPath) {
    if (fieldPath.isEmpty) return false;
    
    // Allow alphanumeric characters, dots, underscores, and array notation
    final validPattern = RegExp(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*|\[\d+\])*$');
    return validPattern.hasMatch(fieldPath);
  }

  @override
  dynamic navigateFieldPath(dynamic data, String fieldPath) {
    if (data == null) return null;
    
    final parts = _parseFieldPath(fieldPath);
    dynamic current = data;
    
    for (final part in parts) {
      if (current == null) return null;
      
      if (part.isArrayIndex) {
        // Handle array indexing
        if (current is List) {
          final index = part.arrayIndex!;
          if (index >= 0 && index < current.length) {
            current = current[index];
          } else {
            return null;
          }
        } else {
          return null;
        }
      } else {
        // Handle object property access
        if (current is Map) {
          current = current[part.key];
        } else {
          // Try to access as a property using reflection or other means
          current = _extractProperty(current, part.key);
        }
      }
    }
    
    return current;
  }

  /// Parses a field path into individual parts, handling array notation.
  List<_FieldPathPart> _parseFieldPath(String fieldPath) {
    final parts = <_FieldPathPart>[];
    final segments = fieldPath.split('.');
    
    for (final segment in segments) {
      if (segment.contains('[') && segment.contains(']')) {
        // Handle array notation: "items[0]" -> "items" + [0]
        final bracketIndex = segment.indexOf('[');
        final closeBracketIndex = segment.indexOf(']');
        
        if (bracketIndex > 0 && closeBracketIndex > bracketIndex) {
          final key = segment.substring(0, bracketIndex);
          final indexStr = segment.substring(bracketIndex + 1, closeBracketIndex);
          final index = int.tryParse(indexStr);
          
          if (index != null) {
            parts.add(_FieldPathPart.property(key));
            parts.add(_FieldPathPart.arrayIndex(index));
          } else {
            // Invalid array index, treat as regular property
            parts.add(_FieldPathPart.property(segment));
          }
        } else {
          // Invalid array notation, treat as regular property
          parts.add(_FieldPathPart.property(segment));
        }
      } else {
        parts.add(_FieldPathPart.property(segment));
      }
    }
    
    return parts;
  }

  /// Helper to extract a property from an object.
  dynamic _extractProperty(dynamic object, String propertyName) {
    // For DocumentCRDT, we mainly work with Maps and basic types
    // This could be extended with reflection if needed
    return null;
  }

  /// Clears the cache of discovered fields.
  void clearDiscoveredFields() {
    _discoveredFields.clear();
    // Note: The supported fields cache is managed by the base class
  }

  /// Adds a field to the discovered fields cache.
  void addDiscoveredField(String fieldPath) {
    _discoveredFields.add(fieldPath);
    // Note: The supported fields cache is managed by the base class
  }

  /// Returns the set of dynamically discovered fields.
  Set<String> get discoveredFields => Set<String>.from(_discoveredFields);
}

/// Represents a part of a field path (either a property name or array index).
class _FieldPathPart {
  final String key;
  final int? arrayIndex;
  final bool isArrayIndex;

  _FieldPathPart.property(this.key)
      : arrayIndex = null,
        isArrayIndex = false;

  _FieldPathPart.arrayIndex(int index)
      : key = '',
        arrayIndex = index,
        isArrayIndex = true;

  @override
  String toString() {
    return isArrayIndex ? '[$arrayIndex]' : key;
  }
}

/// Specialized field extractor for specific document types.
/// This can be extended for domain-specific document structures.
abstract class TypedDocumentFieldExtractor<T> extends DocumentFieldExtractor {
  /// The document type this extractor handles.
  String get documentType;

  /// Validates that a document matches the expected type.
  bool isValidDocument(Map<String, dynamic> document);

  /// Extracts type-specific fields that might not be available in the generic extractor.
  dynamic extractTypeSpecificField(Map<String, dynamic> document, String fieldPath);

  @override
  dynamic extractField(MerkleNode<DocumentCRDT> node, String fieldPath) {
    final document = node.payload.value;
    
    // Check if this is the right document type
    if (!isValidDocument(document)) {
      return super.extractField(node, fieldPath);
    }

    // Try type-specific extraction first
    final typeSpecificResult = extractTypeSpecificField(document, fieldPath);
    if (typeSpecificResult != null) {
      return typeSpecificResult;
    }

    // Fall back to generic extraction
    return super.extractField(node, fieldPath);
  }
}

/// Example typed extractor for user profile documents.
class UserProfileFieldExtractor extends TypedDocumentFieldExtractor<Map<String, dynamic>> {
  @override
  String get documentType => 'user_profile';

  @override
  bool isValidDocument(Map<String, dynamic> document) {
    return document['type'] == 'user_profile' || 
           document.containsKey('username') ||
           document.containsKey('email');
  }

  @override
  dynamic extractTypeSpecificField(Map<String, dynamic> document, String fieldPath) {
    switch (fieldPath) {
      case 'displayName':
        return document['displayName'] ?? document['name'] ?? document['username'];
      case 'fullName':
        final firstName = document['firstName'] ?? '';
        final lastName = document['lastName'] ?? '';
        return '$firstName $lastName'.trim();
      case 'isActive':
        return document['status'] == 'active';
      case 'avatarUrl':
        return document['avatar']?['url'] ?? document['profileImage'];
      default:
        return null;
    }
  }

  @override
  List<String> computeSupportedFields() {
    final baseFields = super.computeSupportedFields();
    final profileFields = [
      'displayName',
      'fullName',
      'isActive',
      'avatarUrl',
      'username',
      'email',
      'firstName',
      'lastName',
      'bio',
      'location',
      'website',
      'joinedAt',
      'lastActive',
    ];
    
    return [...baseFields, ...profileFields]..sort();
  }
}
