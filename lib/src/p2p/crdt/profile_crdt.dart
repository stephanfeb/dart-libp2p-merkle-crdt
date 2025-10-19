import 'dart:typed_data';
import 'dart:convert' show jsonEncode, jsonDecode;

import 'package:crypto/crypto.dart' as crypto;
import 'package:dart_multihash/dart_multihash.dart';
import 'package:merkledag/merkledag.dart' show CRDTPayload; // Assuming this path and interface
import 'package:dcid/dcid.dart' show CID;

// Helper class for LWW entries
class LwwEntry<T> {
  final T value;
  final int timestamp; // Unix epoch milliseconds

  LwwEntry(this.value, this.timestamp);

  Map<String, dynamic> toJson() => {
        'value': value is CID ? value.toString() : value,
        'timestamp': timestamp,
      };

  factory LwwEntry.fromJson(Map<String, dynamic> json, T Function(dynamic) valueDecoder) {
    return LwwEntry(valueDecoder(json['value']), json['timestamp'] as int);
  }
}

// Represents the resolved, viewable profile data
class ProfileData {
  final String id; // DID, immutable for this profile instance
  final String type; // e.g., "Person", "Service"
  final Map<String, String> publicKeyJwk; // Immutable for this profile instance

  final String? name;
  final String? preferredUsername;
  final String? summary;
  final CID? icon; // CID to an image
  final CID? image; // CID to an image

  ProfileData({
    required this.id,
    required this.publicKeyJwk,
    this.type = "Person",
    this.name,
    this.preferredUsername,
    this.summary,
    this.icon,
    this.image,
  });

  // Convenience factory to create an empty profile for an actor
  factory ProfileData.empty(String did, Map<String, String> jwk) {
    return ProfileData(id: did, publicKeyJwk: jwk);
  }

  @override
  String toString() {
    return 'ProfileData(id: $id, type: $type, name: $name, preferredUsername: $preferredUsername, summary: $summary, icon: $icon, image: $image, publicKeyJwk: $publicKeyJwk)';
  }
}

// ProfileCrdtPayload implements CRDTPayload<ProfileData>
// It uses an LWW-Map strategy for mutable fields.
class ProfileCrdtPayload implements CRDTPayload<ProfileData> {
  // Immutable fields for this specific CRDT instance
  final String _id; // Actor's DID
  final Map<String, String> _publicKeyJwk;

  // LWW mutable fields
  // Keys: 'type', 'name', 'preferredUsername', 'summary', 'icon', 'image'
  final Map<String, LwwEntry<dynamic>> _fields;

  ProfileCrdtPayload._(this._id, this._publicKeyJwk, this._fields);

  factory ProfileCrdtPayload.create(String did, Map<String, String> publicKeyJwk) {
    return ProfileCrdtPayload._(did, publicKeyJwk, {});
  }


  // --- CRDTPayload<ProfileData> implementation ---

  @override
  ProfileData get value {
    CID? parseCidSafe(dynamic val) {
      if (val is String) {
        try {
          return CID.fromString(val); // Changed from parse to fromString
        } catch (_) {
          return null;
        }
      }
      return null;
    }
    return ProfileData(
      id: _id,
      publicKeyJwk: _publicKeyJwk,
      type: _fields['type']?.value as String? ?? "Person",
      name: _fields['name']?.value as String?,
      preferredUsername: _fields['preferredUsername']?.value as String?,
      summary: _fields['summary']?.value as String?,
      icon: parseCidSafe(_fields['icon']?.value),
      image: parseCidSafe(_fields['image']?.value),
    );
  }

  ProfileCrdtPayload set(String key, dynamic val) {
    final newFields = Map<String, LwwEntry<dynamic>>.from(_fields);
    final timestamp = DateTime.now().millisecondsSinceEpoch;

    // Type check for safety, especially for CIDs
    dynamic valueToStore = val;
    if (key == 'icon' || key == 'image') {
      if (val != null && val is! CID) {
        throw ArgumentError('Value for $key must be a CID or null.');
      }
      valueToStore = val?.toString(); // Store CIDs as strings in LwwEntry
    } else if (key == 'type' || key == 'name' || key == 'preferredUsername' || key == 'summary') {
      if (val != null && val is! String) {
        throw ArgumentError('Value for $key must be a String or null.');
      }
    } else if (val != null) {
        // Allow other types if necessary, but be mindful of serialization
    }


    if (val == null) {
      // Setting to null effectively tombstones the field if we want to support explicit nulls
      // Or, we could remove it, but LWW typically keeps entries.
      // For simplicity, allow null values to be stored with a timestamp.
      newFields[key] = LwwEntry(null, timestamp);
    } else {
       newFields[key] = LwwEntry(valueToStore, timestamp);
    }
    return ProfileCrdtPayload._(_id, _publicKeyJwk, newFields);
  }

  @override
  ProfileCrdtPayload merge(covariant ProfileCrdtPayload other) {
    if (_id != other._id) {
      throw ArgumentError('Cannot merge profiles with different DIDs.');
    }
    // publicKeyJwk should also match, or a strategy is needed. For now, assume they do.

    final mergedFields = Map<String, LwwEntry<dynamic>>.from(_fields);

    other._fields.forEach((key, otherEntry) {
      final localEntry = mergedFields[key];
      if (localEntry == null || otherEntry.timestamp > localEntry.timestamp) {
        mergedFields[key] = otherEntry;
      } else if (otherEntry.timestamp == localEntry.timestamp) {
        // Tie-breaking rule: prefer higher value (e.g., lexicographically for strings)
        // This is a common LWW tie-breaking strategy.
        // For simplicity here, we can just take 'other' or 'local' consistently, e.g., prefer 'other'.
        // A more robust solution might compare the values themselves if they are comparable.
        // If values are CIDs (stored as strings), string comparison works.
        if (otherEntry.value != null && localEntry.value != null) {
            if (otherEntry.value.toString().compareTo(localEntry.value.toString()) > 0) {
                 mergedFields[key] = otherEntry;
            }
        } else if (otherEntry.value != null) { // other is not null, local is null
            mergedFields[key] = otherEntry;
        }
        // if localEntry.value is not null and otherEntry.value is null, keep localEntry (already there)
        // if both are null, it doesn't matter
      }
    });
    return ProfileCrdtPayload._(_id, _publicKeyJwk, mergedFields);
  }

  factory ProfileCrdtPayload.fromBytes(Uint8List bytes) {
    final jsonMap = jsonDecode(String.fromCharCodes(bytes)) as Map<String, dynamic>;
    final id = jsonMap['id'] as String;
    final publicKeyJwk = (jsonMap['publicKeyJwk'] as Map<dynamic, dynamic>).cast<String, String>();
    
    final fieldsJson = jsonMap['fields'] as Map<String, dynamic>;
    final fields = fieldsJson.map((key, entryJson) {
      // Need to decode value based on key type, especially for CIDs
      dynamic valueDecoder(dynamic val) {
        if (key == 'icon' || key == 'image') {
          return val != null ? CID.fromString(val as String) : null; // Changed from parse to fromString
        }
        return val; // Assumes String or null for others
      }
      // The LwwEntry.fromJson expects a valueDecoder for the specific type T.
      // Here, T is dynamic, but we store CIDs as strings.
      // So, when reconstructing, we need to parse CIDs back.
      // For simplicity, LwwEntry.fromJson will receive the raw value,
      // and the `get value` accessor in ProfileData will handle parsing.
      // This means LwwEntry will store CID.toString() for icon/image.
      return MapEntry(
          key,
          LwwEntry.fromJson(entryJson as Map<String, dynamic>, (dynamic v) => v) 
          // The value 'v' from JSON is already stringified CID or other primitive
      );
    });

    return ProfileCrdtPayload._(id, publicKeyJwk, fields);
  }

  // Required by CRDTPayload for canonical representation, e.g. for signing or consistent hashing.
  // For JSON-based data, canonical form often involves sorting keys.
  @override
  Uint8List toCanonicalBytes() {
    // For this implementation, we'll assume toBytes() is canonical enough.
    // A more rigorous canonical form might sort keys in all maps.
    // For example, sorting keys in _fields before serializing.
    // However, jsonEncode by default does not guarantee key order.
    // For true canonical JSON, one might need a specialized library or manual key sorting.
    // Let's make a simple attempt by sorting the top-level keys of the main map.
    final sortedFields = Map.fromEntries(
      _fields.entries.toList()..sort((e1, e2) => e1.key.compareTo(e2.key))
    );

    final jsonMap = {
      'id': _id,
      'publicKeyJwk': _publicKeyJwk, // Assuming key order in publicKeyJwk is stable or doesn't matter for canonical form here
      'fields': sortedFields.map((key, entry) => MapEntry(key, entry.toJson())),
    };
    return Uint8List.fromList(jsonEncode(jsonMap).codeUnits);
  }
}
