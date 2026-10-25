// lib/src/p2p/crdt/canonical_json.dart
import 'dart:convert' show jsonEncode, utf8;
import 'dart:typed_data';

/// Encodes [value] as JSON with every map's keys sorted, so equal values
/// always produce the same string.
String canonicalJsonEncode(Object? value) => jsonEncode(_sorted(value));

/// [canonicalJsonEncode] as UTF-8 bytes.
Uint8List canonicalJsonBytes(Object? value) =>
    Uint8List.fromList(utf8.encode(canonicalJsonEncode(value)));

Object? _sorted(Object? value) {
  if (value is Map) {
    final keys = value.keys.map((k) => k.toString()).toList()..sort();
    return {for (final k in keys) k: _sorted(value[k])};
  }
  if (value is List) return value.map(_sorted).toList();
  return value;
}
