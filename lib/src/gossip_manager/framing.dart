// lib/src/gossip_manager/framing.dart
import 'dart:typed_data';

import 'package:dart_libp2p/core/network/stream.dart' show P2PStream;

/// Thrown when a peer sends a frame that is malformed or too large, or closes
/// the stream before a whole frame arrives.
class FrameException implements Exception {
  final String message;
  const FrameException(this.message);

  @override
  String toString() => 'FrameException: $message';
}

/// Prefixes [body] with its length as an unsigned varint.
Uint8List encodeFrame(Uint8List body) {
  final builder = BytesBuilder(copy: false);
  var length = body.length;
  while (length >= 0x80) {
    builder.addByte((length & 0x7F) | 0x80);
    length >>= 7;
  }
  builder
    ..addByte(length)
    ..add(body);
  return builder.toBytes();
}

/// Reads unsigned-varint length-prefixed frames from a [P2PStream].
///
/// A stream's `read` returns whatever chunk the muxer has, which may hold part
/// of a frame or several frames, so this buffers chunks until a whole frame
/// is available.
class FrameReader {
  final P2PStream _stream;
  final List<Uint8List> _chunks = [];
  int _offset = 0; // Read position within _chunks.first.
  int _available = 0;

  FrameReader(this._stream);

  /// Reads the next frame. Throws [FrameException] if its declared length
  /// exceeds [maxLength] or the stream ends first.
  Future<Uint8List> readFrame({required int maxLength}) async {
    var length = 0;
    var shift = 0;
    while (true) {
      final byte = (await _take(1))[0];
      length |= (byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
      if (shift >= 35) throw const FrameException('Frame length varint too long');
    }
    if (length > maxLength) {
      throw FrameException('Frame of $length bytes exceeds limit of $maxLength');
    }
    return _take(length);
  }

  Future<Uint8List> _take(int n) async {
    while (_available < n) {
      final chunk = await _stream.read();
      if (chunk.isEmpty) throw const FrameException('Stream ended mid-frame');
      _chunks.add(chunk);
      _available += chunk.length;
    }
    final out = Uint8List(n);
    var filled = 0;
    while (filled < n) {
      final first = _chunks.first;
      final count = (first.length - _offset).clamp(0, n - filled);
      out.setRange(filled, filled + count, first, _offset);
      filled += count;
      _offset += count;
      if (_offset == first.length) {
        _chunks.removeAt(0);
        _offset = 0;
      }
    }
    _available -= n;
    return out;
  }
}
