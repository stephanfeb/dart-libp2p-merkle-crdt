import 'dart:async';
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart' show CID;
// Import the IBlockStore interface from the bitswap package
// Assuming the bitswap package will be available under 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart'
// or similar, once 'pub get' is run.
import 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart' show IBlockStore; 

class InMemoryBlockStore implements IBlockStore {
  final Map<String, Uint8List> _store = {};

  @override
  Future<bool> has(CID cid) async {
    return _store.containsKey(cid.toString());
  }

  @override
  Future<Uint8List?> get(CID cid) async {
    return _store[cid.toString()];
  }

  @override
  Future<void> put(CID cid, Uint8List data) async {
    _store[cid.toString()] = data;
  }

  Future<void> delete(CID cid) async {
    _store.remove(cid.toString());
  }

  Future<void> clear() async {
    _store.clear();
  }
}
