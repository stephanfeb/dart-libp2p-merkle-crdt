import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:merkledag/merkledag.dart';
import 'package:test/test.dart';

void main() {
  test('put never replaces a block already stored under the same CID', () async {
    final store = InMemoryBlockStore<GSet<String>, Set<String>>();
    final original = MerkleNode.create(GSet<String>.fromList(['real']), {});
    final forged = MerkleNode(
        cid: original.cid, payload: GSet<String>.fromList(['forged']), children: {});

    await store.put(original);
    await store.put(forged);

    expect((await store.get(original.cid))!.payload.value, {'real'});
    expect(await store.has(original.cid), isTrue);
    expect(store.length, 1);
  });
}
