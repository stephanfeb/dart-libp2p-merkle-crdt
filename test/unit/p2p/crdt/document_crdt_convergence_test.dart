import 'dart:math';

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:test/test.dart';

/// Replicas that write concurrently, sometimes syncing with each other, then
/// merge everything in random orders and groupings.
void main() {
  for (final strategy in [
    ConflictResolutionStrategy.lastWriterWins,
    ConflictResolutionStrategy.firstWriterWins,
  ]) {
    test('${strategy.name}: every merge order converges', () {
      final rng = Random(7);
      for (var trial = 0; trial < 200; trial++) {
        final actors = ['a', 'b', 'c'];
        final replicas = {
          for (final a in actors) a: DocumentCRDT.empty(strategy: strategy)
        };
        final snapshots = <DocumentCRDT>[];
        for (var op = 0; op < 12; op++) {
          final actor = actors[rng.nextInt(3)];
          var doc = replicas[actor]!;
          final key = 'k${rng.nextInt(3)}';
          switch (rng.nextInt(4)) {
            case 0:
              doc = doc.delete(key, actorId: actor);
            case 1:
              doc = doc.merge(replicas[actors[rng.nextInt(3)]]!);
            default:
              doc = doc.put(key, {'v': rng.nextInt(5), 'w': 'é$op'}, actorId: actor);
          }
          replicas[actor] = doc;
          snapshots.add(doc);
        }

        final expected = _foldShuffled(snapshots, Random(trial)).toCanonicalBytes();
        for (var order = 0; order < 5; order++) {
          final result = _foldShuffled(snapshots, Random(trial * 31 + order));
          expect(result.toCanonicalBytes(), expected, reason: 'trial $trial order $order');
          expect(DocumentCRDT.fromBytes(result.toCanonicalBytes()).toCanonicalBytes(), expected);
        }
      }
    });
  }

  test('non-ASCII text survives a canonical-bytes round trip', () {
    final doc = DocumentCRDT.empty().put('greeting', 'héllo 世界 🎉', actorId: 'actör');
    final decoded = DocumentCRDT.fromBytes(doc.toCanonicalBytes());
    expect(decoded.value['greeting'], 'héllo 世界 🎉');
    expect(decoded.getEntry('greeting')!.actorId, 'actör');
    expect(decoded.toCanonicalBytes(), doc.toCanonicalBytes());
  });
}

/// Merges [docs] in a random order, pairing them up in a random tree shape.
DocumentCRDT _foldShuffled(List<DocumentCRDT> docs, Random rng) {
  final pool = List.of(docs)..shuffle(rng);
  while (pool.length > 1) {
    final a = pool.removeAt(rng.nextInt(pool.length));
    final b = pool.removeAt(rng.nextInt(pool.length));
    pool.add(rng.nextBool() ? a.merge(b) : b.merge(a));
  }
  return pool.single;
}
