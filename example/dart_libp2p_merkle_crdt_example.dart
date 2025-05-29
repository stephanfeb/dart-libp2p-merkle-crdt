import 'dart:async';
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart';
import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/identity/actor.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/note.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/follow_activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/like_activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/following_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/liked_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/outbox_crdt.dart';
import 'package:logging/logging.dart';

// --- Minimal Mock Network/Storage Components for Conceptual Example ---
// These mocks are simplified to avoid pulling in full libp2p dependencies for the example.
// They are NOT functional replacements for real libp2p components.

// It's often easier to use the real types and accept that the example won't run standalone
// without a full libp2p setup, rather than trying to perfectly mock complex interfaces.
// However, for this iteration, we'll keep mocks minimal.
// The type errors in previous attempts often came from P2PStreamDagFetcher and GossipSubAnnouncer
// expecting concrete, fully implemented Host, PubSub, IpfsDHT.

// For P2PStreamDagFetcher and GossipSubAnnouncer, we'd need:
// import 'package:dart_libp2p/dart_libp2p.dart' show Host, PeerId;
// import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart' show PubSub, Message, Subscription;
// import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHT; // Or another concrete DHT impl

// To make the example runnable without these, we'd have to mock MerkleCrdtGossipManager itself,
// or significantly simplify its internal dependencies, which is beyond the scope of this example.
// So, the following mocks are very basic.

class MockLibp2pServices {
  // This class would encapsulate mock Host, PubSub, DHT if we were to make them more functional.
  // For now, individual managers will use very basic, non-functional mocks.
}

final _logger = Logger('ClientExample');

// Mock Application Database
class MockAppDB {
  static final MockAppDB instance = MockAppDB._();
  MockAppDB._();

  final Map<String, Actor> actors = {};
  // To avoid complex mock setup for MerkleCrdtGossipManager, we'll simulate its effect directly on Actor's CRDTs.
  final Map<String, Activity> _globalActivities = {}; // cid.toString() -> Activity
  final Map<String, Note> _globalNotes = {}; // cid.toString() -> Note


  Future<Actor> createAndRegisterActor(String name) async {
    final actor = await Actor.create();
    actors[actor.did] = actor;
    _logger.info('[MockAppDB] Created actor $name with DID: ${actor.did}');
    // In a real app, managers for actor's own CRDTs would be set up here.
    return actor;
  }

  void storeActivity(CID cid, Activity activity) {
    _globalActivities[cid.toString()] = activity;
    _logger.info('[MockAppDB] Stored Activity ${cid.toString()} (type: ${activity.type})');
  }
  
  void storeNote(CID noteCid, Note note) {
     _globalNotes[noteCid.toString()] = note;
     _logger.info('[MockAppDB] Stored Note ${noteCid.toString()}');
  }

  Activity? getActivity(CID cid) => _globalActivities[cid.toString()];
  Note? getNote(CID cid) => _globalNotes[cid.toString()];
}

Future<void> runClientAggregationExample(Actor localUser) async {
  _logger.info('--- Running Client Aggregation Example for ${localUser.did} ---');

  // 1. Client can sync `following` set of the local user.
  // (Directly from actor state in this simplified example)
  Set<String> followedDids = localUser.following;
  _logger.info('Local user ${localUser.did} is following: $followedDids');

  // 2. Client syncs `outbox` CRDTs of followed actors.
  _logger.info('Conceptually syncing outboxes of followed actors...');
  Map<String, List<CID>> followedOutboxes = {}; 

  for (final followedDid in followedDids) {
    final followedActor = MockAppDB.instance.actors[followedDid];
    if (followedActor != null) {
      // In a real app, this involves setting up a MerkleCrdtGossipManager
      // to subscribe to 'crdt/outbox/<followedDid>' and sync the CRDT.
      // For this example, we directly access the followed actor's outbox state.
      followedOutboxes[followedDid] = followedActor.outboxActivities;
      _logger.info('Fetched (mocked) outbox for $followedDid: ${followedOutboxes[followedDid]?.length ?? 0} activities');
    }
  }

  // 3. Basic chronological feed generation from aggregated outboxes.
  _logger.info('Generating chronological feed...');
  List<Activity> feedActivities = [];
  List<CID> allActivityCids = [];
  followedOutboxes.forEach((_, cids) => allActivityCids.addAll(cids));
  _logger.info('Total activity CIDs from followed outboxes: ${allActivityCids.length}');

  for (final activityCid in allActivityCids) {
    final activity = MockAppDB.instance.getActivity(activityCid);
    if (activity != null) {
      feedActivities.add(activity);
    }
  }

  feedActivities.sort((a, b) => b.published.compareTo(a.published));

  _logger.info('--- Generated Feed (${feedActivities.length} items) ---');
  for (final activity in feedActivities) {
    String contentPreview = '';
    if (activity is CreateActivity) {
      final note = MockAppDB.instance.getNote(activity.object);
      contentPreview = 'created content: ${note?.content ?? activity.object.toString()}';
    } else if (activity is FollowActivity) {
        contentPreview = 'followed ${activity.objectDid}';
    } else if (activity is LikeActivity) {
        contentPreview = 'liked content ${activity.objectCid.toString()}';
    }
    _logger.info('[Feed] ${activity.published.toIso8601String()} - ${activity.actor} ${activity.type} ($contentPreview)');
  }
  _logger.info('--- End of Feed ---');

  // 4. Display liked content (e.g., what the localUser has liked)
  Set<CID> likedCids = localUser.likedItems;
  _logger.info('Local user ${localUser.did} has liked content with CIDs:');
  likedCids.forEach((cid) => _logger.info('  - ${cid.toString()}'));
}

Future<void> main() async {
  Logger.root.level = Level.INFO;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, STACK: ${record.stackTrace}');
  });

  _logger.info('=== ActivityPub/CRDT Client Example Starting ===');
  
  final alice = await MockAppDB.instance.createAndRegisterActor('Alice');
  final bob = await MockAppDB.instance.createAndRegisterActor('Bob');
  final charlie = await MockAppDB.instance.createAndRegisterActor('Charlie');

  // --- Simulate some actions (directly updating Actor's CRDTs and MockAppDB) ---

  // Alice creates a note
  var aliceNoteResult = await alice.createNote("Hello from Alice!");
  MockAppDB.instance.storeNote(aliceNoteResult.noteCid, aliceNoteResult.note);
  var aliceCreateNoteActivityResult = await alice.createAndSignActivity(objectCid: aliceNoteResult.noteCid);
  MockAppDB.instance.storeActivity(aliceCreateNoteActivityResult.activityCid, aliceCreateNoteActivityResult.activity);
  alice.addActivityToOutbox(aliceCreateNoteActivityResult.activityCid); // Directly update actor's outbox
  _logger.info('Alice created a note. Activity CID: ${aliceCreateNoteActivityResult.activityCid}');

  // Bob creates a note
  var bobNoteResult = await bob.createNote("Bob's first post!");
  MockAppDB.instance.storeNote(bobNoteResult.noteCid, bobNoteResult.note);
  var bobCreateNoteActivityResult = await bob.createAndSignActivity(objectCid: bobNoteResult.noteCid);
  MockAppDB.instance.storeActivity(bobCreateNoteActivityResult.activityCid, bobCreateNoteActivityResult.activity);
  bob.addActivityToOutbox(bobCreateNoteActivityResult.activityCid);
  _logger.info('Bob created a note. Activity CID: ${bobCreateNoteActivityResult.activityCid}');

  // Alice follows Bob
  var aliceFollowBobResult = await alice.follow(bob.did); // This updates alice._followingCrdt
  MockAppDB.instance.storeActivity(aliceFollowBobResult.activityCid, aliceFollowBobResult.activity);
  alice.addActivityToOutbox(aliceFollowBobResult.activityCid);
  _logger.info('Alice followed Bob. Follow Activity CID: ${aliceFollowBobResult.activityCid}');

  // Alice follows Charlie
  var aliceFollowCharlieResult = await alice.follow(charlie.did);
  MockAppDB.instance.storeActivity(aliceFollowCharlieResult.activityCid, aliceFollowCharlieResult.activity);
  alice.addActivityToOutbox(aliceFollowCharlieResult.activityCid);
  _logger.info('Alice followed Charlie. Follow Activity CID: ${aliceFollowCharlieResult.activityCid}');

  // Bob likes Alice's first note (the content is the note, so we use aliceNoteResult.noteCid)
  var bobLikeAliceNoteResult = await bob.like(aliceNoteResult.noteCid); 
  MockAppDB.instance.storeActivity(bobLikeAliceNoteResult.activityCid, bobLikeAliceNoteResult.activity);
  bob.addActivityToOutbox(bobLikeAliceNoteResult.activityCid);
  _logger.info('Bob liked Alice\'s note (Note CID: ${aliceNoteResult.noteCid}). Like Activity CID: ${bobLikeAliceNoteResult.activityCid}');
  
  _logger.info('Simulating data propagation (not needed for this simplified example)...');
  await Future.delayed(Duration(milliseconds: 50));

  await runClientAggregationExample(alice);
  
  _logger.info('--- Running Client Aggregation Example for Bob\'s client view ---');
  await runClientAggregationExample(bob);

  _logger.info('=== ActivityPub/CRDT Client Example Finished ===');
}
