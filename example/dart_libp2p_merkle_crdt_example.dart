import 'dart:async';
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart';
import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/identity/actor.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/note.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/follow_activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/like_activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/announce_activity.dart'; // Import AnnounceActivity
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/delete_activity.dart'; // Import DeleteActivity
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/following_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/liked_crdt.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/outbox_crdt.dart';
import 'package:logging/logging.dart';

// --- Minimal Mock Network/Storage Components for Conceptual Example ---
// These mocks are simplified to avoid pulling in full libp2p dependencies for the example.
// They are NOT functional replacements for real libp2p components.

class MockLibp2pServices {
  // This class would encapsulate mock Host, PubSub, DHT if we were to make them more functional.
}

final _logger = Logger('ClientExample');

// Mock Application Database
class MockAppDB {
  static final MockAppDB instance = MockAppDB._();
  MockAppDB._();

  final Map<String, Actor> actors = {};
  final Map<String, Activity> _globalActivities = {}; // cid.toString() -> Activity
  final Map<String, Note> _globalNotes = {}; // cid.toString() -> Note
  final Set<CID> _deletedObjectCids = {}; // Store CIDs of objects marked by DeleteActivity

  Future<Actor> createAndRegisterActor(String name) async {
    final actor = await Actor.create();
    actors[actor.did] = actor;
    _logger.info('[MockAppDB] Created actor $name with DID: ${actor.did}');
    return actor;
  }

  void storeActivity(CID cid, Activity activity) {
    _globalActivities[cid.toString()] = activity;
    _logger.info('[MockAppDB] Stored Activity ${cid.toString()} (type: ${activity.type})');
    if (activity is DeleteActivity) {
      _deletedObjectCids.add(activity.object);
      // Also mark the DeleteActivity itself as "processed" for deletion effects,
      // or handle its display specially. For now, just track the target.
      // If the object of DeleteActivity is another Activity (e.g. a CreateActivity),
      // we might also want to mark that CreateActivity's object (the Note) as deleted.
      final deletedTargetActivity = _globalActivities[activity.object.toString()];
      if (deletedTargetActivity is CreateActivity) {
        _deletedObjectCids.add(deletedTargetActivity.object); // Mark the Note as deleted too
      }
       _logger.info('[MockAppDB] Processed DeleteActivity for object: ${activity.object}');
    }
  }
  
  void storeNote(CID noteCid, Note note) {
     _globalNotes[noteCid.toString()] = note;
     _logger.info('[MockAppDB] Stored Note ${noteCid.toString()}');
  }

  Activity? getActivity(CID cid) {
    if (_deletedObjectCids.contains(cid)) {
      // _logger.info('[MockAppDB] Attempted to get deleted activity: $cid');
      // return null; // Or return a special marker if needed
    }
    return _globalActivities[cid.toString()];
  }

  Note? getNote(CID cid) {
     if (_deletedObjectCids.contains(cid)) {
      _logger.info('[MockAppDB] Attempted to get deleted note: $cid. Content will be hidden.');
      return Note(content: "[Content Deleted]", published: DateTime.now().toUtc(), context: activityStreamsContext);
    }
    return _globalNotes[cid.toString()];
  }

  bool isDeleted(CID cid) => _deletedObjectCids.contains(cid);

  // Conceptual unpinning
  void unpinContent(CID cid) {
    _logger.info('[MockAppDB] Unpinned (conceptually) content: ${cid.toString()}');
    // In a real system, this would remove the content from being served by Bitswap.
    // For this mock, we could also remove it from _globalNotes or _globalActivities
    // if we want to simulate it being completely gone, but for "tombstoning"
    // just marking it as deleted and not serving it is often enough.
    // Other clients might also choose to unpin from their local cache.
  }
}

Future<void> runClientAggregationExample(Actor localUser) async {
  _logger.info('--- Running Client Aggregation Example for ${localUser.did} ---');

  Set<String> followedDids = localUser.following;
  _logger.info('Local user ${localUser.did} is following: $followedDids');

  Map<String, List<CID>> followedOutboxes = {}; 
  for (final followedDid in followedDids) {
    final followedActor = MockAppDB.instance.actors[followedDid];
    if (followedActor != null) {
      followedOutboxes[followedDid] = followedActor.outboxActivities;
      _logger.info('Fetched (mocked) outbox for $followedDid: ${followedOutboxes[followedDid]?.length ?? 0} activities');
    }
  }

  List<Activity> feedActivities = [];
  List<CID> allActivityCids = [];
  followedOutboxes.forEach((_, cids) => allActivityCids.addAll(cids));
  _logger.info('Total activity CIDs from followed outboxes: ${allActivityCids.length}');

  // Pre-process to identify all DeleteActivities and their targets
  Set<CID> globallyDeletedObjectCids = Set.from(MockAppDB.instance._deletedObjectCids);
  for (final activityCid in allActivityCids) {
    final activity = MockAppDB.instance.getActivity(activityCid);
    if (activity is DeleteActivity) {
      globallyDeletedObjectCids.add(activity.object);
      // If the deleted object is a CreateActivity, also mark its object (the Note) as deleted.
      final targetActivity = MockAppDB.instance.getActivity(activity.object);
      if (targetActivity is CreateActivity) {
        globallyDeletedObjectCids.add(targetActivity.object);
      }
    }
  }


  for (final activityCid in allActivityCids) {
    final activity = MockAppDB.instance.getActivity(activityCid);
    if (activity != null) {
      // Skip adding activities that are themselves deleted or whose objects are deleted
      if (globallyDeletedObjectCids.contains(activityCid)) {
        _logger.info('[FeedGen] Skipping deleted activity: ${activityCid}');
        continue;
      }
      if (activity is CreateActivity && globallyDeletedObjectCids.contains(activity.object)) {
         _logger.info('[FeedGen] Skipping Create activity for deleted note: ${activity.object}');
        continue;
      }
      if (activity is AnnounceActivity && globallyDeletedObjectCids.contains(activity.object)) {
        // If an Announce targets a deleted CreateActivity, skip it.
        final announcedOriginalActivity = MockAppDB.instance.getActivity(activity.object);
        if (announcedOriginalActivity is CreateActivity && globallyDeletedObjectCids.contains(announcedOriginalActivity.object)) {
            _logger.info('[FeedGen] Skipping Announce of a deleted note (via CreateActivity): ${activity.object}');
            continue;
        } else if (globallyDeletedObjectCids.contains(activity.object)) { // Announce of a directly deleted Note CID
             _logger.info('[FeedGen] Skipping Announce of a deleted object: ${activity.object}');
            continue;
        }
      }
       if (activity is LikeActivity && globallyDeletedObjectCids.contains(activity.objectCid)) {
        _logger.info('[FeedGen] Skipping Like activity for deleted content: ${activity.objectCid}');
        continue;
      }

      feedActivities.add(activity);
    }
  }

  feedActivities.sort((a, b) => b.published.compareTo(a.published));

  _logger.info('--- Generated Feed (${feedActivities.length} items) ---');
  Map<CID, List<CreateActivity>> repliesMap = {};
  List<Activity> topLevelActivities = [];

  for (final activity in feedActivities) {
    if (activity is CreateActivity && activity.inReplyTo != null) {
      if (globallyDeletedObjectCids.contains(activity.inReplyTo!)) {
        _logger.info('[FeedDisplay] Skipping reply to a deleted item: ${activity.object} replies to ${activity.inReplyTo}');
        continue;
      }
      repliesMap.putIfAbsent(activity.inReplyTo!, () => []).add(activity);
    } else {
      topLevelActivities.add(activity);
    }
  }
  
  repliesMap.forEach((_, replyList) {
    replyList.sort((a, b) => a.published.compareTo(b.published));
  });

  for (final activity in topLevelActivities) {
    String contentPreview = '';
    CID? activityObjectCid;

    if (activity is DeleteActivity) {
      contentPreview = 'deleted content ${activity.object.toString()}';
       _logger.info('[Feed] ${activity.published.toIso8601String()} - ${activity.actor} ${activity.type} ($contentPreview)');
      continue; // Don't show replies to delete activities, or further process them here
    }


    if (activity is CreateActivity) {
      final note = MockAppDB.instance.getNote(activity.object); // Will return "[Content Deleted]" if applicable
      contentPreview = 'created content: "${note?.content ?? activity.object.toString()}"';
      activityObjectCid = activity.object; 
    } else if (activity is FollowActivity) {
        contentPreview = 'followed ${activity.objectDid}';
    } else if (activity is LikeActivity) {
        contentPreview = 'liked content ${activity.objectCid.toString()}';
    } else if (activity is AnnounceActivity) {
      final announcedContentCid = activity.object;
      final originalActivity = MockAppDB.instance.getActivity(announcedContentCid);
      if (originalActivity is CreateActivity) {
        final originalNote = MockAppDB.instance.getNote(originalActivity.object);
        contentPreview = 'announced: [${originalActivity.actor} created: "${originalNote?.content ?? originalActivity.object.toString()}"]';
      } else {
        final originalNote = MockAppDB.instance.getNote(announcedContentCid);
        if (originalNote != null) {
           contentPreview = 'announced: [Note: "${originalNote.content}"]';
        } else {
           contentPreview = 'announced content ${announcedContentCid.toString()} (possibly deleted)';
        }
      }
    }
    _logger.info('[Feed] ${activity.published.toIso8601String()} - ${activity.actor} ${activity.type} ($contentPreview)');

    if (activity is CreateActivity && activityObjectCid != null && repliesMap.containsKey(activityObjectCid)) {
      if (MockAppDB.instance.isDeleted(activityObjectCid)) {
         _logger.info('  (Original post for replies below is deleted)');
      }
      for (final replyActivity in repliesMap[activityObjectCid]!) {
        final replyNote = MockAppDB.instance.getNote(replyActivity.object);
        String replyContentPreview = replyNote?.content ?? replyActivity.object.toString();
        _logger.info('  [ReplyTo: ${replyActivity.inReplyTo.toString().substring(0,10)}...] ${replyActivity.published.toIso8601String()} - ${replyActivity.actor} created content: "$replyContentPreview"');
      }
    }
  }
  _logger.info('--- End of Feed ---');

  Set<CID> likedCids = localUser.likedItems.where((cid) => !globallyDeletedObjectCids.contains(cid)).toSet();
  _logger.info('Local user ${localUser.did} has liked content with CIDs (excluding deleted):');
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

  // Alice creates a note
  var aliceNoteResult = await alice.createNote("Hello from Alice!");
  MockAppDB.instance.storeNote(aliceNoteResult.noteCid, aliceNoteResult.note);
  var aliceCreateNoteActivityResult = await alice.createAndSignActivity(objectCid: aliceNoteResult.noteCid);
  MockAppDB.instance.storeActivity(aliceCreateNoteActivityResult.activityCid, aliceCreateNoteActivityResult.activity);
  alice.addActivityToOutbox(aliceCreateNoteActivityResult.activityCid);
  _logger.info('Alice created a note. Activity CID: ${aliceCreateNoteActivityResult.activityCid}, Note CID: ${aliceNoteResult.noteCid}');

  // Bob creates a note
  var bobNoteResult = await bob.createNote("Bob's first post!");
  MockAppDB.instance.storeNote(bobNoteResult.noteCid, bobNoteResult.note);
  var bobCreateNoteActivityResult = await bob.createAndSignActivity(objectCid: bobNoteResult.noteCid);
  MockAppDB.instance.storeActivity(bobCreateNoteActivityResult.activityCid, bobCreateNoteActivityResult.activity);
  bob.addActivityToOutbox(bobCreateNoteActivityResult.activityCid);
  _logger.info('Bob created a note. Activity CID: ${bobCreateNoteActivityResult.activityCid}');

  // Alice follows Bob and Charlie
  var aliceFollowBobResult = await alice.follow(bob.did);
  MockAppDB.instance.storeActivity(aliceFollowBobResult.activityCid, aliceFollowBobResult.activity);
  alice.addActivityToOutbox(aliceFollowBobResult.activityCid);
  _logger.info('Alice followed Bob. Follow Activity CID: ${aliceFollowBobResult.activityCid}');
  var aliceFollowCharlieResult = await alice.follow(charlie.did);
  MockAppDB.instance.storeActivity(aliceFollowCharlieResult.activityCid, aliceFollowCharlieResult.activity);
  alice.addActivityToOutbox(aliceFollowCharlieResult.activityCid);
  _logger.info('Alice followed Charlie. Follow Activity CID: ${aliceFollowCharlieResult.activityCid}');

  // Bob likes Alice's first note
  var bobLikeAliceNoteResult = await bob.like(aliceNoteResult.noteCid); 
  MockAppDB.instance.storeActivity(bobLikeAliceNoteResult.activityCid, bobLikeAliceNoteResult.activity);
  bob.addActivityToOutbox(bobLikeAliceNoteResult.activityCid);
  _logger.info('Bob liked Alice\'s note (Note CID: ${aliceNoteResult.noteCid}). Like Activity CID: ${bobLikeAliceNoteResult.activityCid}');

  // Charlie replies to Alice's first note
  var charlieReplyNoteResult = await charlie.createNote("Good point, Alice!");
  MockAppDB.instance.storeNote(charlieReplyNoteResult.noteCid, charlieReplyNoteResult.note);
  var charlieReplyActivity = await charlie.createAndSignActivity(
    objectCid: charlieReplyNoteResult.noteCid,
    inReplyTo: aliceNoteResult.noteCid, 
  );
  MockAppDB.instance.storeActivity(charlieReplyActivity.activityCid, charlieReplyActivity.activity);
  charlie.addActivityToOutbox(charlieReplyActivity.activityCid);
  _logger.info('Charlie replied to Alice\'s note (Note CID: ${aliceNoteResult.noteCid}). Reply Activity CID: ${charlieReplyActivity.activityCid}');
  
  // Alice replies to Charlie's reply
  var aliceReplyToCharlieNoteResult = await alice.createNote("Thanks Charlie!");
  MockAppDB.instance.storeNote(aliceReplyToCharlieNoteResult.noteCid, aliceReplyToCharlieNoteResult.note);
  var aliceReplyToCharlieActivity = await alice.createAndSignActivity(
    objectCid: aliceReplyToCharlieNoteResult.noteCid,
    inReplyTo: charlieReplyNoteResult.noteCid, 
  );
  MockAppDB.instance.storeActivity(aliceReplyToCharlieActivity.activityCid, aliceReplyToCharlieActivity.activity);
  alice.addActivityToOutbox(aliceReplyToCharlieActivity.activityCid);
  _logger.info('Alice replied to Charlie\'s reply (Note CID: ${charlieReplyNoteResult.noteCid}). Reply Activity CID: ${aliceReplyToCharlieActivity.activityCid}');

  // Bob announces Alice's first note (announcing the CreateActivity for the note)
  var bobAnnounceAliceNoteResult = await bob.announce(aliceCreateNoteActivityResult.activityCid);
  MockAppDB.instance.storeActivity(bobAnnounceAliceNoteResult.activityCid, bobAnnounceAliceNoteResult.activity);
  bob.addActivityToOutbox(bobAnnounceAliceNoteResult.activityCid);
  _logger.info('Bob announced Alice\'s note (CreateActivity CID: ${aliceCreateNoteActivityResult.activityCid}). Announce Activity CID: ${bobAnnounceAliceNoteResult.activityCid}');

  _logger.info('\n--- Running Aggregation for Alice (Before Deletion) ---');
  await runClientAggregationExample(alice);

  // Alice deletes her first note (by deleting the Note's CID directly)
  var aliceDeleteNoteResult = await alice.delete(aliceNoteResult.noteCid);
  MockAppDB.instance.storeActivity(aliceDeleteNoteResult.activityCid, aliceDeleteNoteResult.activity);
  alice.addActivityToOutbox(aliceDeleteNoteResult.activityCid);
  _logger.info('Alice deleted her note (Note CID: ${aliceNoteResult.noteCid}). Delete Activity CID: ${aliceDeleteNoteResult.activityCid}');
  
  // Bob creates another note
  var bobNote2Result = await bob.createNote("Bob's second post, after Alice deleted hers.");
  MockAppDB.instance.storeNote(bobNote2Result.noteCid, bobNote2Result.note);
  var bobCreateNote2ActivityResult = await bob.createAndSignActivity(objectCid: bobNote2Result.noteCid);
  MockAppDB.instance.storeActivity(bobCreateNote2ActivityResult.activityCid, bobCreateNote2ActivityResult.activity);
  bob.addActivityToOutbox(bobCreateNote2ActivityResult.activityCid);
  _logger.info('Bob created a second note. Activity CID: ${bobCreateNote2ActivityResult.activityCid}');

  // Charlie creates a note mentioning Alice
  var charlieMentionNoteResult = await charlie.createNote(
    "Hey @${alice.did}, check this out!",
    tags: [Mention(href: alice.did, name: "Alice")]
  );
  MockAppDB.instance.storeNote(charlieMentionNoteResult.noteCid, charlieMentionNoteResult.note);
  var charlieMentionActivity = await charlie.createAndSignActivity(objectCid: charlieMentionNoteResult.noteCid);
  MockAppDB.instance.storeActivity(charlieMentionActivity.activityCid, charlieMentionActivity.activity);
  charlie.addActivityToOutbox(charlieMentionActivity.activityCid);
  _logger.info('Charlie created a note mentioning Alice. Activity CID: ${charlieMentionActivity.activityCid}');


  _logger.info('\nSimulating data propagation after deletion...');
  await Future.delayed(Duration(milliseconds: 50));

  _logger.info('\n--- Running Aggregation for Alice (After Deletion) ---');
  await runClientAggregationExample(alice);
  
  _logger.info('\n--- Running Aggregation for Bob (Sees Alice\'s deletion) ---');
  await runClientAggregationExample(bob);
  
  _logger.info('\n--- Running Aggregation for Charlie (Sees Alice\'s deletion) ---');
  await runClientAggregationExample(charlie);

  _logger.info('\n=== ActivityPub/CRDT Client Example Finished ===');
}
