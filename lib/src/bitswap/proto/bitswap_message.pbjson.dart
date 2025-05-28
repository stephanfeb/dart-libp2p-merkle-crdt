//
//  Generated code. Do not modify.
//  source: bitswap_message.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use messageDescriptor instead')
const Message$json = {
  '1': 'Message',
  '2': [
    {'1': 'wantlist', '3': 1, '4': 1, '5': 11, '6': '.bitswap.pb.Message.Wantlist', '10': 'wantlist'},
    {'1': 'payload', '3': 3, '4': 3, '5': 11, '6': '.bitswap.pb.Message.Block', '10': 'payload'},
    {'1': 'blockPresences', '3': 4, '4': 3, '5': 11, '6': '.bitswap.pb.Message.BlockPresence', '10': 'blockPresences'},
    {'1': 'pendingBytes', '3': 5, '4': 1, '5': 5, '10': 'pendingBytes'},
  ],
  '3': [Message_Wantlist$json, Message_Block$json, Message_BlockPresence$json],
  '4': [Message_BlockPresenceType$json],
};

@$core.Deprecated('Use messageDescriptor instead')
const Message_Wantlist$json = {
  '1': 'Wantlist',
  '2': [
    {'1': 'entries', '3': 1, '4': 3, '5': 11, '6': '.bitswap.pb.Message.Wantlist.Entry', '10': 'entries'},
    {'1': 'full', '3': 2, '4': 1, '5': 8, '10': 'full'},
  ],
  '3': [Message_Wantlist_Entry$json],
  '4': [Message_Wantlist_WantType$json],
};

@$core.Deprecated('Use messageDescriptor instead')
const Message_Wantlist_Entry$json = {
  '1': 'Entry',
  '2': [
    {'1': 'block', '3': 1, '4': 1, '5': 12, '10': 'block'},
    {'1': 'priority', '3': 2, '4': 1, '5': 5, '10': 'priority'},
    {'1': 'cancel', '3': 3, '4': 1, '5': 8, '10': 'cancel'},
    {'1': 'wantType', '3': 4, '4': 1, '5': 14, '6': '.bitswap.pb.Message.Wantlist.WantType', '10': 'wantType'},
    {'1': 'sendDontHave', '3': 5, '4': 1, '5': 8, '10': 'sendDontHave'},
  ],
};

@$core.Deprecated('Use messageDescriptor instead')
const Message_Wantlist_WantType$json = {
  '1': 'WantType',
  '2': [
    {'1': 'Block', '2': 0},
    {'1': 'Have', '2': 1},
  ],
};

@$core.Deprecated('Use messageDescriptor instead')
const Message_Block$json = {
  '1': 'Block',
  '2': [
    {'1': 'prefix', '3': 1, '4': 1, '5': 12, '10': 'prefix'},
    {'1': 'data', '3': 2, '4': 1, '5': 12, '10': 'data'},
  ],
};

@$core.Deprecated('Use messageDescriptor instead')
const Message_BlockPresence$json = {
  '1': 'BlockPresence',
  '2': [
    {'1': 'cid', '3': 1, '4': 1, '5': 12, '10': 'cid'},
    {'1': 'type', '3': 2, '4': 1, '5': 14, '6': '.bitswap.pb.Message.BlockPresenceType', '10': 'type'},
  ],
};

@$core.Deprecated('Use messageDescriptor instead')
const Message_BlockPresenceType$json = {
  '1': 'BlockPresenceType',
  '2': [
    {'1': 'Have', '2': 0},
    {'1': 'DontHave', '2': 1},
  ],
};

/// Descriptor for `Message`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List messageDescriptor = $convert.base64Decode(
    'CgdNZXNzYWdlEjgKCHdhbnRsaXN0GAEgASgLMhwuYml0c3dhcC5wYi5NZXNzYWdlLldhbnRsaX'
    'N0Ugh3YW50bGlzdBIzCgdwYXlsb2FkGAMgAygLMhkuYml0c3dhcC5wYi5NZXNzYWdlLkJsb2Nr'
    'UgdwYXlsb2FkEkkKDmJsb2NrUHJlc2VuY2VzGAQgAygLMiEuYml0c3dhcC5wYi5NZXNzYWdlLk'
    'Jsb2NrUHJlc2VuY2VSDmJsb2NrUHJlc2VuY2VzEiIKDHBlbmRpbmdCeXRlcxgFIAEoBVIMcGVu'
    'ZGluZ0J5dGVzGrgCCghXYW50bGlzdBI8CgdlbnRyaWVzGAEgAygLMiIuYml0c3dhcC5wYi5NZX'
    'NzYWdlLldhbnRsaXN0LkVudHJ5UgdlbnRyaWVzEhIKBGZ1bGwYAiABKAhSBGZ1bGwauAEKBUVu'
    'dHJ5EhQKBWJsb2NrGAEgASgMUgVibG9jaxIaCghwcmlvcml0eRgCIAEoBVIIcHJpb3JpdHkSFg'
    'oGY2FuY2VsGAMgASgIUgZjYW5jZWwSQQoId2FudFR5cGUYBCABKA4yJS5iaXRzd2FwLnBiLk1l'
    'c3NhZ2UuV2FudGxpc3QuV2FudFR5cGVSCHdhbnRUeXBlEiIKDHNlbmREb250SGF2ZRgFIAEoCF'
    'IMc2VuZERvbnRIYXZlIh8KCFdhbnRUeXBlEgkKBUJsb2NrEAASCAoESGF2ZRABGjMKBUJsb2Nr'
    'EhYKBnByZWZpeBgBIAEoDFIGcHJlZml4EhIKBGRhdGEYAiABKAxSBGRhdGEaXAoNQmxvY2tQcm'
    'VzZW5jZRIQCgNjaWQYASABKAxSA2NpZBI5CgR0eXBlGAIgASgOMiUuYml0c3dhcC5wYi5NZXNz'
    'YWdlLkJsb2NrUHJlc2VuY2VUeXBlUgR0eXBlIisKEUJsb2NrUHJlc2VuY2VUeXBlEggKBEhhdm'
    'UQABIMCghEb250SGF2ZRAB');

