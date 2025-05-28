//
//  Generated code. Do not modify.
//  source: bitswap_message.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class Message_BlockPresenceType extends $pb.ProtobufEnum {
  static const Message_BlockPresenceType Have = Message_BlockPresenceType._(0, _omitEnumNames ? '' : 'Have');
  static const Message_BlockPresenceType DontHave = Message_BlockPresenceType._(1, _omitEnumNames ? '' : 'DontHave');

  static const $core.List<Message_BlockPresenceType> values = <Message_BlockPresenceType> [
    Have,
    DontHave,
  ];

  static final $core.Map<$core.int, Message_BlockPresenceType> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Message_BlockPresenceType? valueOf($core.int value) => _byValue[value];

  const Message_BlockPresenceType._($core.int v, $core.String n) : super(v, n);
}

class Message_Wantlist_WantType extends $pb.ProtobufEnum {
  static const Message_Wantlist_WantType Block = Message_Wantlist_WantType._(0, _omitEnumNames ? '' : 'Block');
  static const Message_Wantlist_WantType Have = Message_Wantlist_WantType._(1, _omitEnumNames ? '' : 'Have');

  static const $core.List<Message_Wantlist_WantType> values = <Message_Wantlist_WantType> [
    Block,
    Have,
  ];

  static final $core.Map<$core.int, Message_Wantlist_WantType> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Message_Wantlist_WantType? valueOf($core.int value) => _byValue[value];

  const Message_Wantlist_WantType._($core.int v, $core.String n) : super(v, n);
}


const _omitEnumNames = $core.bool.fromEnvironment('protobuf.omit_enum_names');
