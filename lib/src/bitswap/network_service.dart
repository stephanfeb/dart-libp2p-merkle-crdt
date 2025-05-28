import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
// Assuming 'varint' package for unsigned varint encoding/decoding
// If not available, a helper function would be needed.
import 'package:dart_cid/dart_cid.dart' as dart_cid; // Using varint from dart_libp2p
// Placeholder for varint, actual package might differ or need implementation

import './proto/bitswap_message.pb.dart' as pb;

// Callback type for when a full Bitswap message is received
typedef BitswapMessageHandler = void Function(String peerId, pb.Message message);

class BitswapNetworkService {
  final libp2p.Host _host;
  final BitswapMessageHandler _messageHandler;
  final String protocolId = '/ipfs/bitswap/1.2.0'; // Bitswap 1.2.0 protocol ID
  final String nodeNameForLogging; // <-- ADDED THIS

  // Manage active outgoing streams to peers
  final Map<String, libp2p.P2PStream> activeStreams = {};

  BitswapNetworkService(this._host, this._messageHandler, this.nodeNameForLogging) { // <-- ADDED nodeNameForLogging
    print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService instance created.');
    // print('BitswapNetworkService instance created.'); // Redundant with BSW_LOG
  }

  Future<void> start() async {
    _host.setStreamHandler(protocolId, onIncomingStream);
    print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService: Bitswap protocol handler registered for $protocolId');
    // print('Bitswap protocol handler registered for $protocolId'); // Redundant
  }

  Future<void> stop() async {
    _host.removeStreamHandler(protocolId);
    print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService: Bitswap protocol handler unregistered for $protocolId');
    // print('Bitswap protocol handler unregistered for $protocolId'); // Redundant
    for (final stream in activeStreams.values) {
      try {
        await stream.close();
      } catch (e) {
        print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService: Error closing stream ${stream.id} during stop: $e');
        // print('Error closing stream during stop: $e'); // Redundant
      }
    }
    activeStreams.clear();
    print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService: All active streams cleared.');
  }

  Future<void> handlePeerDisconnected(String peerId) async {
    final stream = activeStreams.remove(peerId);
    if (stream != null) {
      print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService: Peer $peerId disconnected, closing and removing active stream ${stream.id}.');
      // print('Peer $peerId disconnected, closing and removing active stream ${stream.id}.'); // Redundant
      try {
        await stream.close();
      } catch (e) {
        print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService: Error closing stream for disconnected peer $peerId: $e');
        // print('Error closing stream for disconnected peer $peerId: $e'); // Redundant
      }
    } else {
      print('// $nodeNameForLogging BSW_LOG: BitswapNetworkService: handlePeerDisconnected called for $peerId, but no active stream found.');
    }
  }

  Future<void> onIncomingStream(libp2p.P2PStream stream, libp2p.PeerId peerIdObj) async {
    final peerId = peerIdObj.toBase58();
    final String baseLogPrefix = '// $nodeNameForLogging BSW_LOG: [Incoming from $peerId on stream ${stream.id}]';
    print('$baseLogPrefix Received and now processing incoming Bitswap stream continuously.');

    BytesBuilder currentMessageBuffer = BytesBuilder();
    int messageCounter = 0;

    try {
      while (true) { // Loop to process multiple messages from the stream
        messageCounter++;
        final String messageLogPrefix = '$baseLogPrefix [Msg #${messageCounter}]';
        
        int? messagePayloadLength;
        int? varintBytesConsumed;
        const maxVarintLen = 9; // Max length for a 64-bit varint, ensures we don't buffer indefinitely for a bad prefix

        // Stage 1: Read and decode the varint length prefix
        // Ensure we have enough bytes to decode a varint or determine stream closure
        bool varintDecoded = false;
        if (currentMessageBuffer.isNotEmpty) { // Check if buffer has data from previous iteration
            try {
                final tempBuffer = currentMessageBuffer.toBytes();
                if (tempBuffer.isNotEmpty) { // Ensure buffer isn't empty before trying to decode
                    final decodedVarint = dart_cid.decodeVarint(tempBuffer);
                    messagePayloadLength = decodedVarint.key;
                    varintBytesConsumed = decodedVarint.value;
                    print('$messageLogPrefix Decoded varint from existing buffer. MessagePayloadLength: $messagePayloadLength, VarintBytesConsumed: $varintBytesConsumed. Buffer size: ${currentMessageBuffer.length} bytes.');
                    if (varintBytesConsumed! <= 0) {
                        print('$messageLogPrefix Error: Varint decoding (from existing buffer) resulted in 0 or negative bytes consumed. Closing stream.');
                        await stream.close(); return;
                    }
                    varintDecoded = true;
                }
            } catch (e) {
                // Not enough bytes in currentMessageBuffer for a full varint, or it's malformed but short.
                // We'll try to read more. If currentMessageBuffer.length >= maxVarintLen, it's an error.
                if (currentMessageBuffer.length >= maxVarintLen) {
                    print('$messageLogPrefix Error decoding varint from existing buffer (length ${currentMessageBuffer.length} >= $maxVarintLen). Error: $e. Buffer: ${currentMessageBuffer.toBytes()}. Closing stream.');
                    await stream.close(); return;
                }
                print('$messageLogPrefix Could not decode varint from existing buffer (length ${currentMessageBuffer.length}, error: $e). Will attempt to read more.');
            }
        }

        while(!varintDecoded) {
            // Read more data for the prefix if not decoded yet
            print('$messageLogPrefix Attempting to read more data for varint prefix. Current buffer size: ${currentMessageBuffer.length}');
            final Uint8List chunk = await stream.read();
            if (chunk.isEmpty) {
                if (currentMessageBuffer.isEmpty) {
                    print('$messageLogPrefix Stream closed cleanly by remote while waiting for new message prefix. No partial data. Exiting loop.');
                } else {
                    print('$messageLogPrefix Stream closed by remote while reading varint prefix. Partial data buffered: ${currentMessageBuffer.length} bytes: ${currentMessageBuffer.toBytes()}. Cannot continue.');
                }
                // Consider closing our side if not already handled by libp2p
                // await stream.close(); // This might be redundant if read() empty means it's already closed.
                return; // Exit onIncomingStream
            }
            currentMessageBuffer.add(chunk);
            print('$messageLogPrefix Read prefix chunk of ${chunk.length} bytes. Total buffered for prefix: ${currentMessageBuffer.length}');

            try {
                final tempBuffer = currentMessageBuffer.toBytes();
                final decodedVarint = dart_cid.decodeVarint(tempBuffer);
                messagePayloadLength = decodedVarint.key;
                varintBytesConsumed = decodedVarint.value;
                print('$messageLogPrefix Decoded varint. MessagePayloadLength: $messagePayloadLength, VarintBytesConsumed: $varintBytesConsumed. Buffer size: ${currentMessageBuffer.length} bytes.');
                if (varintBytesConsumed! <= 0) {
                    print('$messageLogPrefix Error: Varint decoding resulted in 0 or negative bytes consumed. Closing stream.');
                    await stream.close(); return;
                }
                varintDecoded = true; // Break prefix reading loop
            } catch (e) {
                if (currentMessageBuffer.length >= maxVarintLen) {
                    print('$messageLogPrefix Error decoding varint prefix after ${currentMessageBuffer.length} bytes (maxVarintLen: $maxVarintLen). Error: $e. Buffer: ${currentMessageBuffer.toBytes()}. Closing stream.');
                    await stream.close(); return;
                }
                // Not enough bytes yet, or partial varint. Continue reading in this inner loop.
                print('$messageLogPrefix Could not decode varint yet (buffered: ${currentMessageBuffer.length}, error: $e). Reading more for prefix.');
            }
        }
        
        // Stage 2: Read the message body based on decoded length
        final int totalExpectedMessageLengthWithPrefix = varintBytesConsumed! + messagePayloadLength!;
        print('$messageLogPrefix Expecting total message length (prefix + payload) of $totalExpectedMessageLengthWithPrefix bytes.');

        while (currentMessageBuffer.length < totalExpectedMessageLengthWithPrefix) {
          print('$messageLogPrefix Reading message body. Current buffer: ${currentMessageBuffer.length}, expecting: $totalExpectedMessageLengthWithPrefix');
          final Uint8List chunk = await stream.read();
          if (chunk.isEmpty) {
            print('$messageLogPrefix Stream closed by remote while reading message body. Expected $totalExpectedMessageLengthWithPrefix, got ${currentMessageBuffer.length}. Partial data: ${currentMessageBuffer.toBytes()}.');
            // await stream.close(); // Redundant?
            return; // Exit onIncomingStream
          }
          currentMessageBuffer.add(chunk);
          print('$messageLogPrefix Read body chunk of ${chunk.length} bytes. Total buffered: ${currentMessageBuffer.length} / $totalExpectedMessageLengthWithPrefix');
        }

        // Stage 3: Process the complete message
        final Uint8List allDataForThisMessageAndPotentiallyNext = currentMessageBuffer.toBytes();
        
        final messagePayloadBytes = Uint8List.sublistView(
            allDataForThisMessageAndPotentiallyNext, varintBytesConsumed!, totalExpectedMessageLengthWithPrefix);
        print('$messageLogPrefix Extracted message payload of ${messagePayloadBytes.length} bytes.');
        
        final message = pb.Message.fromBuffer(messagePayloadBytes);
        print('$messageLogPrefix Deserialized message. Calling message handler. Wants: ${message.wantlist.entries.length}, Blocks: ${message.payload.length}');
        _messageHandler(peerId, message);

        // Stage 4: Prepare buffer for the next message (if any)
        if (allDataForThisMessageAndPotentiallyNext.length > totalExpectedMessageLengthWithPrefix) {
          final remainingBytes = Uint8List.sublistView(allDataForThisMessageAndPotentiallyNext, totalExpectedMessageLengthWithPrefix);
          print('$messageLogPrefix ${remainingBytes.length} bytes remaining in buffer after processing message. Will be start of next message.');
          currentMessageBuffer = BytesBuilder()..add(remainingBytes);
        } else {
          currentMessageBuffer = BytesBuilder(); // Clear buffer, exactly one message was processed
        }
        // Loop back to process the next message from the stream
      } // End of while(true) loop for messages
    } catch (e, s) {
      print('$baseLogPrefix Error processing messages on stream: $e\nStack: $s. Stream will likely be unusable.');
      // Don't try to close the stream here if libp2p is managing its lifecycle on error.
      // If we need to close, ensure it's idempotent.
    } finally {
      print('$baseLogPrefix Finished all processing for incoming stream ${stream.id}. Stream might be closed or an error occurred.');
      // It's generally safer for the handler not to unilaterally close the stream passed to it by libp2p
      // unless an unrecoverable local error specific to this handler's logic occurs.
      // libp2p layer should manage stream closure on underlying connection errors or remote close.
    }
  }

  Future<void> sendMessage(String peerId, pb.Message message) async {
    final String logPrefix = '// $nodeNameForLogging BSW_LOG: [Outgoing to $peerId]';
    print('$logPrefix Attempting to send Bitswap message. Summary: Wantlist: ${message.hasWantlist()}, Payloads: ${message.payload.length}, Presences: ${message.blockPresences.length}');
    // print('Attempting to send Bitswap message to $peerId.'); // Redundant
    libp2p.P2PStream? stream = activeStreams[peerId];

    try {
      if (stream == null) {
        print('$logPrefix No active stream, opening new one.');
        // print('No active stream to $peerId, opening new one.'); // Redundant
        final peerIdObj = libp2p.PeerId.fromString(peerId);
        print('$logPrefix _host runtimeType: ${_host.runtimeType}, hashCode: ${_host.hashCode}');
        // print('NetworkService: _host runtimeType: ${_host.runtimeType}, hashCode: ${_host.hashCode}'); // Redundant
        print('$logPrefix Calling _host.newStream for peer $peerId ($peerIdObj) with protocol $protocolId');
        // print('NetworkService: Calling _host.newStream for peer $peerId ($peerIdObj)'); // Redundant
        try {
          stream = await _host.newStream(peerIdObj, [protocolId], libp2p.Context());
          print('$logPrefix _host.newStream completed. Stream ID: ${stream?.id}');
          // print('NetworkService: _host.newStream completed. Stream: ${stream?.id}'); // Redundant
        } catch (e, s) {
          print('$logPrefix Error calling _host.newStream for $peerId: $e\nStack: $s');
          // print('NetworkService: Error calling _host.newStream: $e\nStack: $s'); // Redundant
          rethrow;
        }
        if (stream == null) {
          print('$logPrefix _host.newStream returned null for $peerId! Cannot send message.');
          // print('NetworkService: _host.newStream returned null for $peerId!'); // Redundant
          return;
        }
        activeStreams[peerId] = stream;
        print('$logPrefix New stream ${stream.id} to $peerId added to active streams.');
      } else {
        print('$logPrefix Reusing existing stream ${stream.id}.');
        // print('Reusing existing stream to $peerId.'); // Redundant
      }

      final messageBytes = message.writeToBuffer();
      final lengthPrefixBytes = dart_cid.encodeVarint(messageBytes.length); // Renamed for clarity
      
      print('$logPrefix Preparing to send. LengthPrefix: $lengthPrefixBytes (${lengthPrefixBytes.length} bytes), MessagePayload: ${messageBytes.length} bytes. Stream: ${stream.id}');
      print('$logPrefix MessagePayload (first 64 bytes): ${messageBytes.sublist(0, messageBytes.length > 64 ? 64 : messageBytes.length)}');

      print('$logPrefix Writing length prefix (${lengthPrefixBytes.length} bytes) to stream.');
      await stream.write(lengthPrefixBytes);
      print('$logPrefix Writing message payload (${messageBytes.length} bytes) to stream.');
      await stream.write(messageBytes);
      
      print('$logPrefix Bitswap message sent successfully.');
      // print('Bitswap message sent to $peerId using stream ${stream.id}.'); // Redundant

    } catch (e, s) { // Added stack trace to catch
      print('$logPrefix Error sending Bitswap message: $e\nStack: $s');
      // print('Error sending Bitswap message to $peerId: $e'); // Redundant
      if (stream != null) {
        print('$logPrefix Removing and closing stream ${stream.id} to $peerId due to send error.');
        activeStreams.remove(peerId);
        try {
          await stream.close();
        } catch (closeError) {
          print('$logPrefix Error closing stream ${stream.id} after send error: $closeError');
          // print('NetworkService: Error closing stream after send error: $closeError'); // Redundant
        }
      } else {
        print('$logPrefix Stream was null when catching send error for $peerId.');
        // print('NetworkService: Stream was null when catching send error for $peerId.'); // Redundant
      }
    }
  }
}
