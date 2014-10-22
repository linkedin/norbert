package com.google.protobuf;

/**
 * A small utility to avoid byte copying of arrays by protocol buffers.
 *
 * Trades safety/security for performance.
 *
 */
public class ByteStringUtils {

    public static ByteString wrap(byte[] buffer) {
        return new LiteralByteString(buffer);
    }
}
