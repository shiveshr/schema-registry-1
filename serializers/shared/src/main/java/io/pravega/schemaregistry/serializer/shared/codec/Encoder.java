/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.shared.codec;

import io.pravega.schemaregistry.contract.data.CodecType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Defines method to encode data.
 */
public interface Encoder {
    /**
     * Codec type for the encoder. 
     * 
     * @return Codec Type for the encoder. 
     */
    CodecType getCodecType();
    
    /**
     * Implementation should encode the remaining bytes in the buffer and return a new ByteBuffer that includes
     * the encoded data at its current position. 
     * 
     * The implementation can optionally call flush or close on outputstream with no consequence. 
     * 
     * @param data ByteBuffer to encode.
     * @param outputStream OutputStream where the encoded data should be written.
     * @throws IOException IOException can be thrown while reading from or writing to byte buffers.
     */
    void encode(ByteBuffer data, OutputStream outputStream) throws IOException;
}
