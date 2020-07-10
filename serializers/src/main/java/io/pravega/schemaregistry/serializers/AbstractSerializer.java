/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import com.google.common.base.Preconditions;
import io.pravega.common.util.BitConverter;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.schemas.Schema;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractSerializer<T> extends BaseSerializer<T> {
    private static final byte PROTOCOL = 0x1;
    private static final int HEADER_LENGTH = Byte.BYTES + Integer.BYTES;

    private final String groupId;
    
    private final SchemaInfo schemaInfo;
    private final AtomicReference<EncodingId> encodingId;
    private final boolean encodeHeader;
    private final SchemaRegistryClient client;
    @Getter
    private final Codec codec;
    private final boolean registerSchema;
    
    protected AbstractSerializer(String groupId,
                                 SchemaRegistryClient client,
                                 Schema<T> schema,
                                 Codec codec,
                                 boolean registerSchema, 
                                 boolean encodeHeader) {
        Preconditions.checkNotNull(groupId);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(codec);
        Preconditions.checkNotNull(schema);
        
        this.groupId = groupId;
        this.client = client;
        this.schemaInfo = schema.getSchemaInfo();
        this.registerSchema = registerSchema;
        this.encodingId = new AtomicReference<>();
        this.codec = codec;
        this.encodeHeader = encodeHeader;
        initialize();
    }
    
    private void initialize() {
        VersionInfo version;
        if (registerSchema) {
            // register schema
            version = client.addSchema(groupId, schemaInfo);
        } else {
            // get already registered schema version. If schema is not registered, this will throw an exception. 
            version = client.getVersionForSchema(groupId, schemaInfo);
        }
        if (encodeHeader) {
            encodingId.set(client.getEncodingId(groupId, version, codec.getCodecType().getName()));
        }
    }
    
    @SneakyThrows(IOException.class)
    @Override
    public ByteBuffer serialize(T obj) {
        ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
        if (this.encodeHeader) {
            dataStream.write(PROTOCOL);
            BitConverter.writeInt(dataStream, encodingId.get().getId());
        }
        
        serialize(obj, schemaInfo, dataStream);
        dataStream.flush();
        byte[] serialized = dataStream.toByteArray();
        
        ByteBuffer byteBuffer;
        if (this.encodeHeader) {
            if (codec.equals(Codecs.None.getCodec())) {
                // If no encoding is performed. we can directly use the original serialized byte array.
                byteBuffer = ByteBuffer.wrap(serialized);
            } else {
                ByteBuffer wrap = ByteBuffer.wrap(serialized, HEADER_LENGTH, serialized.length - HEADER_LENGTH);
                ByteBuffer encoded = codec.encode(wrap);
                int bufferSize = HEADER_LENGTH + encoded.remaining();
                byteBuffer = ByteBuffer.allocate(bufferSize);
                // copy the header from serialized array into encoded output array
                byteBuffer.put(serialized, 0, HEADER_LENGTH);
                byteBuffer.put(encoded);
                byteBuffer.rewind();
            }
        } else {
            byteBuffer = ByteBuffer.wrap(serialized);
        }
        
        return byteBuffer;
    }

    protected abstract void serialize(T var, SchemaInfo schema, OutputStream outputStream) throws IOException;
}
