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

import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.Schema;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

@Slf4j
abstract class AbstractDeserializer<T> extends FailingSerializer<T> {
    private static final byte PROTOCOL = 0x0;
    private static final int HEADER_SIZE = 1 + Integer.BYTES;

    private final String groupId;
    private final SchemaRegistryClient client;
    // This can be null. If no schema is supplied, it means the intent is to deserialize into writer schema. 
    // If headers are not encoded, then this will be the latest schema from the registry
    private final SchemaInfo schemaInfo;
    private final boolean encodeHeader;
    private final SerializerConfig.Decoder decoder;
    private final boolean skipHeaders;
    private final EncodingCache encodingCache;

    protected AbstractDeserializer(String groupId,
                                   SchemaRegistryClient client,
                                   @Nullable Schema<T> schema,
                                   boolean skipHeaders,
                                   SerializerConfig.Decoder decoder,
                                   EncodingCache encodingCache,
                                   boolean encodeHeader) {
        this.groupId = groupId;
        this.client = client;
        this.encodingCache = encodingCache;
        this.schemaInfo = schema == null ? null : schema.getSchemaInfo();
        this.encodeHeader = encodeHeader;
        this.skipHeaders = skipHeaders;
        this.decoder = decoder;
            
        initialize();
    }

    @Synchronized
    private void initialize() {
        GroupProperties groupProperties = client.getGroupProperties(groupId);

        if (schemaInfo != null) {
            log.info("Validate caller supplied schema.");
            if (!client.canReadUsing(groupId, schemaInfo)) {
                throw new IllegalArgumentException("Cannot read using schema" + schemaInfo.getType());
            }
        } else {
            if (!this.encodeHeader) {
                log.warn("No reader schema is supplied and stream does not have encoding headers.");
            }
        }
    }
    
    @SneakyThrows(IOException.class)
    @Override
    public T deserialize(ByteBuffer data) {
        int start = data.arrayOffset() + data.position();
        if (this.encodeHeader) {
            SchemaInfo writerSchema = null;
            ByteBuffer decoded;
            if (skipHeaders) {
                data.position(start + HEADER_SIZE);
                decoded = data;
            } else {
                byte protocol = data.get();
                EncodingId encodingId = new EncodingId(data.getInt());
                EncodingInfo encodingInfo = encodingCache.getGroupEncodingInfo(encodingId);
                writerSchema = encodingInfo.getSchemaInfo();
                decoded = decoder.decode(encodingInfo.getCodecType(), data);
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(decoded.array(), 
                    decoded.arrayOffset() + decoded.position(), decoded.remaining());
            if (schemaInfo == null) { // deserialize into writer schema
                // pass writer schema for schema to be read into
                return deserialize(bais, writerSchema, writerSchema);
            } else {
                // pass reader schema for schema on read to the underlying implementation
                return deserialize(bais, writerSchema, schemaInfo);
            }
        } else {
            // pass reader schema for schema on read to the underlying implementation
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data.array(), 
                    data.arrayOffset() + data.position(), data.remaining());

            return deserialize(inputStream, null, schemaInfo);
        }
    }
    
    protected abstract T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) throws IOException;
    
    boolean isEncodeHeader() {
        return encodeHeader;
    }
}