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
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.ProtobufSchema;

import java.io.InputStream;

public class ProtobufDeserlizer<T extends GeneratedMessageV3> extends AbstractDeserializer<T> {
    private final ProtobufSchema<T> protobufSchema;
    ProtobufDeserlizer(String groupId, SchemaRegistryClient client,
                       ProtobufSchema<T> schema, SerializerConfig.Decoder decoder,
                       EncodingCache encodingCache, boolean encodeHeader) {
        super(groupId, client, schema, true, decoder, encodingCache, encodeHeader);
        Preconditions.checkNotNull(schema);
        this.protobufSchema = schema;
    }

    @Override
    protected T deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        try {
            return protobufSchema.getParser().parseFrom(inputStream);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid bytes", e);
        }
    }
}
