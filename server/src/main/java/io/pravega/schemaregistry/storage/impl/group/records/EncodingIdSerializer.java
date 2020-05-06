/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group.records;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;

import java.io.IOException;

/**
 * Encoding Id generated by registry service for each unique combination of schema version and codec type. 
 * The encoding id will typically be attached to the encoded data in a header to describe how to parse the following data. 
 * The registry service exposes APIs to resolve encoding id to {@link EncodingInfo} objects that include details about the
 * encoding used. 
 */
public class EncodingIdSerializer extends VersionedSerializer.WithBuilder<EncodingId, EncodingId.EncodingIdBuilder> {
    public static final EncodingIdSerializer SERIALIZER = new EncodingIdSerializer();

    @Override
        protected EncodingId.EncodingIdBuilder newBuilder() {
            return EncodingId.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(EncodingId e, RevisionDataOutput target) throws IOException {
            target.writeInt(e.getId());
        }

        private void read00(RevisionDataInput source, EncodingId.EncodingIdBuilder b) throws IOException {
            b.id(source.readInt());
        }
}