/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import io.pravega.common.ObjectBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * For each group unique set of Encoding Ids are generated for each unique combination of schema version and codec types
 * registered in the group.
 * The encoding id will typically be attached to the encoded data in a header to describe how to parse the following data. 
 * The registry service exposes APIs to resolve encoding id to {@link EncodingInfo} objects that include details about the
 * encoding used. 
 */
@Data
@Builder
@AllArgsConstructor
public class EncodingId {
    /**
     * A 4byte id that uniquely identifies a {@link VersionInfo} and codecType pair. 
     */
    private final int id;

    public static class EncodingIdBuilder implements ObjectBuilder<EncodingId> {
    }
}