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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Object that encapsulates schemaInfo with its associated version. 
 */
@Data
@Builder
@AllArgsConstructor
public class SchemaWithVersion {
    /**
     * Schema Information object. 
     */
    private @NonNull final SchemaInfo schemaInfo;
    /**
     * Version information object that identifies the corresponding schema object. 
     */
    private @NonNull final VersionInfo versionInfo;
}
