/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

class AuthResources {
    static final String ROOT = "/";
    static final String NAMESPACE_FORMAT = ROOT + "%s";
    static final String NAMESPACE_GROUP_FORMAT = NAMESPACE_FORMAT + "%s";
    static final String NAMESPACE_GROUP_SCHEMA_FORMAT = NAMESPACE_GROUP_FORMAT + "/schemas";
    static final String NAMESPACE_GROUP_CODEC_FORMAT = NAMESPACE_GROUP_FORMAT + "/codecs";
    static final String GROUP_FORMAT = ROOT + "%s";
    static final String GROUP_SCHEMA_FORMAT = GROUP_FORMAT + "/schemas";
    static final String GROUP_CODEC_FORMAT = GROUP_FORMAT + "/codecs";
}
