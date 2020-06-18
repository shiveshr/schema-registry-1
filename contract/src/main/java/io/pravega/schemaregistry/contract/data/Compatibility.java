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

/**
 * Compatibility that are applied for checking if a schema is valid. 
 * There can be one of three rules {@link AllowAny}, {@link DenyAll}, {@link BackwardAndForward}.
 * AllowAny allows all schema changes without performing any checks. DenyAll rejects all schema changes. 
 * And BackwardAndForward defines the exact backwardAndForward checks that need to be performed before schema is added.
 * The schema will be compared against one or more existing schemas in the group by checking it for satisfying each of the 
 * rules. 
 */
public interface Compatibility {
}
