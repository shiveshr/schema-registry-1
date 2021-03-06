/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client;

import java.net.URI;
import org.junit.Test;
import static org.junit.Assert.*;

public class SchemaRegistryClientTest {
    @Test
    public void testClientFactory() throws Exception {
        URI schemaRegistryUri = URI.create("http://localhost:9092");
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
                                                                      .schemaRegistryUri(schemaRegistryUri)
                                                                      .build();

        SchemaRegistryClient client = SchemaRegistryClientFactory.withDefaultNamespace(config);
        assertNull(client.getNamespace());
        client.close();
        client = SchemaRegistryClientFactory.withNamespace("a", config);

        assertEquals("a", client.getNamespace());
    }
}
