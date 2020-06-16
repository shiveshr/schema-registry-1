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

import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
class AbstractResource {
    @Context
    HttpHeaders headers;

    @Getter
    private final SchemaRegistryService registryService;

    AbstractResource(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    CompletableFuture<Response> withCompletion(String request, Supplier<CompletableFuture<Response>> future) {
        try {
            return future.get();
        } catch (IllegalArgumentException e) {
            log.warn("Bad request {}", request);
            return CompletableFuture.completedFuture(Response.status(Response.Status.BAD_REQUEST).build());
        } catch (Exception e) {
            log.error("request failed with exception {}", e);
            return Futures.failedFuture(e);
        }
    }

}
