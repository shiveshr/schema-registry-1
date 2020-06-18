/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.schemas;

import io.pravega.schemaregistry.common.HashUtil;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import lombok.Data;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.*;

/**
 * In memory groups implementation. 
 */
public class InMemorySchemas implements Schemas<Integer> {
    @GuardedBy("$lock")
    private final Map<Key, Value> schemas = new HashMap<>();

    @Synchronized
    @Override
    public CompletableFuture<Void> addNewSchema(SchemaInfo schemaInfo, String group) {
        long fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        SchemaFingerprintKey fingerprintKey = new SchemaFingerprintKey(fingerprint);
        Value fingerprintValue = schemas.get(fingerprintKey);
        String schemaId = fingerprintValue == null ? null : findSchemaId(schemaInfo, fingerprintValue);

        // add schema and fingerprint
        if (schemaId == null) {
            schemaId = addSchemaAndFingerprint(schemaInfo, fingerprintKey, fingerprintValue);
        }
        // add group reference
        SchemaGroupsKey groupsKey = new SchemaGroupsKey(schemaId);
        Value groupsValue = schemas.get(groupsKey);
        if (groupsValue != null) {
            List<String> list = new ArrayList<>(((SchemaGroupsList) groupsValue.value).getGroupIds());
            list.add(group);
            schemas.put(groupsKey, new Value(new SchemaGroupsList(list), groupsValue.version + 1));
        } else {
            schemas.put(groupsKey, new Value(new SchemaGroupsList(Collections.singletonList(group)), 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    private String addSchemaAndFingerprint(SchemaInfo schemaInfo, SchemaFingerprintKey fingerprintKey, Value fingerprintValue) {
        String schemaId;
        schemaId = UUID.randomUUID().toString();
        SchemaIdKey schemaIdKey = new SchemaIdKey(schemaId);
        schemas.put(schemaIdKey, new Value(new SchemaRecord(schemaInfo), 0));

        if (fingerprintValue == null) {
            schemas.put(fingerprintKey, new Value(new SchemaIdList(Collections.singletonList(schemaId)), 0));
        } else {
            List<String> list = new ArrayList<>(((SchemaIdList) fingerprintValue.getValue()).getSchemaIds());
            list.add(schemaId);
            schemas.put(fingerprintKey, new Value(new SchemaIdList(list), fingerprintValue.version + 1));
        }
        return schemaId;
    }

    @Synchronized
    private String findSchemaId(SchemaInfo schemaInfo, Value fingerprintValue) {
        String schemaId;
        SchemaIdList list = (SchemaIdList) fingerprintValue.getValue();
        schemaId = list.getSchemaIds().stream().filter(x -> {
            SchemaIdKey schemaIdKey = new SchemaIdKey(x);
            SchemaInfo schema = ((SchemaRecord) schemas.get(schemaIdKey).getValue()).getSchemaInfo();
            return schema.getType().equals(schemaInfo.getType())
                    && schema.getSerializationFormat().equals(schemaInfo.getSerializationFormat());
        }).findAny().orElse(null);
        return schemaId;
    }

    @Synchronized
    @Override
    public CompletableFuture<List<String>> getGroupsUsing(SchemaInfo schemaInfo) {
        long fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        SchemaFingerprintKey fingerprintKey = new SchemaFingerprintKey(fingerprint);
        Value fingerprintValue = schemas.get(fingerprintKey);
        String schemaId = fingerprintValue == null ? null : findSchemaId(schemaInfo, fingerprintValue);
        if (schemaId == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            SchemaGroupsKey groupsKey = new SchemaGroupsKey(schemaId);
            SchemaGroupsList groupsValue = (SchemaGroupsList) schemas.get(groupsKey).value;

            return CompletableFuture.completedFuture(groupsValue.getGroupIds());
        }
    }

    @Data
    private static class Value {
        private final SchemaRecords.Value value;
        private final int version;
    }

}
