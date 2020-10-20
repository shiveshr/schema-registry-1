/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.client;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Its implementation abstracts the caller classes from the library
 * used for interacting with pravega tables.
 */
@Slf4j
public class TableStore extends AbstractService {
    private static final String SCHEMA_REGISTRY_SCOPE = "_schemaregistry";
    private static final long RETRY_INIT_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 2;
    private static final long RETRY_MAX_DELAY = Duration.ofSeconds(5).toMillis();
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    private static final KeyValueTableConfiguration CONFIG = KeyValueTableConfiguration.builder().partitionCount(1).build();
    private static final TableEntry<ByteBuffer, ByteBuffer> NOT_EXISTS = TableEntry.notExists(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]));
    private static final String KEY_FAMILY = "keys";
    private final int numOfRetries;
    private final ScheduledExecutorService executor;
    /**
     * Cache where callers can cache values against a table name and a key. 
     */
    private final Cache<TableCacheKey, VersionedRecord<?>> cache;
    /**
     * Cache where callers can cache values against a table name and a key. 
     */
    private final LoadingCache<String, KeyValueTable<ByteBuffer, ByteBuffer>> tableCache;
    private final ControllerImpl controller;
    public TableStore(ClientConfig clientConfig, ScheduledExecutorService executor) {
        this.executor = executor;
        numOfRetries = NUM_OF_RETRIES;
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), executor);
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ConnectionPool connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        KeyValueTableFactoryImpl tableSegmentFactory = new KeyValueTableFactoryImpl(SCHEMA_REGISTRY_SCOPE, controller, connectionPool);
        this.cache = CacheBuilder.newBuilder()
                                 .maximumSize(10000)
                                 .build();
        // loading cache which creates a new kvt client if it doesnt exist.. other wise it returns the kvt client 
        this.tableCache = CacheBuilder.newBuilder()
                                      .maximumSize(1000)
                                      .refreshAfterWrite(10, TimeUnit.MINUTES)
                                      .expireAfterWrite(10, TimeUnit.MINUTES)
                                      .build(
                                              new CacheLoader<String, KeyValueTable<ByteBuffer, ByteBuffer>>() {
                                                  @Override
                                                  @ParametersAreNonnullByDefault
                                                  public KeyValueTable<ByteBuffer, ByteBuffer> load(String name) {
                                                      ByteBufferSerializer serializer = new ByteBufferSerializer();
                                                      KeyValueTableClientConfiguration config = KeyValueTableClientConfiguration
                                                              .builder().retryAttempts(numOfRetries).build();
                                                      return tableSegmentFactory.forKeyValueTable(name, serializer, serializer, config);
                                                  }
                                              });
    }

    @Override
    protected void doStart() {
        createScope()
                .whenComplete((v, e) -> {
                    if (e == null) {
                        notifyStarted();
                    } else {
                        notifyFailed(e);
                    }
                });
    }

    @Override
    protected void doStop() {
        
    }

    private CompletableFuture<Void> createScope() {
        return withRetries(() -> Futures.toVoid(controller.createScope(SCHEMA_REGISTRY_SCOPE)));
    }
    
    public CompletableFuture<Void> createTable(String tableName) {
        log.debug("create table called for table: {}", tableName);

        return Futures.toVoid(controller.createKeyValueTable(SCHEMA_REGISTRY_SCOPE, tableName, CONFIG));
    }

    public CompletableFuture<Void> deleteTable(String tableName) {
        log.debug("delete table called for table: {}", tableName);
        return Futures.toVoid(controller.deleteKeyValueTable(SCHEMA_REGISTRY_SCOPE, tableName));
    }

    public CompletableFuture<Void> addNewEntryIfAbsent(String tableName, byte[] key, @NonNull byte[] value) {
        return Futures.toVoid(getTable(tableName).thenCompose(table -> table.putIfAbsent(KEY_FAMILY, ByteBuffer.wrap(key), ByteBuffer.wrap(value))
                                                                           .exceptionally(e -> {
                          // if we get conditional update exception, it means the key exists and we should ignore it.
                          if (e != null && !(Exceptions.unwrap(e) instanceof ConditionalTableUpdateException)) {
                              throw new CompletionException(StoreExceptions.create(StoreExceptions.Type.UNKNOWN, e, 
                                      String.format("Failed trying to add key to %s", tableName)));
                          } 
                          return null;
                      })));
    }

    private CompletableFuture<KeyValueTable<ByteBuffer, ByteBuffer>> getTable(String tableName) {
        try {
            return CompletableFuture.completedFuture(tableCache.get(tableName));
        } catch (ExecutionException | UncheckedExecutionException e) {
            if (e.getCause() != null && e.getCause() instanceof RuntimeException && e.getCause().getCause() instanceof StatusRuntimeException
                    && ((StatusRuntimeException) e.getCause().getCause()).getStatus().getCode().equals(Status.NOT_FOUND.getCode())) {
                return Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, "table not found."));
            } else {
                return Futures.failedFuture(e);
            }
        } 
    }

    public CompletableFuture<Version> updateEntry(String tableName, byte[] key, byte[] value, Version ver) {
        return updateEntries(tableName, Collections.singletonMap(key, new VersionedRecord<>(value, ver))).thenApply(list -> list.get(0));
    }
    
    public CompletableFuture<List<Version>> updateEntries(String tableName, Map<byte[], VersionedRecord<byte[]>> batch) {
        Preconditions.checkNotNull(batch);
        List<TableEntry<ByteBuffer, ByteBuffer>> entries = batch.entrySet().stream().map(x -> {
            return x.getValue().getVersion() == null ?
                    TableEntry.notExists(ByteBuffer.wrap(x.getKey()), ByteBuffer.wrap(x.getValue().getRecord())) :
                    TableEntry.versioned(ByteBuffer.wrap(x.getKey()), x.getValue().getVersion(), ByteBuffer.wrap(x.getValue().getRecord()));
        }).collect(Collectors.toList());
        
        return getTable(tableName).thenCompose(table -> table.replaceAll(KEY_FAMILY, entries)
                                                            .exceptionally(t -> {
                                      String errorMessage = String.format("update entry on %s failed", tableName);
                                      Throwable cause = Exceptions.unwrap(t);
                                      Throwable toThrow;
                                      if (cause instanceof ConditionalTableUpdateException) {
                                          toThrow = StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, errorMessage);
                                      } else {
                                          log.warn("exception of unknown type thrown {} ", errorMessage, cause);
                                          toThrow = StoreExceptions.create(StoreExceptions.Type.UNKNOWN, cause, errorMessage);
                                      }

                                      throw new CompletionException(toThrow);
                                  }));
    }

    public <T> CompletableFuture<VersionedRecord<T>> getEntry(String tableName, byte[] key, Function<byte[], T> fromBytes) {
        return getEntries(tableName, Collections.singletonList(key), true)
                .thenApply(records -> {
                    VersionedRecord<byte[]> value = records.get(0);
                    return new VersionedRecord<>(fromBytes.apply(value.getRecord()), value.getVersion());
                });
    }

    public CompletableFuture<List<VersionedRecord<byte[]>>> getEntries(String tableName, List<byte[]> tableKeys, boolean throwOnNotFound) {
        log.info("get entries called for : {} key : {}", tableName, tableKeys);
        if (tableKeys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        List<ByteBuffer> keys = tableKeys.stream().map(ByteBuffer::wrap).collect(Collectors.toList());
        return getTable(tableName).thenCompose(table -> table.getAll(KEY_FAMILY, keys)
                                                            .thenApply(list -> list.stream().map(x -> {
                    TableEntry<ByteBuffer, ByteBuffer> entry = x != null ? x : NOT_EXISTS;
                    if (entry.getKey().getVersion().equals(Version.NOT_EXISTS) && throwOnNotFound) {
                            throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "key not found");
                    } else {
                        return new VersionedRecord<>(getArray(entry.getValue()), entry.getKey().getVersion());
                    }
                }).collect(Collectors.toList())));
    }
    
    public CompletableFuture<Void> removeEntry(String tableName, byte[] key) {
        log.trace("remove entry called for : {} key : {}", tableName, key);
        return getTable(tableName).thenCompose(table -> table.remove(KEY_FAMILY, ByteBuffer.wrap(key)));
    }

    public <K> CompletableFuture<List<K>> getAllKeys(String tableName, Function<byte[], K> fromBytesKey) {
        List<K> keys = new LinkedList<>();
        return getTable(tableName)
                .thenCompose(table -> {
                    val iterator = table.keyIterator(KEY_FAMILY, 1000, null);

                    return iterator.collectRemaining(x -> {
                        x.getItems().forEach(y -> {
                            byte[] dst = getArray(y.getKey());
                            keys.add(fromBytesKey.apply(dst));
                        });
                        return !x.getItems().isEmpty();
                    }).thenApply(v -> keys);
                });
    }

    public <K, T> CompletableFuture<List<VersionedEntry<K, T>>> getAllEntries(String tableName,
                                                                              Function<byte[], K> fromBytesKey,
                                                                              Function<byte[], T> fromBytesValue) {
        List<VersionedEntry<K, T>> entries = new LinkedList<>();
        return getTable(tableName).thenCompose(table -> {
            val iterator = table.entryIterator(KEY_FAMILY, 1000, null);

            return iterator.collectRemaining(x -> {
                x.getItems().forEach(y -> {
                    byte[] key = getArray(y.getKey().getKey());
                    byte[] value = getArray(y.getValue());
                    entries.add(new VersionedEntry<>(fromBytesKey.apply(key),
                            new VersionedRecord<>(fromBytesValue.apply(value), y.getKey().getVersion())));
                });
                return !x.getItems().isEmpty();
            }).thenApply(v -> entries);
        });
    }

    /**
     * Api to read cached value for the specified key from the requested table.
     *
     * @param table  name of table.
     * @param key    key to query.
     * @param tClass class of object type to deserialize into.
     * @param <K>    Type of key.
     * @param <T>    Type of object to deserialize the response into.
     * @return Returns a completableFuture which when completed will have the deserialized value with its store key version.
     */
    @SuppressWarnings("unchecked")
    public <K, T> VersionedRecord<T> getCachedRecord(String table, K key, Class<T> tClass) {
        return (VersionedRecord<T>) cache.getIfPresent(new TableCacheKey<>(table, key));
    }

    public <K, T> void cacheRecord(String table, K key, VersionedRecord<T> value) {
        cache.put(new TableCacheKey<>(table, key), value);
    }

    public <K, T> void invalidateCache(String table, K key) {
        cache.invalidate(new TableCacheKey<>(table, key));
    }

    public <K> CompletableFuture<ResultPage<K, ByteBuffer>> getKeysPaginated(String tableName, ByteBuffer continuationToken, int limit,
                                                                          Function<byte[], K> fromByteKey) {
        log.trace("get keys paginated called for : {}", tableName);
        
        IteratorState token = continuationToken == null || continuationToken.remaining() == 0 ? null : 
                IteratorState.fromBytes(continuationToken);
        return getTable(tableName)
                .thenCompose(table -> table.keyIterator(KEY_FAMILY, limit, token).getNext()
                                       .thenApply(x -> x == null ? new ResultPage<>(Collections.emptyList(), ByteBuffer.wrap(new byte[0])) : 
                                               new ResultPage<>(x.getItems().stream().map(y -> {
                                           byte[] key = getArray(y.getKey());
                                           return fromByteKey.apply(key);
                                       }).collect(Collectors.toList()), x.getState().toBytes())));
    }

    private <K, T> CompletableFuture<ResultPage<VersionedEntry<K, T>, ByteBuffer>> getEntriesPaginated(
            String tableName, ByteBuffer continuationToken, int limit, Function<byte[], K> fromBytesKey,
            Function<byte[], T> fromBytesValue) {
        log.trace("get entries paginated called for : {}", tableName);
        IteratorState token = continuationToken == null || continuationToken.remaining() == 0 ? null :
                IteratorState.fromBytes(continuationToken);

        return getTable(tableName)
                .thenCompose(table -> table.entryIterator(KEY_FAMILY, limit, token).getNext()
                                           .thenApply(x -> x == null ? new ResultPage<>(Collections.emptyList(), ByteBuffer.wrap(new byte[0])) : 
                                                   new ResultPage<>(x.getItems().stream().map(y -> {
                                               byte[] key = getArray(y.getKey().getKey());
                                               byte[] value = getArray(y.getValue());
                                               return new VersionedEntry<>(fromBytesKey.apply(key),
                                                       new VersionedRecord<>(fromBytesValue.apply(value), y.getKey().getVersion()));
                                           }).collect(Collectors.toList()), x.getState().toBytes())));
    }

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier) {
        return Retry.withExpBackoff(RETRY_INIT_DELAY, RETRY_MULTIPLIER, numOfRetries, RETRY_MAX_DELAY)
                    .retryWhen(e -> true)
                    .runAsync(futureSupplier, executor);
    }
    
    @Data
    private static class TableCacheKey<K> {
        private final String table;
        private final K key;
    }

    private byte[] getArray(ByteBuffer buf) {
        final byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }
}
