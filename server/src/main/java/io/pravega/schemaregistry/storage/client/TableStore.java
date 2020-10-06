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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.NoSuchKeyException;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collection;
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
    public static final String SCHEMA_REGISTRY_SCOPE = "_schemaregistry";
    private final static long RETRY_INIT_DELAY = 100;
    private final static int RETRY_MULTIPLIER = 2;
    private final static long RETRY_MAX_DELAY = Duration.ofSeconds(5).toMillis();
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    public static final KeyValueTableConfiguration CONFIG = KeyValueTableConfiguration.builder().partitionCount(1).build();
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
    private final KeyValueTableManager kvtManager;
    private final ControllerImpl controller;
    private final KeyValueTableFactoryImpl tableSegmentFactory;
    public TableStore(ClientConfig clientConfig, ScheduledExecutorService executor) {
        this.executor = executor;
        numOfRetries = NUM_OF_RETRIES;
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), executor);
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ConnectionPool connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        this.tableSegmentFactory = new KeyValueTableFactoryImpl(SCHEMA_REGISTRY_SCOPE, controller, connectionPool);
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

        kvtManager = KeyValueTableManager.create(clientConfig);
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

        return CompletableFuture.runAsync(() -> kvtManager.createKeyValueTable(SCHEMA_REGISTRY_SCOPE, tableName, CONFIG), executor);
    }

    public CompletableFuture<Void> deleteTable(String tableName) {
        log.debug("delete table called for table: {}", tableName);
        return CompletableFuture.runAsync(() -> kvtManager.deleteKeyValueTable(SCHEMA_REGISTRY_SCOPE, tableName), executor);
    }

    public CompletableFuture<Void> addNewEntryIfAbsent(String tableName, byte[] key, @NonNull byte[] value) {
        // get table from tablecache
        return Futures.toVoid(getTable(tableName).putIfAbsent("", ByteBuffer.wrap(key), ByteBuffer.wrap(value)));
    }

    @SneakyThrows(ExecutionException.class)
    private KeyValueTable<ByteBuffer, ByteBuffer> getTable(String tableName) {
        return tableCache.get(tableName);
    }

    public CompletableFuture<Version> updateEntry(String tableName, byte[] key, byte[] value, Version ver) {
        getTable(tableName).replace("", ByteBuffer.wrap(key), ByteBuffer.wrap(value), ver);

        return updateEntries(tableName, Collections.singletonMap(key, new VersionedRecord<>(value, ver))).thenApply(list -> list.get(0));
    }
    
    public CompletableFuture<List<Version>> updateEntries(String tableName, Map<byte[], VersionedRecord<byte[]>> batch) {
        Preconditions.checkNotNull(batch);
        List<TableEntry<ByteBuffer, ByteBuffer>> entries = batch.entrySet().stream().map(x -> {
            return x.getValue().getVersion() == null ?
                    TableEntry.notExists(ByteBuffer.wrap(x.getKey()), ByteBuffer.wrap(x.getValue().getRecord())) :
                    TableEntry.versioned(ByteBuffer.wrap(x.getKey()), x.getValue().getVersion(), ByteBuffer.wrap(x.getValue().getRecord()));
        }).collect(Collectors.toList());
        
        return getTable(tableName).replaceAll("", entries);
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
        
        List<ByteBuffer> keys = tableKeys.stream().map(ByteBuffer::wrap).collect(Collectors.toList());
        return getTable(tableName).getAll("", keys)
                .thenApply(list -> list.stream().map(x -> {
                    if (x.getKey().getVersion().equals(Version.NOT_EXISTS) && throwOnNotFound) {
                            throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "key not found");
                    } else {
                        return new VersionedRecord<>(getArray(x.getValue()), x.getKey().getVersion());
                    }
                }).collect(Collectors.toList()));
    }
    
    public CompletableFuture<Void> removeEntry(String tableName, byte[] key) {
        log.trace("remove entry called for : {} key : {}", tableName, key);
        List<TableSegmentKey> keys = Collections.singletonList(TableSegmentKey.unversioned(key));
        return getTable(tableName).remove("", ByteBuffer.wrap(key));
    }

    public <K> CompletableFuture<List<K>> getAllKeys(String tableName, Function<byte[], K> fromBytesKey) {
        List<K> keys = new LinkedList<>();
        AsyncIterator<IteratorItem<TableKey<ByteBuffer>>> iterator = getTable(tableName).keyIterator("", 1000, null);

        return iterator.collectRemaining(x -> {
            x.getItems().forEach(y -> {
                byte[] dst = getArray(y.getKey());
                keys.add(fromBytesKey.apply(dst));
            });
            return !x.getItems().isEmpty();
        }).thenApply(v -> keys);
    }

    public <K, T> CompletableFuture<List<VersionedEntry<K, T>>> getAllEntries(String tableName,
                                                                              Function<byte[], K> fromBytesKey,
                                                                              Function<byte[], T> fromBytesValue) {
        List<VersionedEntry<K, T>> entries = new LinkedList<>();
        AsyncIterator<IteratorItem<TableEntry<ByteBuffer, ByteBuffer>>> iterator = getTable(tableName)
                .entryIterator("", 1000, null);
        
        return iterator.collectRemaining(x -> {
            x.getItems().forEach(y -> {
                byte[] key = getArray(y.getKey().getKey());
                byte[] value = getArray(y.getValue());
                entries.add(new VersionedEntry<>(fromBytesKey.apply(key), 
                        new VersionedRecord<>(fromBytesValue.apply(value), y.getKey().getVersion())));
            });
            return !x.getItems().isEmpty();
        }).thenApply(v -> entries);
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

        return getTable(tableName).keyIterator("", limit, IteratorState.fromBytes(continuationToken)).getNext()
                .thenApply(x -> new ResultPage<>(x.getItems().stream().map(y -> {
                    byte[] key = getArray(y.getKey());
                    return fromByteKey.apply(key);
                }).collect(Collectors.toList()), x.getState().toBytes()));
    }

    private <K, T> CompletableFuture<ResultPage<VersionedEntry<K, T>, ByteBuffer>> getEntriesPaginated(
            String tableName, ByteBuffer continuationToken, int limit, Function<byte[], K> fromBytesKey,
            Function<byte[], T> fromBytesValue) {
        log.trace("get entries paginated called for : {}", tableName);
        return getTable(tableName).entryIterator("", limit, IteratorState.fromBytes(continuationToken)).getNext()
                                  .thenApply(x -> new ResultPage<>(x.getItems().stream().map(y -> {
                                      byte[] key = getArray(y.getKey().getKey());
                                      byte[] value = getArray(y.getValue());
                                      return new VersionedEntry<>(fromBytesKey.apply(key),
                                              new VersionedRecord<>(fromBytesValue.apply(value), y.getKey().getVersion()));
                                  }).collect(Collectors.toList()), x.getState().toBytes()));
    }

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier) {
        return Retry.withExpBackoff(RETRY_INIT_DELAY, RETRY_MULTIPLIER, numOfRetries, RETRY_MAX_DELAY)
                    .retryWhen(e -> true)
                    .runAsync(futureSupplier, executor);
    }

    private <T> Supplier<CompletableFuture<T>> exceptionalCallback(Supplier<CompletableFuture<T>> future, Supplier<String> errorMessageSupplier,
                                                                   String tableName, boolean throwOriginalOnCfe) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
            String errorMessage = errorMessageSupplier.get();
            Throwable cause = Exceptions.unwrap(t);
            Throwable toThrow;
            if (cause instanceof ConditionalTableUpdateException || cause instanceof BadKeyVersionException) {
                toThrow = StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, errorMessage);
            } else if (cause instanceof NoSuchKeyException) {
                toThrow = StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, errorMessage);
            } else if (cause instanceof NoSuchKeyException) {
                toThrow = StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, errorMessage);
                    case SegmentDoesNotExist:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableKeyDoesNotExist:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableKeyBadVersion:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, wcfe, errorMessage);
                        break;
                    default:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.UNKNOWN, wcfe, errorMessage);
                }
            } else if (cause instanceof ControllerFailureException) {
                log.warn("Host Store exception {}", cause.getMessage());
                toThrow = StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, cause, errorMessage);
            } else {
                log.warn("exception of unknown type thrown {} ", errorMessage, cause);
                toThrow = StoreExceptions.create(StoreExceptions.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
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
