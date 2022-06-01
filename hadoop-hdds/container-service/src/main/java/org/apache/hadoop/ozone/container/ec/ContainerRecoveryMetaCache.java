/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.ec;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * A cache for chunk-level & block-level metadata,
 * prepared for consolidating a container.
 */
public final class ContainerRecoveryMetaCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerRecoveryMetaCache.class);

  private final ConfigurationSource config;
  private final ContainerLayoutVersion layoutVersion;
  private final String schemaVersion;
  private final ContainerProtos.ContainerDataProto.State state;
  private final long maxContainerSize;

  private final Cache<Long, Map<BlockID, BlockData>> containerBlockDataCache;
  // need to keep track of containers under recovery for cleanup on timeout
  private final Map<Long, KeyValueContainer> containerMap;
  // caller registered invalidator
  private final ContainerInvalidator containerInvalidator;

  public ContainerRecoveryMetaCache(ConfigurationSource conf,
      ContainerInvalidator invalidator) {
    this.config = conf;
    layoutVersion = ContainerLayoutVersion.getConfiguredVersion(conf);
    schemaVersion = VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion();
    state = ContainerProtos.ContainerDataProto.State.CLOSED;
    maxContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    ContainerRecoveryMetaCacheConfig cacheConfig =
        conf.getObject(ContainerRecoveryMetaCacheConfig.class);
    this.containerMap = new ConcurrentHashMap<>();
    this.containerInvalidator = invalidator;
    this.containerBlockDataCache = CacheBuilder.newBuilder()
        .expireAfterAccess(cacheConfig.getCacheExpireThreshold(), MINUTES)
        .maximumSize(cacheConfig.getMaxSize())
        .removalListener((RemovalListener<Long, Map<BlockID, BlockData>>) event
            -> {
          long containerID = event.getKey();
          LOG.debug("BlockData cache for container {} under recovery " +
              "is evicted", containerID);
          if (containerInvalidator != null) {
            containerInvalidator.invalidate(containerMap.get(containerID));
          }
          containerMap.remove(containerID);
        })
        .build();
  }

  void addChunkToBlock(BlockID blockID, ChunkInfo chunkInfo)
      throws IOException {
    long containerID = blockID.getContainerID();

    try {
      Map<BlockID, BlockData> blockDataMap = containerBlockDataCache
          .get(containerID, ConcurrentHashMap::new);

      blockDataMap.compute(blockID, (blk, data) -> {
        if (data == null) {
          data = new BlockData(blockID);
        }
        data.addChunk(chunkInfo.getProtoBufMessage());
        return data;
      });
    } catch (ExecutionException e) {
      throw new IOException("Failed to add chunk " + chunkInfo.getChunkName() +
          " to block " + blockID, e);
    }
  }

  Iterator<BlockData> getBlockIterator(long containerID)
      throws IOException {
    Map<BlockID, BlockData> blockDataMap =
        containerBlockDataCache.getIfPresent(containerID);
    if (blockDataMap == null) {
      throw new IOException("No block metadata found for container " +
          containerID);
    }
    return blockDataMap.values().iterator();
  }

  KeyValueContainer getOrCreateContainer(long containerID,
      String pipelineID, String datanodeID, int replicaIndex) {
    return containerMap.compute(containerID, (id, container) -> {
      if (container == null) {
        KeyValueContainerData containerData =
            new KeyValueContainerData(containerID, layoutVersion,
                maxContainerSize, pipelineID, datanodeID);
        containerData.setSchemaVersion(schemaVersion);
        containerData.setState(state);
        containerData.setReplicaIndex(replicaIndex);
        container = new KeyValueContainer(containerData, config);
      }
      return container;
    });
  }

  KeyValueContainer getContainer(long containerID) {
    return containerMap.get(containerID);
  }

  void dropContainerAll(long containerID) {
    containerBlockDataCache.invalidate(containerID);
    containerMap.remove(containerID);
  }

  /**
   * Caller could register a customized invalidator on cache expire
   * for cleanup.
   */
  interface ContainerInvalidator {

    void invalidate(KeyValueContainer container);
  }

  /**
   * Class to hold configuration for ContainerRecoveryMetaCache.
   */
  @ConfigGroup(prefix = "hdds.datanode")
  public static class ContainerRecoveryMetaCacheConfig {

    @Config(key = "ec.recovery.metacache.expire.threshold",
        type = ConfigType.TIME, timeUnit = MINUTES,
        defaultValue = "30m",
        description = "The timeout for container KV metadata cache to expire.",
        tags = ConfigTag.DATANODE)
    private long cacheExpireThreshold =
        Duration.ofMinutes(30).toMinutes();

    public long getCacheExpireThreshold() {
      return cacheExpireThreshold;
    }

    public void setExpireThreshold(long expireThreshold) {
      cacheExpireThreshold = expireThreshold;
    }

    @Config(key = "ec.recovery.metacache.max.size",
        type = ConfigType.LONG,
        defaultValue = "20",
        description = "The max number of containers to cache.",
        tags = ConfigTag.DATANODE)
    private long maxSize = 20;

    public long getMaxSize() {
      return maxSize;
    }

    public void setMaxSize(long max) {
      maxSize = max;
    }
  }
}
