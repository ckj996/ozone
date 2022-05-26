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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.ozone.container.ec.ContainerRecoveryStoreImpl.getChunkName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Test for {@link ContainerRecoveryMetaCache}.
 */
public class TestContainerRecoveryMetaCache {

  private OzoneConfiguration conf;

  @Before
  public void setup() {
    this.conf = new OzoneConfiguration();
  }

  @Test
  public void testBasicOperations() throws Exception {
    ContainerRecoveryMetaCache metaCache =
        new ContainerRecoveryMetaCache(conf, null);

    // 3 concurrent adds
    int numContainers = 3;
    List<Thread> threads = IntStream.range(0, numContainers)
        .mapToObj(t -> new Thread(() -> {
          // write 5 blocks, 5 chunks each
          for (int i = 0; i < 5; i++) {
            BlockID block = new BlockID(t, i);
            long offset = 0L;
            long len = 1024L;
            for (int j = 0; j < 5; j++) {
              ChunkInfo chunk = new ChunkInfo(
                  getChunkName(block, j), offset, len);
              offset += len;
              try {
                metaCache.addChunkToBlock(block, chunk);
              } catch (IOException e) {
                fail("IOException when addChunkToBlock" + e);
              }
            }
          }
        }))
        .collect(Collectors.toList());

    threads.forEach(Thread::start);
    for (Thread thread : threads) {
      thread.join();
    }

    // check if all chunks are added correctly
    for (int t = 0; t < numContainers; t++) {
      // check through all the chunks in each block
      Iterator<BlockData> iter = metaCache.getBlockIterator(t);
      int blockCount = 0;

      while (iter.hasNext()) {
        BlockData blockData = iter.next();
        assertEquals(5, blockData.getChunks().size());
        blockCount++;
      }
      assertEquals(5, blockCount);
    }

    metaCache.dropContainerAll(0);
    // check all stuff of this container is gone
    try {
      metaCache.getBlockIterator(0);
      fail("Should throw IOException if no block data for this container");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }

  @Test
  public void testDefaultConfig() {
    ContainerRecoveryMetaCache.ContainerRecoveryMetaCacheConfig cacheConfig =
        conf.getObject(ContainerRecoveryMetaCache
            .ContainerRecoveryMetaCacheConfig.class);
    assertEquals(30, cacheConfig.getCacheExpireThreshold());
    assertEquals(20, cacheConfig.getMaxSize());
  }

  @Test
  public void testCacheInvalidationOnRemoval() {
    ContainerRecoveryMetaCache.ContainerInvalidator mockInvalidator =
        mock(ContainerRecoveryMetaCache.ContainerInvalidator.class);
    doNothing().when(mockInvalidator).invalidate(any());
    ContainerRecoveryMetaCache metaCache =
        new ContainerRecoveryMetaCache(conf, mockInvalidator);

    // populate cache
    for (int i = 0; i < 2; i++) {
      BlockID block = new BlockID(0, i);
      long offset = 0L;
      long len = 1024L;
      for (int j = 0; j < 2; j++) {
        ChunkInfo chunk = new ChunkInfo(
            getChunkName(block, j), offset, len);
        offset += len;
        try {
          metaCache.addChunkToBlock(block, chunk);
        } catch (IOException e) {
          fail("IOException when addChunkToBlock" + e);
        }
      }
    }
    // call removal operation
    metaCache.dropContainerAll(0);
    // invalidator should be called
    verify(mockInvalidator, timeout(1000).atLeastOnce()).invalidate(any());

    // check all stuff of this container is gone
    try {
      metaCache.getBlockIterator(0);
      fail("Should throw IOException if no block data for this container");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }
}
