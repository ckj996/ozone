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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerFactory;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ECContainerDownloader;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.container.ec.ContainerRecoveryStoreImpl.getChunkName;
import static org.apache.hadoop.ozone.container.ec.ECContainerRecoverHelper.BLOCK_GROUP_LEN_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

/**
 * Test for ECContainerRecoverHelper.
 */
@RunWith(Parameterized.class)
public class TestECContainerRecoverHelper {
  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private static final long CONTAINER_ID = 1L;
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final int NUM_VOLUMES = 1;
  private static final int NUM_BLOCKS_PER_CONTAINER = 3;
  private static final int NUM_STRIPES_PER_BLOCK = 3;
  private static final int LAST_STRIPE_INDEX = NUM_STRIPES_PER_BLOCK - 1;
  private static final ByteBufferPool BYTE_BUFFER_POOL =
      new ElasticByteBufferPool();

  private OzoneConfiguration conf;
  private MutableVolumeSet hddsVolumeSet;
  private ContainerRecoveryStoreImpl store;
  private ContainerSet containerSet;
  private KeyValueHandler handler;
  private ContainerController containerController;
  private DataTransferThrottler throttler;
  private BlockManager blockManager;
  private ChunkManager chunkManager;
  private DispatcherContext dispatcherContext;

  private ECReplicationConfig repConfig;
  private int dataCount;
  private int parityCount;
  private int nodeCount;
  private int chunkSize;
  private List<DatanodeDetails> datanodes;
  private RawErasureEncoder encoder;
  private List<Integer> replicaIndexes;

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {new ECReplicationConfig("rs-3-2-1024")},
        {new ECReplicationConfig("rs-6-3-1024")},
        {new ECReplicationConfig("rs-10-4-1024")},
    });
  }

  public TestECContainerRecoverHelper(ECReplicationConfig repConfig)
      throws IOException {
    tempDir.create();
    this.conf = new OzoneConfiguration();
    this.repConfig = repConfig;
    this.dataCount = repConfig.getData();
    this.parityCount = repConfig.getParity();
    this.chunkSize = repConfig.getEcChunkSize();
    this.nodeCount = dataCount + parityCount;
    this.datanodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    this.replicaIndexes = new ArrayList<>();
    for (int i = 1; i <= nodeCount; i++) {
      replicaIndexes.add(i);
    }
    this.encoder = CodecUtil.createRawEncoderWithFallback(repConfig);

    this.throttler = mock(DataTransferThrottler.class);
    doNothing().when(throttler).throttle(anyLong(), any());
    this.blockManager = new BlockManagerImpl(conf);
    this.chunkManager =
        ChunkManagerFactory.createChunkManager(conf, blockManager, null);
    this.dispatcherContext = new DispatcherContext.Builder().build();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
  }

  @Test
  public void testAllFullStripe()
      throws IOException {
    // e.g. EC 3+2 |---|---|---|---|---|
    int[] lastStripeLens = new int[dataCount];
    for (int i = 0; i < dataCount; i++) {
      lastStripeLens[i] = chunkSize;
    }
    try (MockContainerGroup containerGroup =
             new MockContainerGroup(lastStripeLens)) {
      testRecoverContainerGroup(containerGroup);
    }
  }

  @Test
  public void testPartialStripeWithSinglePartialChunk()
      throws IOException {
    // e.g. EC 3+2 |-|         |-|-|
    int[] lastStripeLens = {chunkSize - 1};
    try (MockContainerGroup containerGroup =
             new MockContainerGroup(lastStripeLens)) {
      testRecoverContainerGroup(containerGroup);
    }
  }

  @Test
  public void testPartialStripeSingleFullChunk()
      throws IOException {
    // e.g. EC 3+2 |---|       |---|---|
    int[] lastStripeLens = {chunkSize};
    try (MockContainerGroup containerGroup =
             new MockContainerGroup(lastStripeLens)) {
      testRecoverContainerGroup(containerGroup);
    }
  }

  @Test
  public void testPartialStripeFullAndPartialChunkBoth()
      throws IOException {
    // e.g. EC 3+2 |---|-|     |---|---|
    int[] lastStripeLens = {chunkSize, chunkSize - 1};
    try (MockContainerGroup containerGroup =
             new MockContainerGroup(lastStripeLens)) {
      testRecoverContainerGroup(containerGroup);
    }
  }

  @Test
  public void testPartialStripeMultipleFullAndPartialChunk()
      throws IOException {
    // e.g. EC 3+2 |---|---|-|  |---|---|
    int[] lastStripeLens = {chunkSize, chunkSize, chunkSize - 1};
    try (MockContainerGroup containerGroup =
             new MockContainerGroup(lastStripeLens)) {
      testRecoverContainerGroup(containerGroup);
    }
  }

  public void testRecoverContainerGroup(MockContainerGroup containerGroup)
      throws IOException {
    for (int badCount = 1; badCount <= parityCount; badCount++) {
      for (int badStart = 0; badStart <= nodeCount - badCount; badStart++) {
        List<Integer> targetIndexes =
            replicaIndexes.subList(badStart, badStart + badCount);
        List<DatanodeDetails> targetNodes =
            datanodes.subList(badStart, badStart + badCount);
        List<Integer> sourceIndexes = replicaIndexes.stream()
            .filter(i -> !targetIndexes.contains(i))
            .collect(Collectors.toList());
        List<DatanodeDetails> sourceNodes = datanodes.stream()
            .filter(dn -> !targetNodes.contains(dn))
            .collect(Collectors.toList());

        for (int i = 0; i < targetNodes.size(); i++) {
          DatanodeDetails localDn = targetNodes.get(i);
          ECContainerRecoverHelper helper =
              createRecoverHelper(localDn, containerGroup);

          helper.recoverContainerGroup(CONTAINER_ID, sourceNodes, targetNodes,
              sourceIndexes, targetIndexes, repConfig, localDn);

          Container container = containerController.getContainer(CONTAINER_ID);
          assertTrue(container.scanMetaData());
          assertTrue(container.scanData(throttler, null));

          assertTrue(containerGroup.verifyContainerReplica(container,
              targetIndexes.get(i)));
          container.delete();
        }
      }
    }
  }

  private ECContainerRecoverHelper createRecoverHelper(DatanodeDetails dn,
      MockContainerGroup containerGroup)
      throws IOException {
    StringBuilder datanodeDirs = new StringBuilder();
    File[] volumeDirs = new File[NUM_VOLUMES];
    for (int i = 0; i < NUM_VOLUMES; i++) {
      volumeDirs[i] = tempDir.newFolder();
      datanodeDirs.append(volumeDirs[i]).append(",");
    }
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, datanodeDirs.toString());
    hddsVolumeSet = new MutableVolumeSet(dn.getUuidString(), CLUSTER_ID,
        conf, null, StorageVolume.VolumeType.DATA_VOLUME, null);
    store = new ContainerRecoveryStoreImpl(hddsVolumeSet, conf);
    containerSet = new ContainerSet();
    handler = new KeyValueHandler(conf, dn.getUuidString(),
        containerSet, hddsVolumeSet, null, c -> { });
    containerController = new ContainerController(containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer,
            handler));
    return new ECContainerRecoverHelper(containerGroup.getDownloader(),
        store, conf, containerController, BYTE_BUFFER_POOL);
  }

  private byte[] genRandomData(int size) {
    return RandomStringUtils.randomAscii(size).getBytes(UTF_8);
  }

  private class MockContainerGroup implements Closeable {
    private ECContainerDownloader downloader;
    private Map<Long, List<BlockData>> blockReplicas;
    private Map<Long, List<ByteBuffer[]>> blockStripeDatas;

    MockContainerGroup(int[] lastStripeChunkLens) throws IOException {
      this.blockReplicas = new TreeMap<>();
      this.blockStripeDatas = new TreeMap<>();

      // init the slots
      for (long b = 0; b < NUM_BLOCKS_PER_CONTAINER; b++) {
        blockReplicas.put(b, new ArrayList<>());
        for (int r = 0; r < nodeCount; r++) {
          blockReplicas.get(b)
              .add(new BlockData(new BlockID(CONTAINER_ID, b)));
        }

        blockStripeDatas.put(b, new ArrayList<>());
        for (int r = 0; r < nodeCount; r++) {
          blockStripeDatas.get(b)
              .add(new ByteBuffer[NUM_STRIPES_PER_BLOCK]);
        }
      }

      // fill the data and meta
      for (long b = 0; b < NUM_BLOCKS_PER_CONTAINER; b++) {
        BlockID blockID = new BlockID(CONTAINER_ID, b);
        long offset = 0L;
        long blockGroupLen = 0L;

        // full stripes
        for (int s = 0; s < NUM_STRIPES_PER_BLOCK; s++) {
          // create data chunks
          ByteBuffer[] dataBuffers = new ByteBuffer[dataCount];
          for (int r = 0; r < dataCount; r++) {
            dataBuffers[r] = BYTE_BUFFER_POOL.getBuffer(false, chunkSize);
            if (s == LAST_STRIPE_INDEX) {
              // last stripe with single chunk
              if (lastStripeChunkLens.length == 1) {
                dataBuffers[r].limit(lastStripeChunkLens[0]);
              } else {
                dataBuffers[r].limit(chunkSize);
              }

              if (r < lastStripeChunkLens.length) {
                dataBuffers[r].put(genRandomData(lastStripeChunkLens[r]));
                blockGroupLen += lastStripeChunkLens[r];
              }
              // pad
              Arrays.fill(dataBuffers[r].array(), dataBuffers[r].position(),
                  dataBuffers[r].limit(), (byte) 0);
            } else {
              dataBuffers[r].limit(chunkSize);
              dataBuffers[r].put(genRandomData(chunkSize));
              blockGroupLen += chunkSize;
            }
            dataBuffers[r].position(dataBuffers[r].limit());
            dataBuffers[r].flip();
          }
          // generate parity chunks
          ByteBuffer[] parityBuffers = new ByteBuffer[parityCount];
          for (int r = 0; r < parityCount; r++) {
            parityBuffers[r] =
                BYTE_BUFFER_POOL.getBuffer(false, chunkSize);
            parityBuffers[r].limit(dataBuffers[0].limit());
          }

          encoder.encode(dataBuffers, parityBuffers);

          for (int r = 0; r < dataCount; r++) {
            dataBuffers[r].flip();
            if (s == LAST_STRIPE_INDEX) {
              if (r < lastStripeChunkLens.length) {
                dataBuffers[r].limit(lastStripeChunkLens[r]);
              } else {
                dataBuffers[r].limit(0);
              }
            }
          }

          ByteBuffer[] stripeBuffers = new ByteBuffer[nodeCount];
          ChunkInfo[] chunkInfos = new ChunkInfo[nodeCount];

          for (int r = 0; r < nodeCount; r++) {
            if (r < dataCount) {
              stripeBuffers[r] = dataBuffers[r];
            } else {
              stripeBuffers[r] = parityBuffers[r - dataCount];
            }
            chunkInfos[r] = new ChunkInfo(getChunkName(blockID, s + 1),
                offset, stripeBuffers[r].limit());

            blockReplicas.get(b).get(r).addChunk(
                chunkInfos[r].getProtoBufMessage());
            blockStripeDatas.get(b).get(r)[s] = stripeBuffers[r];
          }

          offset += chunkSize;
        }

        // record block group length
        for (BlockData blockData : blockReplicas.get(b)) {
          blockData.addMetadata(BLOCK_GROUP_LEN_KEY,
              Long.toString(blockGroupLen));
        }
      }

      // init the block and chunk sources
      this.downloader = new MockECContainerDownloader(
          blockReplicas, blockStripeDatas);
    }

    boolean verifyContainerReplica(Container container, int replicaIndex)
        throws IOException {
      List<BlockData> blockDataList =
          blockManager.listBlock(container, 0, NUM_BLOCKS_PER_CONTAINER);
      assertEquals(NUM_BLOCKS_PER_CONTAINER, blockDataList.size());

      for (BlockData blockData : blockDataList) {
        BlockID blockID = blockData.getBlockID();
        int chunkIndex = 0;

        for (ContainerProtos.ChunkInfo chunkInfo : blockData.getChunks()) {
          ChunkBuffer decodedeData = chunkManager.readChunk(container, blockID,
              ChunkInfo.getFromProtoBuf(chunkInfo), dispatcherContext);
          ByteBuffer originalData = blockStripeDatas.get(blockID.getLocalID())
              .get(replicaIndex - 1)[chunkIndex];

          assertArrayEquals(
              Arrays.copyOf(originalData.array(), (int) chunkInfo.getLen()),
              decodedeData.toByteString().toByteArray());
          chunkIndex++;
        }
      }

      return true;
    }

    ECContainerDownloader getDownloader() {
      return downloader;
    }

    @Override
    public void close() throws IOException {
      for (Map.Entry<Long, List<ByteBuffer[]>> entry
          : blockStripeDatas.entrySet()) {
        List<ByteBuffer[]> blockBuffers = entry.getValue();
        blockBuffers.forEach(arr -> {
          for (int i = 0; i < NUM_STRIPES_PER_BLOCK; i++) {
            BYTE_BUFFER_POOL.putBuffer(arr[i]);
          }
        });
      }
    }
  }

  private static class MockECContainerDownloader
      implements ECContainerDownloader {

    private Map<Integer, DatanodeDetails> sourceNodeMap;
    private Map<Long, List<BlockData>> blockReplicas;
    private Map<Long, List<ByteBuffer[]>> blockStripeDatas;

    MockECContainerDownloader(
        Map<Long, List<BlockData>> blockReplicas,
        Map<Long, List<ByteBuffer[]>> blockStripeDatas) {
      this.blockReplicas = blockReplicas;
      this.blockStripeDatas = blockStripeDatas;
      this.sourceNodeMap = new TreeMap<>();
    }

    @Override
    public void startClients(List<DatanodeDetails> sourceNodes,
        List<Integer> sourceIndexes, ECReplicationConfig repConfig) {
      for (int i = 0; i < sourceNodes.size(); i++) {
        sourceNodeMap.put(sourceIndexes.get(i), sourceNodes.get(i));
      }
    }

    @Override
    public void startPushClients(List<DatanodeDetails> targetNodes,
        List<Integer> sourceIndexes, ECReplicationConfig repConfig) {
      // nothing
    }

    @Override
    public SortedMap<Long, BlockData[]> listBlockGroup(long containerID) {
      SortedMap<Long, BlockData[]> blockGroup = new TreeMap<>();

      for (Map.Entry<Long, List<BlockData>> entry : blockReplicas.entrySet()) {
        long localID = entry.getKey();
        List<BlockData> blockDataList = entry.getValue();
        int nodeCount = blockDataList.size();

        blockGroup.put(localID, blockDataList.toArray(new BlockData[0]));
        for (int i = 0; i < nodeCount; i++) {
          if (!sourceNodeMap.containsKey(i + 1)) {
            blockGroup.get(localID)[i] = null;
          }
        }
      }
      return blockGroup;
    }

    @Override
    public ByteBuffer[] readStripe(BlockData[] blockGroup, int stripeIndex) {
      long localID = 0;
      for (BlockData blockData : blockGroup) {
        if (blockData != null) {
          localID = blockData.getLocalID();
          break;
        }
      }

      int nodeCount = blockStripeDatas.get(localID).size();
      ByteBuffer[] stripe = new ByteBuffer[nodeCount];

      for (int i = 0; i < nodeCount; i++) {
        if (sourceNodeMap.containsKey(i + 1)) {
          ByteBuffer sourceBuffer =
              blockStripeDatas.get(localID).get(i)[stripeIndex - 1];
          stripe[i] =
              BYTE_BUFFER_POOL.getBuffer(false, sourceBuffer.capacity());
          sourceBuffer.rewind();
          stripe[i].put(sourceBuffer);
          sourceBuffer.rewind();
          stripe[i].flip();
        }
      }
      return stripe;
    }

    @Override
    public CompletableFuture<Long> writeChunkAsync(long containerID,
        int replicaIndex, BlockID blockID, ChunkInfo chunkInfo,
        ChunkBuffer data, boolean last) throws IOException {
      return CompletableFuture.completedFuture(0L);
    }

    @Override
    public void consolidateContainer(long containerID) throws IOException {
      // nothing
    }

    @Override
    public void close() throws IOException {
      // nothing
    }
  }
}
