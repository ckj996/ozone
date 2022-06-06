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
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ECContainerDownloader;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.ozone.container.ec.ContainerRecoveryStoreImpl.getChunkName;

/**
 * A Class to help recover EC Containers for a CoordinatorDN.
 */
public class ECContainerRecoverHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECContainerRecoverHelper.class);

  public static final String BLOCK_GROUP_LEN_KEY = "blockGroupLen";

  private final ContainerLayoutVersion layoutVersion;
  private final String schemaVersion;
  private final long maxContainerSize;

  private final ByteBufferPool byteBufferPool;

  // for ListBlock
  private final ECContainerDownloader downloader;
  // store recovered files and metadata
  private final ContainerRecoveryStore recoveryStore;
  // make the recovered container under control
  private final ContainerController containerController;
  private final ConfigurationSource conf;

  public ECContainerRecoverHelper(
      ECContainerDownloader downloader,
      ContainerRecoveryStore recoveryStore,
      ConfigurationSource conf,
      ContainerController containerController,
      ByteBufferPool byteBufferPool) {
    this.conf = conf;
    this.layoutVersion = ContainerLayoutVersion.getConfiguredVersion(conf);
    this.schemaVersion = VersionedDatanodeFeatures.SchemaV2
        .chooseSchemaVersion();
    this.maxContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    this.downloader = downloader;
    this.recoveryStore = recoveryStore;
    this.containerController = containerController;
    this.byteBufferPool = byteBufferPool;
  }

  public void recoverContainerGroup(long containerID,
      List<DatanodeDetails> sourceNodes,
      List<DatanodeDetails> targetNodes,
      List<Integer> sourceIndexes,
      List<Integer> targetIndexes,
      ECReplicationConfig repConfig,
      DatanodeDetails localDn)
      throws IOException {

    List<DatanodeDetails> remoteNodes = new ArrayList<>();
    List<Integer> remoteIndexes = new ArrayList<>();
    for (int i = 0; i < targetNodes.size(); i++) {
      // build grpc clients only for remote target DNs
      if (targetNodes.get(i).equals(localDn)) {
        continue;
      }
      remoteNodes.add(targetNodes.get(i));
      remoteIndexes.add(targetIndexes.get(i));
    }

    downloader.startClients(sourceNodes, sourceIndexes, repConfig);
    if (!remoteIndexes.isEmpty()) {
      downloader.startPushClients(remoteNodes, remoteIndexes, repConfig);
    }

    ContainerRecoveryContext containerContext =
        new ContainerRecoveryContext(containerID, repConfig,
            sourceNodes, sourceIndexes, targetNodes, targetIndexes,
            localDn);

    try {
      // ListBlock
      SortedMap<Long, BlockData[]> blockReplicaMap =
          getAllBlockReplicas(containerContext);

      // Calculate effective blockGroup length
      SortedMap<Long, BlockRecoveryContext> blockRecoveryContextMap =
          buildBlockRecoveryContexts(containerContext, blockReplicaMap);

      // recover blockGroup by blockGroup
      for (BlockRecoveryContext bContext : blockRecoveryContextMap.values()) {
        recoverBlockGroup(containerContext, bContext);
      }

      // rebuild on-disk container and make the container visible globally
      containerController.consolidateContainer(
          ContainerProtos.ContainerType.KeyValueContainer,
          containerID, recoveryStore);

      LOG.info("Recovered container {} replicaIndex {} locally",
          containerID, containerContext.getLocalReplica().getReplicaIndex());

      if (!remoteIndexes.isEmpty()) {
        // rebuild on-disk container on target datanodes.
        downloader.consolidateContainer(containerID);
      }
      LOG.info("Recovered container {} for all missing replicas {}",
          containerID, targetIndexes);
    } catch (IOException e) {
      LOG.error("Failed to recover container {} for all missing replicas {}",
          containerID, targetIndexes, e);
      recoveryStore.cleanupContainerAll(containerID);
    }
  }

  void recoverBlockGroup(ContainerRecoveryContext containerContext,
      BlockRecoveryContext blockContext)
      throws IOException {

    ECReplicationConfig repConfig = containerContext.getRepConfig();
    long remaining = blockContext.getBlockGroupLen();
    long stripeLen = ((long) repConfig.getEcChunkSize()) * repConfig.getData();
    int chunkIndex = 1;

    try {
      while (remaining > 0) {
        stripeLen = Math.min(remaining, stripeLen);

        StripeRecoveryContext stripeContext =
            new StripeRecoveryContext(remaining, stripeLen, chunkIndex);

        recoverStripe(containerContext, blockContext, stripeContext);

        remaining -= stripeLen;
        chunkIndex++;
      }
    } catch (IOException e) {
      LOG.error("Failed to recover block {}", blockContext.getLocalID());
      throw e;
    }
  }

  void recoverStripe(ContainerRecoveryContext containerContext,
      BlockRecoveryContext blockContext,
      StripeRecoveryContext stripeContext)
      throws IOException {

    long containerID = containerContext.getContainerID();
    ECReplicationConfig repConfig = containerContext.getRepConfig();
    Long localID = blockContext.getLocalID();
    int stripeIndex = stripeContext.getChunkIndexInBlock();
    long blockGroupRemaining = stripeContext.getBlockGroupRemaining();
    long stripeLen = stripeContext.getStripeLen();
    boolean isLastStripe = blockGroupRemaining == stripeLen;

    // prepare chunkInfos for both read available chunks
    // and write recovered chunks
    ChunkInfo[] stripeChunkInfos = buildStripeChunkInfos(containerID, localID,
        stripeIndex, repConfig, stripeLen);

    if (isGoodPartialStripe(stripeChunkInfos,
        containerContext.getTargetIndexNodeMap())) {
      LOG.debug("Stripe {} is a good partial stripe, skip recover",
          stripeIndex);
      return;
    }

    // This check is not absolutely accurate, some failed stripes
    // may be recovered as well due to that those failed chunks are
    // included in the missing ones, so not detectable.
    // But it doesn't harm the data consistency from the client view,
    // just a bit wast of storage space.
    if (isLastStripe && isFailedStripe(stripeChunkInfos,
        blockContext.getBlockGroup(), stripeIndex)) {
      LOG.debug("Stripe {} is a failed last stripe, skip recover",
          stripeIndex);
      return;
    }

    ByteBuffer[] stripeAvailChunks = null;
    Map<Integer, ByteBuffer> stripeRecoveredChunks = null;
    try {
      // ReadChunk
      stripeAvailChunks = readStripeAvailChunks(blockContext.getBlockGroup(),
          stripeIndex, containerContext.getSourceIndexNodeMap(),
          stripeChunkInfos, repConfig);

      // pad partial input chunks
      padPartialCells(stripeChunkInfos, stripeAvailChunks);

      // decode the whole stripe
      stripeRecoveredChunks =
          decodeStripe(containerContext, stripeAvailChunks, stripeChunkInfos);

      // unpad partial output chunks
      unpadPartialCells(stripeChunkInfos, stripeRecoveredChunks);

      // write recovered chunks to temp store
      writeRecoveredChunks(containerContext, blockContext,
          stripeRecoveredChunks, stripeChunkInfos,
          isLastStripe);
    } catch (IOException e) {
      LOG.error("Failed to recover stripe {} for block {}",
          stripeIndex, localID);
      throw e;
    } finally {
      // release buffers
      if (stripeAvailChunks != null) {
        releaseBuffers(stripeAvailChunks);
      }
      if (stripeRecoveredChunks != null) {
        releaseBuffers(stripeRecoveredChunks.values()
            .toArray(new ByteBuffer[0]));
      }
    }
  }

  /**
   * Need to list all healthy replicas of each block,
   * then we could determine the effective length of each block group.
   * @param containerContext
   * @return
   */
  SortedMap<Long, BlockData[]> getAllBlockReplicas(
      ContainerRecoveryContext containerContext)
      throws IOException {

    SortedMap<Long, BlockData[]> blockReplicaMap =
        downloader.listBlockGroup(containerContext.getContainerID());

    LOG.debug("List {} blocks in total for container {}",
        blockReplicaMap.size(), containerContext.getContainerID());

    return blockReplicaMap;
  }

  /**
   * For each block, calculate the effective block length
   * and chunk checksum info.
   * @param containerContext
   * @param blockReplicaMap
   * @return
   */
  SortedMap<Long, BlockRecoveryContext> buildBlockRecoveryContexts(
      ContainerRecoveryContext containerContext,
      SortedMap<Long, BlockData[]> blockReplicaMap)
      throws IOException {

    SortedMap<Long, BlockRecoveryContext> blockRecoveryContextMap =
        new TreeMap<>();
    int replicaCount = containerContext.getRepConfig().getRequiredNodes();

    for (Map.Entry<Long, BlockData[]> entry : blockReplicaMap.entrySet()) {
      Long localID = entry.getKey();
      BlockData[] blockReplicas = entry.getValue();

      long blockGroupLen = Long.MAX_VALUE;
      Checksum checksum = null;

      for (int r = 0; r < replicaCount; r++) {
        if (blockReplicas[r] == null) {
          continue;
        }

        String lenStr = blockReplicas[r]
            .getMetadata().get(BLOCK_GROUP_LEN_KEY);
        long len = (lenStr == null) ? Long.MAX_VALUE : Long.parseLong(lenStr);

        // Use the min to be conservative
        blockGroupLen = Math.min(len, blockGroupLen);

        // we need the checksum info from the chunk
        if (checksum == null) {
          ContainerProtos.ChunkInfo firstChunk =
              blockReplicas[r].getChunks().get(0);
          if (firstChunk.hasChecksumData()) {
            checksum = new Checksum(firstChunk.getChecksumData().getType(),
                firstChunk.getChecksumData().getBytesPerChecksum());
          }
        }
      }

      // A bad block with single failed stripe, no need to recover it.
      if (blockGroupLen == Long.MAX_VALUE) {
        LOG.warn("No valid blockGroupLen found for block {}, skip it",
            localID);
        continue;
      }

      LOG.debug("Calculated effective blockGroupLen {} for block {}",
          blockGroupLen, localID);

      blockRecoveryContextMap.put(localID, new BlockRecoveryContext(localID,
          blockGroupLen, checksum, blockReplicas));
    }
    return blockRecoveryContextMap;
  }

  /**
   * Calculate chunk info(off, len) for each chunk according
   * to the block length and block-replica-wide chunk index.
   * @param containerID
   * @param localID
   * @param chunkIndex
   * @param repConfig
   * @param stripeLen
   * @return
   */
  ChunkInfo[] buildStripeChunkInfos(long containerID, Long localID,
      int chunkIndex, ECReplicationConfig repConfig, long stripeLen) {

    BlockID blockID = new BlockID(containerID, localID);
    ChunkInfo[] chunkInfos = new ChunkInfo[repConfig.getRequiredNodes()];
    int chunkSize = repConfig.getEcChunkSize();
    long chunkOffset = (long) (chunkIndex - 1) * chunkSize;
    int stripeLastIndex = (int) (stripeLen + chunkSize - 1) / chunkSize - 1;

    // data chunks
    for (int r = 0; r <= stripeLastIndex; r++) {
      long dataLen = chunkSize;

      if (r == stripeLastIndex && stripeLen % chunkSize > 0) {
        dataLen = stripeLen % chunkSize;
      }
      chunkInfos[r] = new ChunkInfo(getChunkName(blockID,
          chunkIndex), chunkOffset, dataLen);
    }

    // parity chunks
    long parityLen = stripeLen < chunkSize ? stripeLen : chunkSize;
    for (int r = repConfig.getData(); r < repConfig.getRequiredNodes(); r++) {
      chunkInfos[r] = new ChunkInfo(getChunkName(blockID,
          chunkIndex), chunkOffset, parityLen);
    }
    return chunkInfos;
  }

  /**
   * A partial stripe and no chunk with data is missing,
   * so don't need to decode stripe.
   * @param stripeChunkInfos
   * @param targetIndexNodeMap
   * @return
   */
  boolean isGoodPartialStripe(ChunkInfo[] stripeChunkInfos,
      Map<Integer, DatanodeDetails> targetIndexNodeMap) {
    return targetIndexNodeMap.keySet().stream()
        .allMatch(index -> stripeChunkInfos[index - 1] == null);
  }

  /**
   * If the retrieved stripe chunk list doesn't match that
   * calculated with the effective blockGroupLen, then
   * the stripe is a failed stripe.
   * @param stripeChunkInfos
   * @param blockGroup
   * @param stripeIndex
   * @return
   */
  boolean isFailedStripe(ChunkInfo[] stripeChunkInfos,
      BlockData[] blockGroup, int stripeIndex) {
    int replicaCount = stripeChunkInfos.length;

    for (int r = 0; r < replicaCount; r++) {
      // skip expected empty chunk for partial stripe
      if (stripeChunkInfos[r] == null) {
        continue;
      }
      // skip missing indexes
      if (blockGroup[r] == null) {
        continue;
      }

      if (stripeIndex - 1 >= blockGroup[r].getChunks().size()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Read enough number(=dataBlocks) of available chunks.
   * @param blockGroup
   * @param stripeIndex
   * @return
   */
  ByteBuffer[] readStripeAvailChunks(BlockData[] blockGroup, int stripeIndex,
      Map<Integer, DatanodeDetails> sourceIndexNodeMap, ChunkInfo[] chunkInfos,
      ECReplicationConfig repConfig)
      throws IOException {

    ByteBuffer[] remoteChunks = downloader.readStripe(blockGroup, stripeIndex);
    int replicaCount = repConfig.getRequiredNodes();
    ByteBuffer[] availChunks = new ByteBuffer[replicaCount];
    int expectedCount = repConfig.getData();
    int availCount = 0;
    int redundentStart = replicaCount;

    for (int r = 0; r < replicaCount; r++) {
      if (sourceIndexNodeMap.containsKey(r + 1)) {
        availChunks[r] =
            byteBufferPool.getBuffer(false, repConfig.getEcChunkSize());
        // this node has no actual chunk, must be in a partial stripe
        if (chunkInfos[r] == null) {
          availChunks[r].limit(0);
          availCount++;
        } else if (remoteChunks[r] != null) {
          availChunks[r].put(remoteChunks[r]);
          availChunks[r].flip();
          availChunks[r].limit((int) chunkInfos[r].getLen());
          availCount++;
        }
      }

      if (availCount == expectedCount) {
        redundentStart = r + 1;
        break;
      }
    }

    if (availCount < expectedCount) {
      throw new IOException("Not enough chunks to decode stripe expected: "
          + expectedCount + " avail: " + availCount);
    }

    // ensure that the number of availChunks == expectedCount
    for (int r = redundentStart; r < replicaCount; r++) {
      availChunks[r] = null;
    }

    return availChunks;
  }

  /**
   * Pad potential partial chunk buffers to be aligned with
   * the first chunk length.
   * @param stripeChunkInfos
   * @param availChunks
   */
  void padPartialCells(ChunkInfo[] stripeChunkInfos,
      ByteBuffer[] availChunks) {
    int cellSize = (int) stripeChunkInfos[0].getLen();

    for (ByteBuffer buf : availChunks) {
      if (buf != null && buf.limit() < cellSize) {
        buf.position(buf.limit());
        buf.limit(cellSize);
        Arrays.fill(buf.array(), buf.position(), buf.limit(), (byte) 0);
        buf.position(buf.limit());
        buf.flip();
      }
    }
  }

  /**
   * Unpad the decoded partial chunks to the actual length.
   * @param stripeChunkInfos
   * @param recoveredChunks
   */
  void unpadPartialCells(ChunkInfo[] stripeChunkInfos,
      Map<Integer, ByteBuffer> recoveredChunks) {
    recoveredChunks.forEach((index, buf) -> {
      if (stripeChunkInfos[index - 1] != null) {
        buf.limit((int) stripeChunkInfos[index - 1].getLen());
      }
    });
  }

  /**
   * Decode missing chunks from avail chunks.
   * @param containerContext
   * @param availChunks
   * @param chunkInfos
   * @return
   * @throws IOException
   */
  Map<Integer, ByteBuffer> decodeStripe(
      ContainerRecoveryContext containerContext,
      ByteBuffer[] availChunks,
      ChunkInfo[] chunkInfos)
      throws IOException {

    int cellSize = (int) chunkInfos[0].getLen();
    ECReplicationConfig repConfig = containerContext.getRepConfig();
    RawErasureDecoder decoder = containerContext.getDecoder();
    // index to recover from the container context
    int[] missingIndexes = containerContext.getTargetIndexNodeMap()
        .keySet().stream()
        .mapToInt(Integer::valueOf).map(i -> i - 1).toArray();

    // prepare available chunks
    ByteBuffer[] decoderInputBuffers =
        new ByteBuffer[repConfig.getRequiredNodes()];
    for (int r = 0; r < repConfig.getRequiredNodes(); r++) {
      if (availChunks[r] != null) {
        decoderInputBuffers[r] = availChunks[r];
      }
    }

    // prepare buffers to hold recovered chunks
    ByteBuffer[] decoderOutputBuffers = new ByteBuffer[missingIndexes.length];
    for (int i = 0; i < missingIndexes.length; i++) {
      decoderOutputBuffers[i] =
          byteBufferPool.getBuffer(false, repConfig.getEcChunkSize());
      decoderOutputBuffers[i].limit(cellSize);
    }

    decoder.decode(decoderInputBuffers, missingIndexes, decoderOutputBuffers);

    Map<Integer, ByteBuffer> recoveredChunkMap = new HashMap<>();
    for (int i = 0; i < missingIndexes.length; i++) {
      recoveredChunkMap.put(missingIndexes[i] + 1, decoderOutputBuffers[i]);
    }
    return recoveredChunkMap;
  }

  /**
   * Write recovered chunks locally then we could rebuild a container
   * with them.
   * @param containerContext
   * @param blockContext
   * @param recoveredChunks
   * @param chunkInfos
   * @param last
   * @throws IOException
   */
  void writeRecoveredChunks(ContainerRecoveryContext containerContext,
      BlockRecoveryContext blockContext,
      Map<Integer, ByteBuffer> recoveredChunks,
      ChunkInfo[] chunkInfos, boolean last)
      throws IOException {

    BlockID blockID = new BlockID(containerContext.getContainerID(),
        blockContext.getLocalID());
    KeyValueContainerData localReplica = containerContext.getLocalReplica();

    List<CompletableFuture<Long>> futures = new ArrayList<>();

    for (Map.Entry<Integer, ByteBuffer> entry : recoveredChunks.entrySet()) {
      int replicaIndex = entry.getKey();
      ChunkBuffer data = ChunkBuffer.wrap(recoveredChunks.get(replicaIndex));

      if (chunkInfos[replicaIndex - 1] != null) {
        // compute checksums for each chunk before write or transfer
        computeChecksumForChunk(chunkInfos[replicaIndex - 1],
            blockContext.getChecksum(), data);

        // write local chunks to disk
        if (replicaIndex == localReplica.getReplicaIndex()) {
          recoveryStore.writeChunk(localReplica.getContainerID(),
              localReplica.getReplicaIndex(),
              blockID, chunkInfos[replicaIndex - 1], data, last);
        } else {
          futures.add(downloader.writeChunkAsync(
              containerContext.getContainerID(), replicaIndex,
              blockID, chunkInfos[replicaIndex - 1], data, last));
        }
      }
    }
    for (CompletableFuture<Long> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for WriteChunkAsync");
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.error("WriteChunkAsync failed", e);
        throw new IOException(e);
      }
    }
  }

  /**
   * Release decode input and output buffers.
   * @param buffers
   */
  void releaseBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buf : buffers) {
      if (buf != null) {
        byteBufferPool.putBuffer(buf);
      }
    }
  }

  void computeChecksumForChunk(ChunkInfo chunkInfo,
      Checksum checksum, ChunkBuffer data)
      throws IOException {
    try {
      chunkInfo.setChecksumData(checksum.computeChecksum(data));
      data.rewind();
    } catch (OzoneChecksumException e) {
      LOG.error("Failed to checksum chunk {}", chunkInfo.getChunkName());
      throw e;
    }
  }

  /**
   * Container level recovery infos.
   */
  private class ContainerRecoveryContext {
    private final long containerID;
    private final ECReplicationConfig repConfig;
    private final KeyValueContainerData localReplica;
    private final Map<Integer, DatanodeDetails> sourceIndexNodeMap;
    private final Map<Integer, DatanodeDetails> targetIndexNodeMap;
    private final RawErasureDecoder decoder;

    ContainerRecoveryContext(long containerID,
        ECReplicationConfig repConfig,
        List<DatanodeDetails> sourceNodes,
        List<Integer> sourceIndexes,
        List<DatanodeDetails> targetNodes,
        List<Integer> targetIndexes,
        DatanodeDetails localDn) {
      this.containerID = containerID;
      this.repConfig = repConfig;
      this.sourceIndexNodeMap = new HashMap<>();
      this.targetIndexNodeMap = new HashMap<>();

      for (int i = 0; i < sourceIndexes.size(); i++) {
        sourceIndexNodeMap.put(sourceIndexes.get(i), sourceNodes.get(i));
      }

      int localIndex = 0;
      for (int i = 0; i < targetIndexes.size(); i++) {
        targetIndexNodeMap.put(targetIndexes.get(i), targetNodes.get(i));
        if (targetNodes.get(i).equals(localDn)) {
          localIndex = targetIndexes.get(i);
        }
      }

      this.localReplica = new KeyValueContainerData(
          containerID, layoutVersion, maxContainerSize,
          "", localDn.getUuidString());
      localReplica.setSchemaVersion(schemaVersion);
      localReplica.setReplicaIndex(localIndex);
      localReplica.setState(State.CLOSED);

      this.decoder = CodecUtil.createRawDecoderWithFallback(repConfig);
    }

    public long getContainerID() {
      return containerID;
    }

    public ECReplicationConfig getRepConfig() {
      return repConfig;
    }

    public KeyValueContainerData getLocalReplica() {
      return localReplica;
    }

    public Map<Integer, DatanodeDetails> getSourceIndexNodeMap() {
      return sourceIndexNodeMap;
    }

    public Map<Integer, DatanodeDetails> getTargetIndexNodeMap() {
      return targetIndexNodeMap;
    }

    public RawErasureDecoder getDecoder() {
      return decoder;
    }
  }

  /**
   * Block level recovery infos.
   */
  private static class BlockRecoveryContext {
    private final Long localID;
    private final long blockGroupLen;
    private final Checksum checksum;
    private BlockData[] blockGroup;

    BlockRecoveryContext(Long localID, long blockGroupLen,
        Checksum checksum, BlockData[] blockGroup) {
      this.localID = localID;
      this.blockGroupLen = blockGroupLen;
      this.checksum = checksum;
      this.blockGroup = blockGroup;
    }

    Long getLocalID() {
      return localID;
    }

    long getBlockGroupLen() {
      return blockGroupLen;
    }

    Checksum getChecksum() {
      return checksum;
    }

    BlockData[] getBlockGroup() {
      return blockGroup;
    }
  }

  /**
   * Stripe level recovery infos.
   */
  private static class StripeRecoveryContext {
    private final long blockGroupRemaining;
    private final long stripeLen;
    private final int chunkIndexInBlock;

    StripeRecoveryContext(long remaining, long stripeLen, int chunkIndex) {
      this.blockGroupRemaining = remaining;
      this.stripeLen = stripeLen;
      this.chunkIndexInBlock = chunkIndex;
    }

    public long getBlockGroupRemaining() {
      return blockGroupRemaining;
    }

    public long getStripeLen() {
      return stripeLen;
    }

    public int getChunkIndexInBlock() {
      return chunkIndexInBlock;
    }
  }

}
