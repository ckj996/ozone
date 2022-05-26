/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Tests for {@link GrpcReplicationClient} and {@link GrpcReplicationService}.
 */
public class TestECContainerDownloader {

  private static MiniOzoneCluster cluster;
  private static OzoneManager om;
  private static StorageContainerManager scm;
  private static OzoneClient client;
  private static ObjectStore store;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final int EC_DATA = 10;
  private static final int EC_PARITY = 4;
  private static final EcCodec EC_CODEC = EcCodec.RS;
  private static final int EC_CHUNK_SIZE = 1024;
  private static final int NUM_DN = EC_DATA + EC_PARITY;
  private static final int NUM_KEYS = 3;
  private static final int KEY_SIZE =
      EC_CHUNK_SIZE * EC_DATA + EC_CHUNK_SIZE * 3 / 2;
  private static byte[] value;
  private static long containerID;
  private static List<DatanodeDetails> datanodeDetails;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(conf);
    prepareData(NUM_KEYS);
  }

  @AfterAll
  public static void stop() throws IOException {
    stopCluster();
  }

  @Test
  public void testListBlockGroupAndReadStripe() {
    ECContainerDownloaderImpl downloader =
        new ECContainerDownloaderImpl(cluster.getConf(), cluster.getCAClient());
    downloader.startClients(datanodeDetails);

    Collection<BlockData[]> blockGroups =
        downloader.listBlockGroup(containerID).values();

    Assertions.assertEquals(NUM_KEYS, blockGroups.size());
    /* for debug
    for (int i = 0; i < blockGroups.size(); i++) {
      System.out.print("block group " + i + ":");
      for (int j = 0; j < blockGroups.get(i).length; j++) {
        System.out.print(" [" + j + ", " +
            Objects.nonNull(blockGroups.get(i)[j]) + "]");
      }
      System.out.println();
    }
    */

    for (BlockData[] blockGroup : blockGroups) {
      final int stripeCnt = Arrays.stream(blockGroup).filter(Objects::nonNull)
          .mapToInt(b -> b.getChunks().size()).max().orElse(0);
      ByteBuffer buffer = ByteBuffer.allocate(value.length);
      int remaining = KEY_SIZE;
      for (int stripeIdx = 1; stripeIdx <= stripeCnt; stripeIdx++) {
        ByteBuffer[] stripe = downloader.readStripe(blockGroup, stripeIdx);
        final int parityCellSize = Math.min(EC_CHUNK_SIZE, remaining);

        /* for debug
        System.out.print("stripe index " + stripeIdx + ":");
        for (int i = 0; i < stripe.length; i++) {
          final int cellSize = stripe[i] == null ? 0 : stripe[i].limit();
          System.out.print(" [" + i + ", " + cellSize + "]");
        }
        System.out.println();
        */

        for (int i = 0; i < EC_DATA && remaining > 0; i++) {
          final int expectedSize = Math.min(EC_CHUNK_SIZE, remaining);
          Assertions.assertNotNull(stripe[i]);
          Assertions.assertEquals(expectedSize, stripe[i].limit());
          buffer.put(stripe[i]);
          remaining -= stripe[i].limit();
        }

        for (int i = EC_DATA; i < EC_DATA + EC_PARITY; i++) {
          Assertions.assertNotNull(stripe[i]);
          Assertions.assertEquals(parityCellSize, stripe[i].limit());
        }
      }
      Assertions.assertArrayEquals(value, buffer.array());
    }
    downloader.close();
  }

  public static void startCluster(OzoneConfiguration conf) throws Exception {
    // Reduce long wait time in MiniOzoneClusterImpl#waitForHddsDatanodesStop
    //  for testZReadKeyWithUnhealthyContainerReplica.
    conf.set("ozone.scm.stale.node.interval", "10s");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DN)
        .setECPipelineMinimum(1)
        .setScmId(SCM_ID)
        .setClusterId(CLUSTER_ID)
        .build();
    cluster.waitForClusterToBeReady();
    om = cluster.getOzoneManager();
    scm = cluster.getStorageContainerManager();
    client = OzoneClientFactory.getRpcClient(conf);
    store = client.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
  }

  public static void prepareData(int numKeys) throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    final ReplicationConfig repConfig =
        new ECReplicationConfig(EC_DATA, EC_PARITY, EC_CODEC, EC_CHUNK_SIZE);
    value = RandomUtils.nextBytes(KEY_SIZE);
    for (int i = 0; i < numKeys; i++) {
      final String keyName = UUID.randomUUID().toString();
      try (OutputStream out = bucket.createKey(
          keyName, value.length, repConfig, new HashMap<>())) {
        out.write(value);
      }
    }
    List<ContainerID> containerIDs =
        new ArrayList<>(scm.getContainerManager().getContainerIDs());
    Assertions.assertEquals(1, containerIDs.size());
    containerID = containerIDs.get(0).getId();
    List<Pipeline> pipelines = scm.getPipelineManager().getPipelines(repConfig);
    Assertions.assertEquals(1, pipelines.size());
    datanodeDetails = pipelines.get(0).getNodes();
  }

  public static void stopCluster() throws IOException {
    if (client != null) {
      client.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
