/*
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

package org.apache.hadoop.ozone.container.replication;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simple ContainerDownloaderImplementation to download the missing container
 * from the first available datanode.
 * <p>
 * This is not the most effective implementation as it uses only one source
 * for the container download.
 */
public class ECContainerDownloaderImpl implements ECContainerDownloader {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECContainerDownloaderImpl.class);
  private static final int LIST_BATCH = 10000;

  private final Path workingDirectory;
  private final SecurityConfig securityConfig;
  private final CertificateClient certClient;
  private List<DatanodeDetails> sourceDNs;
  private List<DatanodeDetails> targetDNs;
  private List<GrpcReplicationClient> clients;
  private List<GrpcReplicationClient> pushClients;

  public ECContainerDownloaderImpl(
      ConfigurationSource conf, CertificateClient certClient) {

    String workDirString =
        conf.get(OzoneConfigKeys.OZONE_CONTAINER_COPY_WORKDIR);

    if (workDirString == null) {
      workingDirectory = Paths.get(System.getProperty("java.io.tmpdir"))
          .resolve("container-copy");
    } else {
      workingDirectory = Paths.get(workDirString);
    }
    securityConfig = new SecurityConfig(conf);
    this.certClient = certClient;
  }

  @Override
  public void startClients(List<DatanodeDetails> sourceDatanodes,
      List<Integer> sourceIndexes, ECReplicationConfig repConfig) {
    startClients(sortDatanode(sourceDatanodes, sourceIndexes,
        repConfig.getRequiredNodes()));
  }

  @Override
  public void startPushClients(List<DatanodeDetails> targetDatanodes,
      List<Integer> targetIndexes, ECReplicationConfig repConfig) {
    startPushClients(sortDatanode(targetDatanodes, targetIndexes,
        repConfig.getRequiredNodes()));
  }

  private List<DatanodeDetails> sortDatanode(List<DatanodeDetails> datanodes,
      List<Integer> replicaIndexes, int numReplicas) {
    Preconditions.checkArgument(replicaIndexes != null);
    Preconditions.checkArgument(datanodes != null);
    Preconditions.checkArgument(!replicaIndexes.isEmpty());
    Preconditions.checkArgument(datanodes.size() == replicaIndexes.size());
    DatanodeDetails[] sortedDataNodes = new DatanodeDetails[numReplicas];
    for (int i = 0; i < replicaIndexes.size(); i++) {
      sortedDataNodes[replicaIndexes.get(i) - 1] = datanodes.get(i);
    }
    return Arrays.asList(sortedDataNodes);
  }

  public void startClients(List<DatanodeDetails> sourceDatanodes) {
    if (clients != null) {
      LOG.warn("Restarting Grpc clients");
      stopClients();
    }
    sourceDNs = sourceDatanodes;
    clients = new ArrayList<>(sourceDNs.size());
    for (DatanodeDetails dn : sourceDNs) {
      clients.add(connectTo(dn));
    }
  }

  public void startPushClients(List<DatanodeDetails> targetDataNodes) {
    if (pushClients != null) {
      LOG.warn("Restarting Grpc push clients");
      stopPushClients();
    }
    targetDNs = targetDataNodes;
    pushClients = new ArrayList<>(targetDNs.size());
    for (DatanodeDetails dn : targetDNs) {
      pushClients.add(connectTo(dn));
    }
  }

  private GrpcReplicationClient connectTo(DatanodeDetails dn) {
    if (dn == null) {
      return null;
    }
    try {
      return new GrpcReplicationClient(dn.getIpAddress(),
          dn.getPort(Name.REPLICATION).getValue(),
          workingDirectory, securityConfig, certClient);
    } catch (Exception e) {
      LOG.warn("Failed to connect to DN {}: {}", dn.getHostName(), e);
    }
    return null;
  }

  public void stopClients() {
    closeClients(clients);
    clients = null;
  }

  public void stopPushClients() {
    closeClients(pushClients);
    pushClients = null;
  }

  private void closeClients(List<GrpcReplicationClient> grpcClients) {
    if (grpcClients == null) {
      return;
    }
    grpcClients.stream()
        .filter(Objects::nonNull)
        .forEach(GrpcReplicationClient::close);
  }

  @Override
  public SortedMap<Long, BlockData[]> listBlockGroup(long containerID) {

    final SortedMap<Long, BlockData[]> map = new TreeMap<>();

    for (int i = 0; i < sourceDNs.size(); i++) {
      if (clients.get(i) == null) {
        continue;
      }
      try {
        List<BlockData> blocks = listBlock(clients.get(i), sourceDNs.get(i),
            containerID, LIST_BATCH).get();
        for (BlockData block : blocks) {
          final long id = block.getBlockID().getLocalID();
          if (!map.containsKey(id)) {
            map.put(id, new BlockData[sourceDNs.size()]);
          }
          map.get(id)[i] = block;
        }
      } catch (ExecutionException e) {
        LOG.warn("Failed to list blocks from DN {} replicaIndex {}: {}",
            sourceDNs.get(i).getHostName(), i + 1, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return map;
  }

  private static CompletableFuture<List<BlockData>> listBlock(
      GrpcReplicationClient client, DatanodeDetails datanode,
      long containerId, int batchSize) {

    return listBlockFrom(-1, batchSize, start -> client
        .listBlock(containerId, datanode.getUuidString(), start, batchSize))
        .thenApply(s -> s.collect(Collectors.toList()));
  }

  private static CompletableFuture<Stream<BlockData>> listBlockFrom(
      long startLocalID, int batchSize,
      Function<Long, CompletableFuture<List<BlockData>>> listFrom) {

    return listFrom.apply(startLocalID).thenCompose(head -> {
      if (head == null) {
        return CompletableFuture.completedFuture(Stream.empty());
      }
      if (head.size() < batchSize) {
        return CompletableFuture.completedFuture(head.stream());
      }
      long lastID = head.get(head.size() - 1).getBlockID().getLocalID();
      return listBlockFrom(lastID + 1, batchSize, listFrom)
          .thenApply(tail -> Stream.concat(head.stream(), tail));
    });
  }

  @Override
  public ByteBuffer[] readStripe(BlockData[] blockGroup, int stripeIndex) {

    // read DNs in parallel
    CompletableFuture<ByteBuffer>[] futures =
        new CompletableFuture[sourceDNs.size()];
    for (int i = 0; i < sourceDNs.size(); i++) {
      if (clients.get(i) == null || blockGroup[i] == null ||
          stripeIndex > blockGroup[i].getChunks().size()) {
        continue;
      }
      futures[i] = readChunk(clients.get(i), sourceDNs.get(i), blockGroup[i],
          blockGroup[i].getChunks().get(stripeIndex - 1));
    }

    // join at stripe level
    ByteBuffer[] stripe = new ByteBuffer[sourceDNs.size()];
    for (int i = 0; i < sourceDNs.size(); i++) {
      if (futures[i] == null) {
        continue;
      }
      try {
        stripe[i] = futures[i].get();
      } catch (ExecutionException e) {
        LOG.warn("Failed to read chunk {} from DN {} replicaIndex {}: {}",
            stripeIndex, sourceDNs.get(i).getHostName(), i + 1, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return stripe;
  }

  @Override
  public CompletableFuture<Long> writeChunkAsync(long containerID,
      int replicaIndex, BlockID blockID, ChunkInfo chunkInfo,
      ChunkBuffer data, boolean last) throws IOException {

    final GrpcReplicationClient client = pushClients.get(replicaIndex - 1);
    if (client == null) {
      throw new IOException("TargetDN " + replicaIndex + " not connected");
    }

    return client.pushChunk(replicaIndex, blockID.getDatanodeBlockIDProtobuf(),
        chunkInfo.getProtoBufMessage(), data, last);
  }

  @Override
  public void consolidateContainer(long containerID) throws IOException {
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < pushClients.size(); i++) {
      if (pushClients.get(i) == null) {
        continue;
      }
      LOG.info("Sending ConsolidateContainer to targetDN {} replicaIndex {}",
          targetDNs.get(i).getHostName(), i + 1);
      futures.add(pushClients.get(i).consolidateContainer(containerID));
    }

    try {
      for (CompletableFuture<Long> future : futures) {
        future.get();
      }
    } catch (InterruptedException e) {
      LOG.warn("interrupted");
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.error("ConsolidateContainer failed", e);
      throw new IOException(e);
    }
  }

  private static CompletableFuture<ByteBuffer> readChunk(
      GrpcReplicationClient client, DatanodeDetails datanode,
      BlockData block, ContainerProtos.ChunkInfo chunk) {

    return client.readChunk(block.getBlockID().getContainerID(),
        datanode.getUuidString(), block.getBlockID(), chunk);
  }

  @Override
  public void close() {
    stopClients();
    stopPushClients();
  }
}
