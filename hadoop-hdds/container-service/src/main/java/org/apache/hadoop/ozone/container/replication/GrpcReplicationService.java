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

import java.io.IOException;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkDataMessage;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ConsolidateMessage;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PushChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PushChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc;

import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.ec.ContainerRecoveryStore;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to make containers available for replication.
 */
public class GrpcReplicationService extends
    IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceImplBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcReplicationService.class);

  private static final int BUFFER_SIZE = 1024 * 1024;

  private final ContainerReplicationSource source;

  private final HddsDispatcher dispatcher;
  private final ContainerRecoveryStore tempStore;
  private final ContainerController controller;

  public GrpcReplicationService(ContainerReplicationSource source,
      HddsDispatcher dispatcher, ContainerRecoveryStore tempStore,
      ContainerController controller) {
    this.source = source;
    this.dispatcher = dispatcher;
    this.tempStore = tempStore;
    this.controller = controller;
  }

  @Override
  public void download(CopyContainerRequestProto request,
      StreamObserver<CopyContainerResponseProto> responseObserver) {
    long containerID = request.getContainerID();
    LOG.info("Streaming container data ({}) to other datanode", containerID);
    try {
      GrpcOutputStream outputStream =
          new GrpcOutputStream(responseObserver, containerID, BUFFER_SIZE);
      source.copyData(containerID, outputStream);
    } catch (IOException e) {
      LOG.error("Error streaming container {}", containerID, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void send(ContainerCommandRequestProto request,
      StreamObserver<ContainerCommandResponseProto> observer) {

    observer.onNext(dispatcher.dispatch(request, null));
    observer.onCompleted();
  }

  @Override
  public void push(PushChunkRequestProto request,
      StreamObserver<PushChunkResponseProto> observer) {

    switch (request.getMessage()) {
    case CHUNK_DATA:
      pushChunk(request.getChunkData(), observer);
      break;
    case CONSOLIDATE:
      consolidate(request.getConsolidate(), observer);
      break;
    default:
      // nothing
    }
    observer.onCompleted();
  }

  private void pushChunk(ChunkDataMessage request,
      StreamObserver<PushChunkResponseProto> observer) {
    final long containerID = request.getBlockID().getContainerID();
    final String chunkName = request.getChunkData().getChunkName();
    final BlockID blockID = BlockID.getFromProtobuf(request.getBlockID());
    final int replicaIndex = request.getReplicaIndex();
    final boolean last = request.hasLast() && request.getLast();

    LOG.debug("Received chunk {} of container {}", chunkName, containerID);

    final ChunkBuffer data =
        ChunkBuffer.wrap(request.getData().asReadOnlyByteBuffer());
    try {
      final ChunkInfo chunkInfo =
          ChunkInfo.getFromProtoBuf(request.getChunkData());
      tempStore.writeChunk(containerID, replicaIndex,
          blockID, chunkInfo, data, last);
      observer.onNext(PushChunkResponseProto.newBuilder().build());
    } catch (Exception e) {
      LOG.error("Error when writing chunk {} to tempStore", chunkName, e);
      observer.onError(e);
    }
  }

  private void consolidate(ConsolidateMessage request,
      StreamObserver<PushChunkResponseProto> observer) {
    final long containerID = request.getContainerID();
    LOG.info("Starting to reconstruct container {}", containerID);
    try {
      controller.consolidateContainer(
          ContainerProtos.ContainerType.KeyValueContainer,
          containerID, tempStore);
      observer.onNext(PushChunkResponseProto.newBuilder().build());
    } catch (IOException e) {
      LOG.error("Error when consolidating container {}", containerID, e);
      observer.onError(e);
    }
  }

}
