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
package org.apache.hadoop.ozone.container.replication;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;

/**
 * Service to download container data from other datanodes.
 * <p>
 * The implementation of this interface should copy the raw container data in
 * compressed form to working directory.
 * <p>
 * A smart implementation would use multiple sources to do parallel download.
 */
public interface ECContainerDownloader extends Closeable {

  void startClients(List<DatanodeDetails> sourceNodes,
      List<Integer> sourceIndexes, ECReplicationConfig repConfig);

  void startPushClients(List<DatanodeDetails> targetNodes,
      List<Integer> targetIndexes, ECReplicationConfig repConfig);

  SortedMap<Long, BlockData[]> listBlockGroup(long containerID);

  ByteBuffer[] readStripe(BlockData[] blockGroup, int stripeIndex);

  void writeChunk(long containerID, int replicaIndex,
      BlockID blockID, ChunkInfo chunkInfo, ChunkBuffer data,
      boolean last) throws IOException;

  void consolidateContainer(long containerID) throws IOException;
}
