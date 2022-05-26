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
package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ozone.container.ec.ContainerRecoveryStore;
import org.apache.hadoop.ozone.container.ec.ECContainerRecoverHelper;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ECContainerDownloader;
import org.apache.hadoop.ozone.container.replication.ECContainerDownloaderImpl;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is the actual EC reconstruction coordination task.
 */
public class ECReconstructionCoordinatorTask implements Runnable {

  static final Logger LOG =
      LoggerFactory.getLogger(ECReconstructionCoordinatorTask.class);

  private final ConfigurationSource conf;
  private final ContainerRecoveryStore tempStore;
  private final ContainerController controller;
  private final ByteBufferPool bufferPool;
  private final CertificateClient certClient;
  private final ECReconstructionCommandInfo commandInfo;

  public ECReconstructionCoordinatorTask(
      ECReconstructionCommandInfo reconstructionCommandInfo,
      ConfigurationSource conf,
      ContainerRecoveryStore tempStore,
      ContainerController controller,
      ByteBufferPool bufferPool,
      CertificateClient certClient) {

    this.commandInfo = reconstructionCommandInfo;
    this.conf = conf;
    this.tempStore = tempStore;
    this.controller = controller;
    this.bufferPool = bufferPool;
    this.certClient = certClient;
  }

  @Override
  public void run() {
    // Implement the coordinator logic to handle a container group
    // reconstruction.
    LOG.info("Running EC reconstruction task {}", this);

    ECContainerDownloader downloader =
        new ECContainerDownloaderImpl(conf, certClient);
    ECContainerRecoverHelper helper = new ECContainerRecoverHelper(
        downloader, tempStore, conf, controller, bufferPool);
    List<DatanodeDetails> sourceDNs = commandInfo.getSources()
        .stream().map(DatanodeDetailsAndReplicaIndex::getDnDetails)
        .collect(Collectors.toList());
    List<Integer> sourceIndexes = commandInfo.getSources()
        .stream().map(DatanodeDetailsAndReplicaIndex::getReplicaIndex)
        .collect(Collectors.toList());
    List<Integer> targetIndexes = new ArrayList<>();
    for (byte i : commandInfo.getMissingContainerIndexes()) {
      targetIndexes.add((int) i);
    }

    try {
      helper.recoverContainerGroup(commandInfo.getContainerID(),
          sourceDNs, commandInfo.getTargetDatanodes(),
          sourceIndexes, targetIndexes,
          commandInfo.getEcReplicationConfig(),
          commandInfo.getCoordinatorDatanode());
    } catch (Exception e) {
      LOG.error("Run {} failed", this, e);
    } finally {
      try {
        downloader.close();
      } catch (IOException e) {
        LOG.warn("Failed to close ECContainerDownloader", e);
      }
    }
  }

  @Override
  public String toString() {
    return "ECReconstructionCoordinatorTask{" + "reconstructionCommandInfo="
        + commandInfo + '}';
  }
}
