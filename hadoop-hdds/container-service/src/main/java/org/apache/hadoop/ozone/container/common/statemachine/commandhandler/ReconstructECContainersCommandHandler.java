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
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.ec.ContainerRecoveryStore;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCommandInfo;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionSupervisor;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Command handler for reconstructing the lost EC containers.
 */
public class ReconstructECContainersCommandHandler implements CommandHandler {

  private final CertificateClient certClient;
  private final ECReconstructionSupervisor supervisor;
  private final ConfigurationSource conf;
  private final MutableVolumeSet volumeSet;
  private final ContainerRecoveryStore tempStore;
  private final ContainerController controller;
  private final ByteBufferPool bufferPool;

  public ReconstructECContainersCommandHandler(ConfigurationSource conf,
      ECReconstructionSupervisor supervisor, MutableVolumeSet volumeSet,
      ContainerRecoveryStore tempStore,
      ContainerController controller, CertificateClient certClient) {
    this.conf = conf;
    this.supervisor = supervisor;
    this.volumeSet = volumeSet;
    this.tempStore = tempStore;
    this.controller = controller;
    this.certClient = certClient;
    this.bufferPool = new ElasticByteBufferPool();
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    ReconstructECContainersCommand ecContainersCommand =
        (ReconstructECContainersCommand) command;
    DatanodeDetails thisDN = ecContainersCommand.getTargetDatanodes().stream()
        .filter(dn -> dn.getUuidString().equals(volumeSet.getDatanodeUuid()))
        .findAny().orElse(null);
    Preconditions.checkArgument(thisDN != null,
        "CoordinatorDN must be one of the TargetDN");
    ECReconstructionCommandInfo reconstructionCommandInfo =
        new ECReconstructionCommandInfo(ecContainersCommand.getContainerID(),
            ecContainersCommand.getEcReplicationConfig(),
            ecContainersCommand.getMissingContainerIndexes(),
            ecContainersCommand.getSources(),
            ecContainersCommand.getTargetDatanodes(),
            thisDN);
    this.supervisor.addTask(
        new ECReconstructionCoordinatorTask(reconstructionCommandInfo, conf,
            tempStore, controller, bufferPool, certClient));
  }

  @Override
  public Type getCommandType() {
    return Type.reconstructECContainersCommand;
  }

  @Override
  public int getInvocationCount() {
    return 0;
  }

  @Override
  public long getAverageRunTime() {
    return 0;
  }

  public ConfigurationSource getConf() {
    return conf;
  }
}
