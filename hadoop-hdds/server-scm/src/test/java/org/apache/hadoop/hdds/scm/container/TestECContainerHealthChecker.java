/**
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

package org.apache.hadoop.hdds.scm.container;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.replication.ECContainerHealthChecker;
import org.apache.hadoop.hdds.scm.container.replication.InflightAction;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMDBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reconstructECContainersCommand;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getECContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getEcReplicas;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;

/**
 * Test cases to verify the functionality of EcContainerHealtheChecker.
 */
public class TestECContainerHealthChecker {

  private ReplicationManager replicationManager;
  private ContainerStateManager containerStateManager;
  private PlacementPolicy containerPlacementPolicy;
  private EventQueue eventQueue;
  private TestReplicationManager.DatanodeCommandHandler datanodeCommandHandler;
  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private SCMServiceManager serviceManager;
  private TestClock clock;
  private File testDir;
  private DBStore dbStore;
  private PipelineManager pipelineManager;
  private SCMHAManager scmhaManager;
  private GenericTestUtils.LogCapturer scmLogs;

  @Before
  public void setup()
      throws IOException, InterruptedException,
      NodeNotFoundException, InvalidStateTransitionException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);

    scmLogs = GenericTestUtils.LogCapturer.
        captureLogs(ECContainerHealthChecker.LOG);
    containerManager = Mockito.mock(ContainerManager.class);
    nodeManager = new SimpleMockNodeManager();
    eventQueue = new EventQueue();
    scmhaManager = SCMHAManagerStub.getInstance(true);
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    pipelineManager = Mockito.mock(PipelineManager.class);
    Mockito.when(pipelineManager
            .containsPipeline(Mockito.any(PipelineID.class))).thenReturn(true);
    containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
    serviceManager = new SCMServiceManager();

    datanodeCommandHandler =
        new TestReplicationManager.DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, datanodeCommandHandler);

    Mockito.when(containerManager.getContainers())
        .thenAnswer(invocation -> {
          Set<ContainerID> ids = containerStateManager.getContainerIDs();
          List<ContainerInfo> containers = new ArrayList<>();
          for (ContainerID id : ids) {
            containers.add(containerStateManager.getContainer(
                id));
          }
          return containers;
        });

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0])));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0])));

    containerPlacementPolicy = Mockito.mock(PlacementPolicy.class);

    Mockito.when(containerPlacementPolicy.chooseDatanodes(
        Mockito.any(), Mockito.any(), Mockito.anyInt(),
            Mockito.anyLong(), Mockito.anyLong()))
        .thenAnswer(invocation -> {
          int count = (int) invocation.getArguments()[2];
          return IntStream.range(0, count)
              .mapToObj(i -> randomDatanodeDetails())
              .collect(Collectors.toList());
        });

    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.any(),
        Mockito.anyInt()
        )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(2, 2, 3));
    clock = new TestClock(Instant.now(), ZoneId.of("UTC"));
    createReplicationManager(new ReplicationManagerConfiguration());
  }

  private void createReplicationManager(ReplicationManagerConfiguration rmConf)
      throws InterruptedException, IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    testDir = GenericTestUtils
      .getTestDir(TestContainerManagerImpl.class.getSimpleName());
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    config.setTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);
    config.setFromObject(rmConf);

    SCMHAManager scmHAManager = SCMHAManagerStub
        .getInstance(true, new SCMDBTransactionBufferImpl());
    dbStore = DBStoreBuilder.createDBStore(
      config, new SCMDBDefinition());

    replicationManager = new ReplicationManager(
        config,
        containerManager,
        containerPlacementPolicy,
        eventQueue,
        SCMContext.emptyContext(),
        serviceManager,
        nodeManager,
        clock,
        scmHAManager,
        SCMDBDefinition.MOVE.getTable(dbStore));

    replicationManager.getEcContainerHealthChecker()
        .setContainerPlacement(containerPlacementPolicy);

    serviceManager.notifyStatusChanged();
    scmLogs.clearOutput();
    Thread.sleep(100L);
  }

  @After
  public void teardown() throws Exception {
    containerStateManager.close();
    replicationManager.stop();
    if (dbStore != null) {
      dbStore.close();
    }
    FileUtils.deleteDirectory(testDir);
  }

  /**
   * Open containers are not handled by ECContainerHealthchecker.
   * This test-case makes sure that ECcontainerHealthchecker doesn't take
   * any action on OPEN containers.
   */
  @Test
  public void testOpenEcContainer() throws IOException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(
        LifeCycleState.OPEN, PipelineID.randomId(), repConfig);
    containerStateManager.addContainer(container.getProtobuf());
    replicationManager.processAll();
    eventQueue.processAll(1000);
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.OPEN));
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());
  }

  /**
   * If the EC container is in CLOSING state we resend close container command
   * to all the datanodes.
   */
  @Test
  public void testClosingEcContainer() throws IOException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(
        LifeCycleState.CLOSING, PipelineID.randomId(), repConfig);
    final ContainerID id = container.containerID();

    containerStateManager.addContainer(container.getProtobuf());

    // Two replicas in CLOSING state
    final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSING,
        randomDatanodeDetails(),
        randomDatanodeDetails());

    // three replica in OPEN state
    final List<DatanodeDetails> datanodes =
        Arrays.asList(randomDatanodeDetails(),
        randomDatanodeDetails(), randomDatanodeDetails());
    replicas.addAll(getReplicas(id, State.OPEN,
        datanodes.get(0), datanodes.get(1), datanodes.get(2)));

    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentCloseCommandCount + 5, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));

    // Update the OPEN to CLOSING
    for (ContainerReplica replica : getReplicas(id, State.CLOSING,
        datanodes.get(0), datanodes.get(1), datanodes.get(2))) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentCloseCommandCount + 10, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.CLOSING));
  }

  /**
   * The container is QUASI_CLOSED ECcontainerHealthchecker should send close
   * command to all the replicas.
   */
  @Test
  public void testQuasiClosedEcContainer() throws IOException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(2, 1);
    final ContainerInfo container = getECContainer(
        LifeCycleState.QUASI_CLOSED, PipelineID.randomId(), repConfig);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getEcReplicas(id, State.QUASI_CLOSED,
        1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaTwo = getEcReplicas(
        id, State.OPEN, 1000L, originNodeId, randomDatanodeDetails(), 2);
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final ContainerReplica replicaThree = getEcReplicas(
        id, State.OPEN, 1000L, datanodeDetails.getUuid(), datanodeDetails, 3);

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(
        id, replicaThree);

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);
    // Two of the replicas are in OPEN state
    replicationManager.processAll();
    eventQueue.processAll(1000);
    //only quasi closed container will be force closed, open container
    // will be handled as unhealthy in the next process round
    Assert.assertEquals(currentCloseCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
    ReplicationManagerReport report = replicationManager.getContainerReport();
    Assert.assertEquals(1, report.getStat(LifeCycleState.QUASI_CLOSED));
  }

  /**
   * When a CLOSED container is over replicated, ECcontainerHealthchecker
   * deletes the excess replicas.
   */
  @Test
  public void testOverReplicatedClosedEcContainer() throws IOException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(2, 1);
    final ContainerInfo container = getECContainer(
        LifeCycleState.CLOSED, PipelineID.randomId(), repConfig);
    final ContainerID id = container.containerID();
    container.setUsedBytes(101);
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaTwo = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);
    final ContainerReplica replicaThree = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 3);
    //the over-replicated replica
    final ContainerReplica replicaFour = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaFive = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaSix = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);
    final ContainerReplica replicaSeven = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);
    containerStateManager.updateContainerReplica(id, replicaFive);
    containerStateManager.updateContainerReplica(id, replicaSix);
    containerStateManager.updateContainerReplica(id, replicaSeven);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 4, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentDeleteCommandCount + 4,
        replicationManager.getMetrics().getNumDeletionCmdsSent());

    // Now we remove the replica according to inflight
    final Map<ContainerID, List<InflightAction>> inflightEcDeletion =
        replicationManager.getInflightECDeletion();
    List<DatanodeDetails> targetDns = inflightEcDeletion.get(id)
        .stream().map(a -> a.getDatanode()).collect(Collectors.toList());
    // two index 1 and tow index 2 should be deleted
    Assert.assertEquals(targetDns.size(), 4);
    int deletedIndexOne = 0;
    int deletedIndexTwo = 0;
    for (DatanodeDetails d : targetDns) {
      if (d.equals(replicaOne.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaOne);
        deletedIndexOne++;
      } else if (d.equals(replicaTwo.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaTwo);
        deletedIndexTwo++;
      } else if (d.equals(replicaThree.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaThree);
      } else if (d.equals(replicaFour.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaFour);
        deletedIndexOne++;
      } else if (d.equals(replicaFive.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaFive);
        deletedIndexOne++;
      } else if (d.equals(replicaSix.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaSix);
        deletedIndexTwo++;
      } else if (d.equals(replicaSeven.getDatanodeDetails())) {
        containerStateManager.removeContainerReplica(
            id, replicaSeven);
        deletedIndexTwo++;
      }
    }
    Assert.assertEquals(deletedIndexOne, 2);
    Assert.assertEquals(deletedIndexTwo, 2);

    final long currentDeleteCommandCompleted = replicationManager.getMetrics()
        .getNumDeletionCmdsCompleted();
    final long deleteBytesCompleted =
        replicationManager.getMetrics().getNumDeletionBytesCompleted();

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(0, inflightEcDeletion.size());
    Assert.assertEquals(currentDeleteCommandCompleted + 4,
        replicationManager.getMetrics().getNumDeletionCmdsCompleted());
    Assert.assertEquals(deleteBytesCompleted + 404,
        replicationManager.getMetrics().getNumDeletionBytesCompleted());
  }

  /**
   * When a CLOSED container is under replicated , ECContaienrHealther should
   * send EcReconstruct command to recover it.
   */
  @Test
  public void testUnderReplicatedClosedEcContainer() throws IOException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(
        LifeCycleState.CLOSED, PipelineID.randomId(), repConfig);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaTwo = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);
    final ContainerReplica replicaThree = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 3);

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    final int currentReconstructCommandCount = datanodeCommandHandler
        .getInvocationCount(reconstructECContainersCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentReconstructCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            reconstructECContainersCommand));
    Assert.assertEquals(currentReconstructCommandCount + 1,
        replicationManager.getMetrics().getNumEcReconstrucCmdsSent());

    Assert.assertEquals(1,
        replicationManager.getInflightEcReconstruct().size());

    //verify the command
    List<CommandForDatanode> commands =
        datanodeCommandHandler.getReceivedCommands();
    Assert.assertEquals(1, commands.size());
    SCMCommand scmCommand = commands.get(0).getCommand();
    Assert.assertTrue(scmCommand instanceof ReconstructECContainersCommand);
    ReconstructECContainersCommand command =
        (ReconstructECContainersCommand)(scmCommand);
    Assert.assertEquals(command.getContainerID(), id.getId());
    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
        source = command.getSources();
    Assert.assertEquals(source.size(), 3);
    for (ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex
        rd : source) {
      if (rd.getReplicaIndex() == replicaOne.getReplicaIndex()) {
        Assert.assertEquals(rd.getDnDetails(), replicaOne.getDatanodeDetails());
      } else if (rd.getReplicaIndex() == replicaTwo.getReplicaIndex()) {
        Assert.assertEquals(rd.getDnDetails(), replicaTwo.getDatanodeDetails());
      } else if (rd.getReplicaIndex() == replicaThree.getReplicaIndex()) {
        Assert.assertEquals(rd.getDnDetails(),
            replicaThree.getDatanodeDetails());
      }
    }
    Assert.assertTrue(command.getTargetDatanodes().size() == 2);
    Assert.assertTrue(command.getMissingContainerIndexes().length == 2);
    Assert.assertTrue(command.getMissingContainerIndexes()[0] == (byte)4);
    Assert.assertTrue(command.getMissingContainerIndexes()[1] == (byte)5);

    //if only one replica is reconstructed, this command should not be completed
    final ContainerReplica replicaFour = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 4);
    containerStateManager.updateContainerReplica(id, replicaFour);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(1,
        replicationManager.getInflightEcReconstruct().size());

    //all the replica are reconstructed, this command should be completed
    final long currentEcReconstructCommandCompleted =
        replicationManager.getMetrics().getNumEcReconstructCmdsCompleted();
    final ContainerReplica replicaFive = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 5);
    containerStateManager.updateContainerReplica(id, replicaFive);
    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(0,
        replicationManager.getInflightEcReconstruct().size());
    Assert.assertEquals(currentEcReconstructCommandCompleted + 1,
        replicationManager.getMetrics().getNumEcReconstructCmdsCompleted());
  }

  /**
   * This test-case makes sure that ECcontainerHealthchecker doesn't take
   * any action on unrecoverable EC containers. just log a warn
   */
  @Test
  public void testUnreconverableEcContainer() throws IOException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(
        LifeCycleState.CLOSED, PipelineID.randomId(), repConfig);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaTwo = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);

    final List<CommandForDatanode> currentConmmands = datanodeCommandHandler
        .getReceivedCommands();
    Assert.assertTrue(currentConmmands.isEmpty());
    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertTrue(scmLogs.getOutput().contains(
        "container is unrecoverable"));
    Assert.assertTrue(currentConmmands.isEmpty());
  }

  /**
   * if a container is both over-replicated and under-replicated,
   * EcContainerHealthChecker should delete the over-replicated replicas
   * and reconstruct the missing index.
   */
  @Test
  public void testBothUnderAndOverReplicatedEcContainer() throws IOException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(2, 1);
    final ContainerInfo container = getECContainer(
        LifeCycleState.CLOSED, PipelineID.randomId(), repConfig);
    final ContainerID id = container.containerID();
    container.setUsedBytes(101);
    final UUID originNodeId = UUID.randomUUID();

    //index 1, 2 are over replicated, but index 3 is missing
    final ContainerReplica replicaOne = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaTwo = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);
    final ContainerReplica replicaFour = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaFive = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 1);
    final ContainerReplica replicaSix = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);
    final ContainerReplica replicaSeven = getEcReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails(), 2);

    containerStateManager.addContainer(container.getProtobuf());
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaFour);
    containerStateManager.updateContainerReplica(id, replicaFive);
    containerStateManager.updateContainerReplica(id, replicaSix);
    containerStateManager.updateContainerReplica(id, replicaSeven);
    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);
    final int currentEcReconstructCommandCount = datanodeCommandHandler
        .getInvocationCount(reconstructECContainersCommand);

    replicationManager.processAll();
    eventQueue.processAll(1000);
    Assert.assertEquals(currentDeleteCommandCount + 4, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertEquals(currentEcReconstructCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.reconstructECContainersCommand));
  }
}
