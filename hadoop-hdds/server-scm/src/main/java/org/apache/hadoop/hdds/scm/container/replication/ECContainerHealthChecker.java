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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ECContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;


/**
 * ECContainerHealthChecker is used to check ec container health,
 * and handle under and over replication containers if necessary.
 */
public class ECContainerHealthChecker {

  public static final Logger LOG =
      LoggerFactory.getLogger(ECContainerHealthChecker.class);

  /**
   * Reference to the ContainerManager.
   */
  private final ContainerManager containerManager;

  /**
   * SCMContext from StorageContainerManager.
   */
  private final SCMContext scmContext;

  /**
   * PlacementPolicy which is used to identify where a container
   * should be replicated.
   */
  private PlacementPolicy containerPlacement;

  /**
   * Used to lookup the health of a nodes or the nodes operational state.
   */
  private final NodeManager nodeManager;

  /**
   * Replication progress related metrics.
   */
  private ReplicationManagerMetrics metrics;

  /**
   * EventPublisher to fire Replicate and Delete container events.
   */
  private final EventPublisher eventPublisher;

  private final Clock clock;

  private final long eventTimeout;

  /**
   * Current container size as a bound for choosing datanodes with
   * enough space for a replica.
   */
  private long currentContainerSize;

  /**
   * remainingMaintenanceRedundancy is how many replicas we can still lose
   * when some are in maintenance.
   * For EC-3-2, and remainingRedundancy of 1, we can have 1
   * in maintenance without replicating.
   * For EC-6-3, and remainingRedundancy of 1, we can have 2
   * in maintenance without replicating.
   * In both these cases, one more replica could fail without
   * affecting data availability.
   * */
  private final int remainingMaintenanceRedundancy;

  /**
   * This is used for tracking EC container Reconstruct
   * commands which are issued by ReplicationManager and not yet complete.
   * for EC container, only one Reconstruct command could be inflight.
   * if it fail or timeout , we could send a new one.
   */
  private final Map<ContainerID, List<InflightAction>> inflightECReconstruct;

  /**
   * This is used for tracking container deletion commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightEcDeletion;

  /**
   * This is used for tracking container replication commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightEcReplication;

  @SuppressWarnings("parameternumber")
  public ECContainerHealthChecker(
      final ConfigurationSource conf,
      final ContainerManager containerManager,
      final EventPublisher eventPublisher,
      final NodeManager nodeManager,
      final SCMContext scmContext,
      final long eventTimeout,
      final PlacementPolicy containerPlacement,
      final Clock clock,
      final int remainingMaintenanceRedundancy) {
    this.containerManager = containerManager;
    this.eventPublisher = eventPublisher;
    inflightECReconstruct = new ConcurrentHashMap<>();
    inflightEcDeletion = new ConcurrentHashMap<>();
    inflightEcReplication = new ConcurrentHashMap<>();
    this.clock = clock;
    this.eventTimeout = eventTimeout;
    this.scmContext = scmContext;
    this.nodeManager = nodeManager;
    this.metrics = null;
    this.containerPlacement = containerPlacement;
    this.remainingMaintenanceRedundancy = remainingMaintenanceRedundancy;

    this.currentContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);
  }

  @VisibleForTesting
  public synchronized void setContainerPlacement(
      final PlacementPolicy containerPlacement) {
    this.containerPlacement = containerPlacement;
  }

  protected synchronized void setMetrics(
      ReplicationManagerMetrics metrics) {
    this.metrics = metrics;
  }

  protected synchronized void clearInflightActions() {
    inflightECReconstruct.clear();
    inflightEcDeletion.clear();
    inflightEcReplication.clear();
  }


  @SuppressWarnings("checkstyle:methodlength")
  protected void processContainer(
      ContainerInfo container,
      ReplicationManagerReport report) {
    if (container.getReplicationType() != HddsProtos.ReplicationType.EC) {
      LOG.warn("container is not an EC container, should not be processed" +
          "in ECContainerHealthChecker, {}", container);
      return;
    }
    final ContainerID id = container.containerID();
    try {
      synchronized (container) {
        final Set<ContainerReplica> replicas = containerManager
            .getContainerReplicas(id);
        final LifeCycleState state = container.getState();
        report.increment(state);

        /*
         * We don't take any action if the container is in OPEN state and
         * the container is healthy. If the container is not healthy, i.e.
         * the replicas are not in OPEN state, send CLOSE_CONTAINER command.
         */
        if (state == LifeCycleState.OPEN) {
          if (!isOpenContainerHealthy(container, replicas)) {
            report.incrementAndSample(
                ReplicationManagerReport.HealthState.OPEN_UNHEALTHY,
                container.containerID());
            eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, id);
          }
          return;
        }

        /*
         * If the container is in CLOSING state, the replicas can either
         * be in OPEN or in CLOSING state. In both of this cases
         * we have to resend close container command to the datanodes.
         */
        if (state == LifeCycleState.CLOSING) {
          for (ContainerReplica replica: replicas) {
            if (replica.getState() != State.UNHEALTHY) {
              sendCloseCommand(container, replica.getDatanodeDetails(), false);
            }
          }
          return;
        }

        /*
         * TODO: we just ignore QUASI_CLOSED, any close it directly for now.
         * later, we should find an approach to identify which replicas has the
         * latest status, and delete replcas with stale state.
         */
        if (state == LifeCycleState.QUASI_CLOSED) {
          forceCloseEcContainer(container, replicas);
          return;
        }

        /*
         * Before processing the container we have to reconcile the
         * inflightEcReconstruct and inflightECDeletion actions.
         *
         * We remove the entry from inflightECReconstruct and inflightDeletion
         * list, if the operation is completed or if it has timed out.
         */

        //if all the container index in missingIndex of the action appears,
        //the command should be considered as completed.
        updateInflightAction(container, inflightECReconstruct,
            action -> replicas.stream().map(ContainerReplica::getReplicaIndex)
                .collect(Collectors.toList())
                .containsAll(((InflightEcReconstruct)action)
                    .getMissingContainerIndexs()),
            () -> metrics.incrNumEcRecstructCmdsTimeout(),
            action -> updateCompletedECReconstructMetrics(container));

        updateInflightAction(container, inflightEcDeletion,
            action -> replicas.stream().noneMatch(
                r -> r.getDatanodeDetails().equals(action.getDatanode())),
            () -> metrics.incrNumDeletionCmdsTimeout(),
            action -> updateCompletedDeletionMetrics(container, action));

        updateInflightAction(container, inflightEcReplication,
            action -> replicas.stream().anyMatch(
                r -> r.getDatanodeDetails().equals(action.getDatanode())),
            () -> metrics.incrNumDeletionCmdsTimeout(),
            action -> updateCompletedReplicationMetrics(container, action));

        /*
         * If container is under deleting and all it's replicas are deleted,
         * then make the container as CLEANED,
         * or resend the delete replica command if needed.
         */
        if (state == LifeCycleState.DELETING) {
          handleContainerUnderDelete(container, replicas);
          return;
        }

        /**
         * We don't need to take any action for a DELETE container - eventually
         * it will be removed from SCM.
         */
        if (state == LifeCycleState.DELETED) {
          return;
        }

        /*
         * We don't have to take any action if the container is healthy.
         *
         * According to ReplicationMonitor container is considered healthy if
         * the container is either in QUASI_CLOSED or in CLOSED state and has
         * exact number of replicas in the same state.
         */
        if (isContainerEmpty(container, replicas)) {
          report.incrementAndSample(
              ReplicationManagerReport.HealthState.EMPTY,
              container.containerID());
          /*
           *  If container is empty, schedule task to delete the container.
           */
          deleteContainerReplicas(container, replicas);
          return;
        }

        ECContainerReplicaCount replicaCount =
            getECContainerReplicaCount(container, replicas);
        ContainerPlacementStatus placementStatus = getPlacementStatus(
            replicas, container.getReplicationConfig().getRequiredNodes());


        /*
        *we need to handle over-replicated index, and then handle
        * under-replicated index
         */
        if (replicaCount.isOverReplicated()) {
          report.incrementAndSample(
              ReplicationManagerReport.HealthState.OVER_REPLICATED,
              container.containerID());
          handleOverReplicatedContainerIndex(container, replicas, replicaCount);
        }

        /*
         * Check if the container is under replicated and take appropriate
         * action.
         * note that, for EC container, it can be both under-replicated and
         * over-replicated
         */
        boolean sufficientlyReplicated =
            replicaCount.isSufficientlyReplicated();
        boolean placementSatisfied = placementStatus.isPolicySatisfied();

        if (!sufficientlyReplicated || !placementSatisfied) {
          if (!sufficientlyReplicated) {
            report.incrementAndSample(
                ReplicationManagerReport.HealthState.UNDER_REPLICATED,
                container.containerID());
            if (replicaCount.unRecoverable()) {
              //TODO:should we delete those unrecoverable container replica?
              //TODO:add a state UNRECOVERABLE later
              report.incrementAndSample(
                  ReplicationManagerReport.HealthState.MISSING,
                  container.containerID());
              LOG.warn("container is unrecoverable {}.", container);
              return;
            }
          }
          if (!placementSatisfied) {
            report.incrementAndSample(
                ReplicationManagerReport.HealthState.MIS_REPLICATED,
                container.containerID());
          }

          handleUnderReplicatedContainerIndex(
              container, replicas, replicaCount, placementStatus);
        }

        /*
       If we get here, the container is not over replicated or under replicated
       but it may be "unhealthy", which means it has one or more replica which
       are not in the same state as the container itself.
       */
        if (!isHealthy(container, replicas)) {
          report.incrementAndSample(
              ReplicationManagerReport.HealthState.UNHEALTHY,
              container.containerID());
          handleUnstableContainer(container, replicas);
        }
      }
    } catch (ContainerNotFoundException ex) {
      LOG.warn("Missing container {}.", id);
    } catch (Exception ex) {
      LOG.warn("Process container {} error: ", id, ex);
    }
  }

  /**
   * Compares the container state with the replica state.
   *
   * @param containerState ContainerState
   * @param replicaState ReplicaState
   * @return true if the state matches, false otherwise
   */
  public static boolean compareState(
      final LifeCycleState containerState,
      final State replicaState) {
    switch (containerState) {
    case OPEN:
      return replicaState == State.OPEN;
    case CLOSING:
      return replicaState == State.CLOSING;
    case QUASI_CLOSED:
      return replicaState == State.QUASI_CLOSED;
    case CLOSED:
      return replicaState == State.CLOSED;
    case DELETING:
      return false;
    case DELETED:
      return false;
    default:
      return false;
    }
  }

  /**
   * An open container is healthy if all its replicas are in the same state as
   * the container.
   * @param container The container to check
   * @param replicas The replicas belonging to the container
   * @return True if the container is healthy, false otherwise
   */
  private boolean isOpenContainerHealthy(
      ContainerInfo container, Set<ContainerReplica> replicas) {
    LifeCycleState state = container.getState();
    return replicas.stream()
        .allMatch(r -> compareState(state, r.getState()));
  }

  /**
   * since the BCSID of EC container is always 0 for now,
   * we just force close all the quasi closed container.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void forceCloseEcContainer(
      final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.QUASI_CLOSED);
    replicas.stream().filter(r -> r.getState() == State.QUASI_CLOSED)
        .forEach(replica -> sendCloseCommand(
            container, replica.getDatanodeDetails(), true));
  }

  private void updateCompletedECReconstructMetrics(
      ContainerInfo container) {
    metrics.incrNumEcReconstructCmdsCompleted();
    metrics.incrNumEcRestructBytesCompleted(container.getUsedBytes());
  }

  private void updateCompletedDeletionMetrics(ContainerInfo container,
                                              InflightAction action) {
    metrics.incrNumDeletionCmdsCompleted();
    metrics.incrNumDeletionBytesCompleted(container.getUsedBytes());
    metrics.addDeletionTime(clock.millis() - action.getTime());
  }

  private void updateCompletedReplicationMetrics(ContainerInfo container,
                                                 InflightAction action) {
    metrics.incrNumReplicationCmdsCompleted();
    metrics.incrNumReplicationBytesCompleted(container.getUsedBytes());
    metrics.addReplicationTime(clock.millis() - action.getTime());
  }

  /**
   * Reconciles the InflightActions for a given container.
   *
   * @param container Container to update
   * @param inflightEcActions inflightDeletion (or) inflightEcRestruct
   * @param filter filter to check if the operation is completed
   * @param timeoutCounter update timeout metrics
   * @param completedCounter update completed metrics
   */
  private void updateInflightAction(
      final ContainerInfo container,
      final Map<ContainerID, List<InflightAction>> inflightEcActions,
      final Predicate<InflightAction> filter,
      final Runnable timeoutCounter,
      final Consumer<InflightAction> completedCounter) {
    final ContainerID id = container.containerID();
    final long deadline = clock.millis() - eventTimeout;
    if (inflightEcActions.containsKey(id)) {
      final List<InflightAction> actions = inflightEcActions.get(id);

      Iterator<InflightAction> iter = actions.iterator();
      while (iter.hasNext()) {
        try {
          InflightAction a = iter.next();
          NodeStatus status = nodeManager.getNodeStatus(a.getDatanode());
          boolean isUnhealthy =
              status.getHealth() != HddsProtos.NodeState.HEALTHY;
          boolean isCompleted = filter.test(a);
          boolean isTimeout = a.getTime() < deadline;
          boolean isNotInService = status.getOperationalState() !=
              HddsProtos.NodeOperationalState.IN_SERVICE;
          if (isCompleted || isUnhealthy || isTimeout || isNotInService) {
            iter.remove();

            if (isTimeout) {
              timeoutCounter.run();
            } else if (isCompleted) {
              completedCounter.accept(a);
            }
            // for now , we do not supporting move EC container
            //updateMoveIfNeeded(isUnhealthy, isCompleted, isTimeout,
             //   isNotInService, container, a.getDatanode(), inflightActions);
          }
        } catch (NodeNotFoundException e) {
          // Should not happen, but if it does, just remove the action as the
          // node somehow does not exist;
          iter.remove();
        }
      }
      if (actions.isEmpty()) {
        inflightEcActions.remove(id);
      }
    }
  }

  /**
   * Handle the container which is under delete.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleContainerUnderDelete(
      final ContainerInfo container,
      final Set<ContainerReplica> replicas) throws IOException,
      InvalidStateTransitionException {
    if (replicas.size() == 0) {
      containerManager.updateContainerState(container.containerID(),
          HddsProtos.LifeCycleEvent.CLEANUP);
      LOG.debug("Container {} state changes to DELETED", container);
    } else {
      // Check whether to resend the delete replica command
      final List<DatanodeDetails> deletionInFlight = inflightEcDeletion
          .getOrDefault(container.containerID(), Collections.emptyList())
          .stream().map(action -> action.getDatanode())
          .collect(Collectors.toList());

      Set<ContainerReplica> filteredReplicas = replicas.stream().filter(
              r -> !deletionInFlight.contains(r.getDatanodeDetails()))
          .collect(Collectors.toSet());
      // Resend the delete command
      if (filteredReplicas.size() > 0) {
        filteredReplicas.stream().forEach(rp ->
            sendDeleteCommand(container, rp, false));
        LOG.debug("Resend delete Container command for {}", container);
      }
    }
  }


  /**
   * If the given container is under replicated, identify a new set of
   * datanode(s) to replicate the container using PlacementPolicy
   * and send EcReconstruct command to the identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicaCount EcContainerReplicaCount of the given container
   * @param replicas the replica set of this container
   */
  private void handleUnderReplicatedContainerIndex(
      final ContainerInfo container,
      final Set<ContainerReplica> replicas,
      final ECContainerReplicaCount replicaCount,
      final ContainerPlacementStatus placementStatus) {

    if (replicaCount.isSufficientlyReplicated()
        && placementStatus.isPolicySatisfied()) {
      LOG.info("The container {} with replicas {} is sufficiently " +
              "replicated and is not mis-replicated",
          container.getContainerID(), replicas);
      return;
    }

    final ReplicationConfig repConf = container.getReplicationConfig();
    if (!(repConf instanceof ECReplicationConfig)) {
      LOG.warn("container {} is not an EC container", container);
      return;
    }

    final int dataNum = ((ECReplicationConfig)repConf).getData();
    final ContainerID id = container.containerID();

    //select source datanodes with different indexes.
    //here, we only need to find dataNum different replica indexes
    // since we can recover all the replicas with them if recoverable
    final List<DatanodeDetails> deletionInFlight = inflightEcDeletion
        .getOrDefault(id, Collections.emptyList())
        .stream().map(action -> action.getDatanode())
        .collect(Collectors.toList());

    final List<ContainerReplica> eligibleSource = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED ||
                r.getState() == State.CLOSED)
        .filter(r -> getNodeStatus(r.getDatanodeDetails()).isHealthy())
        .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
        .collect(Collectors.toList());

    final Map<Integer, List<ContainerReplica>> index2replicas =
        new LinkedHashMap<>();

    eligibleSource.forEach(r -> {
      Integer index = r.getReplicaIndex();
      index2replicas.computeIfAbsent(index, k -> new LinkedList<>());
      index2replicas.get(index).add(r);
    });

    if (index2replicas.size() >= dataNum) {
      final List<DatanodeDetails> excludeList = replicas.stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());

      //find all pending add
      final List<DatanodeDetails> inFlightAdd = inflightEcReplication
          .getOrDefault(id, Collections.emptyList())
          .stream().map(action -> action.getDatanode())
          .collect(Collectors.toList());
      inflightECReconstruct.getOrDefault(id, Collections.emptyList())
          .stream().map(action ->
              ((InflightEcReconstruct)(action)).getTargetDataNodes())
          .forEach(l -> inFlightAdd.addAll(l));

      excludeList.addAll(inFlightAdd);

      final long dataSizeRequired = Math.max(container.getUsedBytes(),
          currentContainerSize);

      //missingNonMaintenanceIndexes has taken inflight reconstruction
      // into account. if missingNonMaintenanceIndexes is empty,
      //it means the reason why this container is under-replicated is
      //placement policy is not satisfied
      final List<Integer> missingNonMaintenanceIndexes =
          replicaCount.missingNonMaintenanceIndexes();

      //if some index is missing , we just schedule reconstruction.
      //if placement policy is not satisfied, we try to see if we could find
      //some other node to improve the replication status. if yes , we can
      //just schedule replication.
      List<DatanodeDetails> selectedDatanodes;
      try {
        if (missingNonMaintenanceIndexes.size() > 0) {
          selectedDatanodes = containerPlacement.chooseDatanodes(excludeList,
              null, missingNonMaintenanceIndexes.size(), 0, dataSizeRequired);
          //send reconstruct command with the necessary infos to the
          //coordinator datanode(the first in selectedDatanodes)
          sendAndTrackEcReconstructCommand(container, eligibleSource,
              selectedDatanodes, missingNonMaintenanceIndexes);
        } else {
          //try to improve placement status if possible
          List<DatanodeDetails> targetReplicas =
              new ArrayList<>(eligibleSource.stream()
              .map(ContainerReplica::getDatanodeDetails)
                  .collect(Collectors.toList()));
          // add any pending additions
          targetReplicas.addAll(inFlightAdd);

          //check placement policy
          final int replicationFactor = container
              .getReplicationConfig().getRequiredNodes();
          final ContainerPlacementStatus inFlightPlacementStatus =
              containerPlacement.validateContainerPlacement(
                  targetReplicas, replicationFactor);
          // the num of extra racks the replcas should be in to satisfy
          // the placement policy. since the container is
          // sufficiently replicated.
          final int misRepDelta = inFlightPlacementStatus.misReplicationCount();
          if (misRepDelta > 0) {
            selectedDatanodes = containerPlacement.chooseDatanodes(excludeList,
                null, misRepDelta, 0, dataSizeRequired);
            targetReplicas.addAll(selectedDatanodes);

            //if replicating some replica will improve placement status
            if (misRepDelta < containerPlacement.validateContainerPlacement(
                targetReplicas, replicationFactor).misReplicationCount()) {
              //since the container is sufficiently replicated, we can select
              //any index to be replicated. to be simple, we just use the index
              final int selectedSize = selectedDatanodes.size();
              if (selectedSize > replicationFactor) {
                LOG.warn("selected target node is bigger than " +
                    "replicationFactor just skip replication. " +
                    "container {}", container);
                return;
              }
              for (int i = 0; i < selectedDatanodes.size(); i++) {
                sendReplicateCommand(container, selectedDatanodes.get(i),
                    index2replicas.get(i));
              }
            }
          } else {
            LOG.info("EC container {} does not meet the placement for now, " +
                "but it will do after including all the inflight ADD. " +
                "no need to schedule new replication now",
                container.containerID());
          }
        }
      } catch (Exception e) {
        LOG.warn("Exception while selecting target datanodes for EC replica. " +
                "contain: {}, cause : {}", container, e.getMessage());
        return;
      }
    } else {
      LOG.warn("Cannot reconstruct container {}, not enough healthy " +
              "replica index found.", container.containerID());
    }
  }

  /**
   * If the given container is over replicated, identify the datanode(s)
   * to delete the container and send delete container command to the
   * identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicas the replica set of the given container
   * @param replicaCount ECContainerReplicaCount of the given container
   */
  private void handleOverReplicatedContainerIndex(
      final ContainerInfo container, final Set<ContainerReplica> replicas,
      final ECContainerReplicaCount replicaCount) {
    List<Integer> overReplicatedIndexs = replicaCount.overReplicatedIndexes();
    Map<Integer, List<ContainerReplica>> index2replicas = new HashMap<>();

    //build a map to indicate the replica list for each index
    replicas.forEach(r -> {
      Integer index = r.getReplicaIndex();
      if (overReplicatedIndexs.contains(index)) {
        index2replicas.computeIfAbsent(index, k -> new LinkedList<>());
        index2replicas.get(index).add(r);
      }
    });

    final Set<ContainerReplica> eligibleSet = new HashSet<>(replicas);

    //for the replica list of each index, we remove all the ineligible
    // replica to get a candidate list and send the command to delete
    // these candidate replicas one by one until there is only one
    // element in the candidate.
    index2replicas.values().forEach(l -> {
      final Map<UUID, ContainerReplica> uniqueReplicas =
          new LinkedHashMap<>();

      if (container.getState() != LifeCycleState.CLOSED) {
        replicas.stream()
            .filter(r -> compareState(container.getState(), r.getState()))
            .forEach(r ->
                uniqueReplicas.putIfAbsent(r.getOriginDatanodeId(), r));
        l.removeAll(uniqueReplicas.values());
        eligibleSet.removeAll(uniqueReplicas.values());
      }

      // Replica which are maintenance or decommissioned are not eligible to
      // be removed, as they do not count toward over-replication and they
      // also many not be available
      l.removeIf(r -> r.getDatanodeDetails().getPersistedOpState() !=
              HddsProtos.NodeOperationalState.IN_SERVICE);
      eligibleSet.removeIf(r -> r.getDatanodeDetails().getPersistedOpState() !=
          HddsProtos.NodeOperationalState.IN_SERVICE);

      final List<ContainerReplica> unhealthyReplicas = l.stream()
          .filter(r -> !compareState(container.getState(), r.getState()))
          .collect(Collectors.toList());

      // If there are unhealthy replicas, then we should remove them even if it
      // makes the container violate the placement policy, as excess unhealthy
      // containers are not really useful. It will be corrected later as a
      // mis-replicated container will be seen as under-replicated.
      for (ContainerReplica r : unhealthyReplicas) {
        if (l.size() > 0) {
          sendDeleteCommand(container, r, true);
          l.remove(r);
        } else {
          break;
        }
      }
      eligibleSet.removeAll(unhealthyReplicas);
    });

    final int replicationFactor =
        container.getReplicationConfig().getRequiredNodes();
    final ContainerPlacementStatus ps =
        getPlacementStatus(eligibleSet, replicationFactor);

    index2replicas.values().forEach(l -> {
      //if we have more than one replicas with the same index, try to delete
      //according to the placement policy
      l.sort(Comparator.comparingLong(ContainerReplica::hashCode));
      Iterator<ContainerReplica> iterator = l.iterator();
      while (iterator.hasNext() && l.size() > 1) {
        ContainerReplica r = iterator.next();
        eligibleSet.remove(r);
        ContainerPlacementStatus nowPS =
            getPlacementStatus(eligibleSet, replicationFactor);
        if (isPlacementStatusActuallyEqual(ps, nowPS)) {
          // Remove the replica if the container was already unsatisfied
          // and losing this replica keep actual placement count unchanged.
          // OR if losing this replica still keep satisfied
          sendDeleteCommand(container, r, true);
          iterator.remove();
        } else {
          eligibleSet.add(r);
        }
      }
    });

  }

  /**
   * Given a container and its set of replicas, create and return a
   * EcContainerReplicaCount representing the container.
   *
   * @param container The container for which to construct a
   *                  ContainerReplicaCount
   * @param replicas The set of existing replica for this container
   * @return EcContainerReplicaCount representing the current state of the
   *         container
   */
  private ECContainerReplicaCount getECContainerReplicaCount(
      ContainerInfo container, Set<ContainerReplica> replicas) {
    ContainerID cid = container.containerID();

    Set<Integer> indexesPendingAdd = new HashSet<>();
    inflightECReconstruct.getOrDefault(cid, Collections.emptyList())
        .forEach(a -> indexesPendingAdd.addAll(
            ((InflightEcReconstruct)a).getMissingContainerIndexs()));

    inflightEcReplication.getOrDefault(cid, Collections.emptyList())
        .forEach(a -> indexesPendingAdd.add(((InflightEcAction)a).getIndex()));

    Set<Integer> indexesPendingDelete = new HashSet<>();
    inflightEcDeletion.getOrDefault(cid, Collections.emptyList()).forEach(
        a -> indexesPendingDelete.add(((InflightEcAction)a).getIndex())
    );

    return new ECContainerReplicaCount(
        container, replicas,
        new ArrayList<>(indexesPendingAdd),
        new ArrayList<>(indexesPendingDelete),
        remainingMaintenanceRedundancy);
  }


  /**
   * Sends close container command for the given container to the given
   * datanode.
   *
   * @param container Container to be closed
   * @param datanode The datanode on which the container
   *                  has to be closed
   * @param force Should be set to true if we want to close a
   *               QUASI_CLOSED container
   */
  private void sendCloseCommand(final ContainerInfo container,
                                final DatanodeDetails datanode,
                                final boolean force) {

    ContainerID containerID = container.containerID();
    LOG.info("Sending close container command for container {}" +
        " to datanode {}.", containerID, datanode);
    CloseContainerCommand closeContainerCommand =
        new CloseContainerCommand(container.getContainerID(),
            container.getPipelineID(), force);
    try {
      closeContainerCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending close container command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    closeContainerCommand.setEncodedToken(getContainerToken(containerID));
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(datanode.getUuid(), closeContainerCommand));
  }

  private String getContainerToken(ContainerID containerID) {
    if (scmContext.getScm() instanceof StorageContainerManager) {
      StorageContainerManager scm =
          (StorageContainerManager) scmContext.getScm();
      return scm.getContainerTokenGenerator().generateEncodedToken(containerID);
    }
    return "";
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode.
   *
   * @param container Container to be deleted
   * @param replica the replica should be deleted
   * @param force Should be set to true to delete an OPEN replica
   */
  private void sendDeleteCommand(final ContainerInfo container,
                                 final ContainerReplica replica,
                                 final boolean force) {
    DatanodeDetails datanode = replica.getDatanodeDetails();
    LOG.info("Sending delete container command for container {}" +
        " to datanode {}", container.containerID(), datanode);

    final ContainerID id = container.containerID();
    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(id.getId(), force);
    inflightEcDeletion.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, deleteCommand,
        replica.getReplicaIndex(),
        action -> inflightEcDeletion.get(id).add(action));

    metrics.incrNumDeletionCmdsSent();
    metrics.incrNumDeletionBytesTotal(container.getUsedBytes());
  }

  /**
   * Send ec reconstruct command for the given container to the coordinator
   * datanode.
   *
   * @param container Container to be reconstrunct
   * @param sources The list of source datanodes on which the healthy replias
   *                exist.
   * @param target The list of target datanodes on which the missing replicas
   *               will be reconstructed
   * @param missingNonMaintenanceIndexes the list of indexes of missing replicas
   */
  private void sendAndTrackEcReconstructCommand(
      final ContainerInfo container,
      final List<ContainerReplica> sources,
      final List<DatanodeDetails> target,
      final List<Integer> missingNonMaintenanceIndexes) {
    long containerID = container.containerID().getId();
    if (target.size() == 0) {
      LOG.warn("can not send ec reconstruct command for container {}, " +
          "no candidate target datanode", container);
      return;
    }

    LOG.info("Sending ec reconstruct command for container {}" +
        " to datanode {}, source: {}, targets: {}",
        containerID, target.get(0), sources, target);

    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
        sourceReplicas = sources.stream()
        .map(r -> new ReconstructECContainersCommand
        .DatanodeDetailsAndReplicaIndex(
            r.getDatanodeDetails(), r.getReplicaIndex()))
        .collect(Collectors.toList());

    int missingNum = missingNonMaintenanceIndexes.size();
    byte[] missingAsBytes = new byte[missingNum];
    for (int i = 0; i < missingNum; i++) {
      missingAsBytes[i] = missingNonMaintenanceIndexes.get(i).byteValue();
    }

    final ReconstructECContainersCommand reconstructECContainersCommand =
        new ReconstructECContainersCommand(containerID, sourceReplicas,
            target, missingAsBytes,
            (ECReplicationConfig)(container.getReplicationConfig()));

    //send and track the command
    try {
      reconstructECContainersCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending datanode command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    DatanodeDetails targetDataNode = target.get(0);

    final CommandForDatanode datanodeCommand =
        new CommandForDatanode(targetDataNode.getUuid(),
            reconstructECContainersCommand);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    ContainerID cid = container.containerID();
    inflightECReconstruct.computeIfAbsent(cid, k -> new LinkedList<>());
    inflightECReconstruct.get(cid).add(new InflightEcReconstruct(
        targetDataNode, clock.millis(),
        missingNonMaintenanceIndexes, target));

    metrics.incrNumEcReconstructCmdsSent();
  }

  /**
   * Creates CommandForDatanode with the given SCMCommand and fires
   * DATANODE_COMMAND event to event queue.
   *
   * Tracks the command using the given tracker.
   *
   * @param datanode Datanode to which the command has to be sent
   * @param command SCMCommand to be sent
   * @param tracker Tracker which tracks the inflight actions
   * @param index the replica index of this action
   * @param <T> Type of SCMCommand
   */
  private <T extends GeneratedMessage> void sendAndTrackDatanodeCommand(
      final DatanodeDetails datanode,
      final SCMCommand<T> command,
      final int index,
      final Consumer<InflightAction> tracker) {
    try {
      command.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending datanode command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    final CommandForDatanode<T> datanodeCommand =
        new CommandForDatanode<>(datanode.getUuid(), command);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    tracker.accept(new InflightEcAction(datanode, clock.millis(), index));
  }

  /**
   * Sends replicate container command for the given container to the given
   * datanode.
   *
   * @param container Container to be replicated
   * @param datanode The destination datanode to replicate
   * @param sources List of source replica from where we can replicate
   */
  private void sendReplicateCommand(final ContainerInfo container,
                                    final DatanodeDetails datanode,
                                    final List<ContainerReplica> sources) {

    LOG.info("Sending replicate container command for container {}" +
            " to datanode {} from datanodes {}",
        container.containerID(), datanode, sources);
    int index = sources.get(0).getReplicaIndex();
    //sanity check
    if (sources.stream().map(ContainerReplica::getReplicaIndex)
        .anyMatch(a -> a != index)) {
      LOG.warn("all the source replica should have the same index when" +
          "sending replication command");
    }

    final ContainerID id = container.containerID();
    final ReplicateContainerCommand replicateCommand =
        new ReplicateContainerCommand(id.getId(),
            sources.stream().map(ContainerReplica::getDatanodeDetails)
                .collect(Collectors.toList()));
    inflightEcReplication.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, replicateCommand, index,
        action -> inflightEcReplication.get(id).add(action));

    metrics.incrNumReplicationCmdsSent();
    metrics.incrNumReplicationBytesTotal(container.getUsedBytes());
  }

  /**
   * Returns true if the container is empty and CLOSED.
   * A container is deemed empty if its keyCount (num of blocks) is 0. The
   * usedBytes counter is not checked here because usedBytes is not a
   * accurate representation of the committed blocks. There could be orphaned
   * chunks in the container which contribute to the usedBytes.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if the container is empty, false otherwise
   */
  private boolean isContainerEmpty(
      final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    return container.getState() == LifeCycleState.CLOSED &&
        container.getNumberOfKeys() == 0 && replicas.stream().allMatch(
            r -> r.getState() == State.CLOSED && r.getKeyCount() == 0);
  }

  /**
   * Delete the container and its replicas.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void deleteContainerReplicas(
      final ContainerInfo container,
      final Set<ContainerReplica> replicas) throws IOException,
      InvalidStateTransitionException {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.CLOSED);
    Preconditions.assertTrue(container.getNumberOfKeys() == 0);

    replicas.stream().forEach(rp -> {
      Preconditions.assertTrue(rp.getState() == State.CLOSED);
      Preconditions.assertTrue(rp.getKeyCount() == 0);
      sendDeleteCommand(container, rp, false);
    });
    containerManager.updateContainerState(container.containerID(),
        HddsProtos.LifeCycleEvent.DELETE);
    LOG.debug("Deleting empty EC container replicas for {},", container);
  }

  /**
   * Handles unstable container.
   * A container is inconsistent if any of the replica state doesn't
   * match the container state. We have to take appropriate action
   * based on state of the replica.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleUnstableContainer(final ContainerInfo container,
                                       final Set<ContainerReplica> replicas) {
    List<ContainerReplica> unhealthyReplicas = replicas.stream()
        .filter(r -> !compareState(container.getState(), r.getState()))
        .collect(Collectors.toList());

    Iterator<ContainerReplica> iterator = unhealthyReplicas.iterator();
    while (iterator.hasNext()) {
      final ContainerReplica replica = iterator.next();
      final State state = replica.getState();
      if (state == State.OPEN || state == State.CLOSING) {
        sendCloseCommand(container, replica.getDatanodeDetails(), false);
        iterator.remove();
      } else if (state == State.QUASI_CLOSED) {
        sendCloseCommand(container, replica.getDatanodeDetails(), true);
        iterator.remove();
      }
    }

    // Now we are left with the replicas which are either unhealthy .
    // These replicas should be deleted.
    unhealthyReplicas.stream().findFirst().ifPresent(replica ->
        sendDeleteCommand(container, replica, true));
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the containe
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  private ContainerPlacementStatus getPlacementStatus(
      Set<ContainerReplica> replicas, int replicationFactor) {
    List<DatanodeDetails> replicaDns = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    return containerPlacement.validateContainerPlacement(
        replicaDns, replicationFactor);
  }

  /**
   * Wrap the call to nodeManager.getNodeStatus, catching any
   * NodeNotFoundException and instead throwing an IllegalStateException.
   * @param dn The datanodeDetails to obtain the NodeStatus for
   * @return NodeStatus corresponding to the given Datanode.
   */
  private NodeStatus getNodeStatus(DatanodeDetails dn) {
    try {
      return nodeManager.getNodeStatus(dn);
    } catch (NodeNotFoundException e) {
      throw new IllegalStateException("Unable to find NodeStatus for " + dn, e);
    }
  }

  /**
   * Returns true if the container is healthy, meaning all replica which are not
   * in a decommission or maintenance state are in the same state as the
   * container and in CLOSED state.
   *
   * @return true if the container is healthy, false otherwise
   */
  private boolean isHealthy(
      ContainerInfo container, Set<ContainerReplica> replicas) {
    return (container.getState() == HddsProtos.LifeCycleState.CLOSED)
        && replicas.stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() == IN_SERVICE)
        .allMatch(r -> compareState(container.getState(), r.getState()));
  }

  /**
   * whether the given two ContainerPlacementStatus are actually equal.
   *
   * @param cps1 ContainerPlacementStatus
   * @param cps2 ContainerPlacementStatus
   */
  private boolean isPlacementStatusActuallyEqual(
      ContainerPlacementStatus cps1,
      ContainerPlacementStatus cps2) {
    return (!cps1.isPolicySatisfied() &&
        cps1.actualPlacementCount() == cps2.actualPlacementCount()) ||
        cps1.isPolicySatisfied() && cps2.isPolicySatisfied();
  }

  public Map<ContainerID, List<InflightAction>> getInflightECDeletion() {
    return inflightEcDeletion;
  }

  public Map<ContainerID, List<InflightAction>> getInflightEcReconstruct() {
    return inflightECReconstruct;
  }
}
