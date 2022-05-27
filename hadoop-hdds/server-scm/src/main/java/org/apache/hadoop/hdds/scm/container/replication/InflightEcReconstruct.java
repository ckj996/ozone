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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;

import java.util.List;


/**
 * InflightEcReconstruct is a Wrapper class to hold the
 * InflightEcReconstruct with its start time, the target
 * datanode and the missingContainerIndex.
 */
public class InflightEcReconstruct extends InflightAction {

  private final List<Integer> missingContainerIndexs;
  private final List<DatanodeDetails> targetDataNodes;

  public InflightEcReconstruct(
      final DatanodeDetails datanode,
      final long time,
      final List<Integer> missingContainerIndexs,
      final List<DatanodeDetails> targetDataNodes) {
    super(datanode, time);
    this.missingContainerIndexs = missingContainerIndexs;
    this.targetDataNodes = targetDataNodes;
  }

  public List<DatanodeDetails> getTargetDataNodes() {
    return targetDataNodes;
  }

  public List<Integer> getMissingContainerIndexs() {
    return missingContainerIndexs;
  }
}
