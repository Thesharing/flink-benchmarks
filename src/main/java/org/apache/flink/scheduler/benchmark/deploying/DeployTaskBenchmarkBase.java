/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.scheduler.benchmark.deploying;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.scheduler.benchmark.ColdStartSchedulerBenchmarkBase;
import org.apache.flink.scheduler.benchmark.JobConfiguration;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createExecutionGraph;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createJobGraph;

public class DeployTaskBenchmarkBase extends ColdStartSchedulerBenchmarkBase {

	List<JobVertex> jobVertices;
	ExecutionGraph executionGraph;
	BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors;

	public void createAndSetupExecutionGraph(JobConfiguration jobConfiguration) throws Exception {

		jobVertices = createDefaultJobVertices(
				PARALLELISM,
				jobConfiguration.distributionPattern,
				jobConfiguration.resultPartitionType);
		final JobGraph jobGraph = createJobGraph(
				jobVertices,
				jobConfiguration.scheduleMode,
				jobConfiguration.executionMode);

		taskDeploymentDescriptors = new ArrayBlockingQueue<>(PARALLELISM * 2);
		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);

		final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

		executionGraph = createExecutionGraph(jobGraph, slotProvider);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		TestingLogicalSlotBuilder slotBuilder =
				new TestingLogicalSlotBuilder().setTaskManagerGateway(taskManagerGateway);

		for (ExecutionJobVertex ejv : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				Execution execution = ev.getCurrentExecutionAttempt();
				LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
				execution.registerProducedPartitions(slot.getTaskManagerLocation(), true).get();
				if (!execution.tryAssignResource(slot)) {
					throw new RuntimeException("Error when assigning slot to execution.");
				}
			}
		}
	}

	public void clearVariables() {
		jobVertices = null;
		executionGraph = null;
		taskDeploymentDescriptors = null;
	}
}
