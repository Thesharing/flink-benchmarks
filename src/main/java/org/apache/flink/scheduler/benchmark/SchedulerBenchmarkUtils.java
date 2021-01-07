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

package org.apache.flink.scheduler.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.DummyJobInformation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.NoOpExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.RegionPartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Utilities for runtime benchmarks.
 */
public class SchedulerBenchmarkUtils {

	public static List<JobVertex> createDefaultJobVertices(
			int parallelism,
			DistributionPattern distributionPattern,
			ResultPartitionType resultPartitionType) {

		List<JobVertex> jobVertices = new ArrayList<>();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(parallelism);
		jobVertices.add(source);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		jobVertices.add(sink);

		sink.connectNewDataSetAsInput(source, distributionPattern, resultPartitionType);

		return jobVertices;
	}

	public static JobGraph createJobGraph(
			List<JobVertex> jobVertices,
			ScheduleMode scheduleMode,
			ExecutionMode executionMode) throws IOException {

		final JobGraph jobGraph = new JobGraph(jobVertices.toArray(new JobVertex[0]));

		jobGraph.setScheduleMode(scheduleMode);
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setExecutionMode(executionMode);
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	public static ExecutionGraph createExecutionGraph(
			JobGraph jobGraph,
			SlotProvider slotProvider) throws IOException {

		final JobInformation jobInformation = new DummyJobInformation(
				jobGraph.getJobID(),
				jobGraph.getName());

		final ClassLoader classLoader = ExecutionGraph.class.getClassLoader();
		return new ExecutionGraph(
				jobInformation,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				AkkaUtils.getDefaultTimeout(),
				new NoRestartStrategy(),
				JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue(),
				new RestartAllStrategy.Factory(),
				slotProvider,
				classLoader,
				VoidBlobWriter.getInstance(),
				Time.seconds(10L),
				new RegionPartitionReleaseStrategy.Factory(),
				NettyShuffleMaster.INSTANCE,
				NoOpJobMasterPartitionTracker.INSTANCE,
				jobGraph.getScheduleMode(),
				NoOpExecutionDeploymentListener.INSTANCE,
				(execution, newState) -> {
				},
				System.currentTimeMillis());
	}

	public static void waitForListFulfilled(
			Collection<?> list,
			int length,
			long maxWaitMillis) throws TimeoutException {

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(maxWaitMillis));
		final Predicate<Collection<?>> predicate = (Collection<?> l) -> l.size() == length;
		boolean predicateResult;

		do {
			predicateResult = predicate.test(list);

			if (!predicateResult) {
				try {
					Thread.sleep(2L);
				} catch (InterruptedException ignored) {
					Thread.currentThread().interrupt();
				}
			}
		} while (!predicateResult && deadline.hasTimeLeft());

		if (!predicateResult) {
			throw new TimeoutException(String.format(
					"List no fulfilled in time, expected %d, actual %d.",
					length,
					list.size()));
		}
	}

	public static void verifyListSize(
			Collection<?> list,
			int length) {
		if (list.size() < length) {
			throw new RuntimeException(String.format(
					"Size of the list mismatch, expected %d, actual %d.",
					length,
					list.size()));
		}
	}

	public static void deployTasks(
			ExecutionGraph executionGraph,
			JobVertexID jobVertexID,
			TestingLogicalSlotBuilder slotBuilder,
			boolean sendScheduleOrUpdateConsumersMessage) throws Exception {

		for (ExecutionVertex vertex : executionGraph.getJobVertex(jobVertexID).getTaskVertices()) {
			LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
			vertex.getCurrentExecutionAttempt()
					.registerProducedPartitions(
							slot.getTaskManagerLocation(),
							sendScheduleOrUpdateConsumersMessage).get();
			vertex.deployToSlot(slot);
		}
	}

	public static void deployAllTasks(
			ExecutionGraph executionGraph,
			TestingLogicalSlotBuilder slotBuilder) throws Exception {

		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
			vertex.getCurrentExecutionAttempt().registerProducedPartitions(slot.getTaskManagerLocation(), true).get();
			vertex.deployToSlot(slot);
		}
	}

	public static void transitionTaskStatus(
			ExecutionGraph executionGraph,
			JobVertexID jobVertexID,
			ExecutionState state) {

		for (ExecutionVertex vertex : executionGraph
				.getJobVertex(jobVertexID)
				.getTaskVertices()) {
			executionGraph.updateState(new TaskExecutionStateTransition(new TaskExecutionState(
					executionGraph.getJobID(),
					vertex.getCurrentExecutionAttempt().getAttemptId(),
					state)));
		}
	}

	public static void transitionAllTaskStatus(
			ExecutionGraph executionGraph,
			ExecutionState state) {
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			executionGraph.updateState(new TaskExecutionStateTransition(new TaskExecutionState(
					executionGraph.getJobID(),
					vertex.getCurrentExecutionAttempt().getAttemptId(),
					state)));
		}
	}

	public static void transitionTaskStatus(
			DefaultScheduler scheduler,
			AccessExecutionJobVertex vertex,
			int subtask,
			ExecutionState executionState) {

		final ExecutionAttemptID attemptId = vertex.getTaskVertices()[subtask]
				.getCurrentExecutionAttempt()
				.getAttemptId();
		scheduler.updateTaskExecutionState(
				new TaskExecutionState(
						scheduler.getExecutionGraph().getJobID(),
						attemptId,
						executionState));
	}
}
