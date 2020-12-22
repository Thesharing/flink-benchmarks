package org.apache.flink.runtime.benchmark.deploying;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.benchmark.ColdStartRuntimeBenchmarkBase;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createExecutionGraph;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createJobGraph;

public class DeployTaskBenchmarkBase extends ColdStartRuntimeBenchmarkBase {

	List<JobVertex> jobVertices;
	ExecutionGraph executionGraph;
	BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors;

	public void createAndSetupExecutionGraph(
			DistributionPattern distributionPattern,
			ResultPartitionType resultPartitionType,
			ScheduleMode scheduleMode,
			ExecutionMode executionMode) throws Exception {

		jobVertices = createDefaultJobVertices(
				PARALLELISM,
				distributionPattern,
				resultPartitionType);
		final JobGraph jobGraph = createJobGraph(
				jobVertices,
				scheduleMode,
				executionMode);

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
				execution.registerProducedPartitions(slot.getTaskManagerLocation()).get();
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
