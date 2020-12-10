package org.apache.flink.runtime.benchmark;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.ExecutorUtils;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createJobGraph;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createScheduler;

@State(Scope.Thread)
public class SchedulerBenchmarkBase extends RuntimeBenchmarkBase {

	List<JobVertex> jobVertices;
	JobGraph jobGraph;
	DefaultScheduler scheduler;
	BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors;
	ExecutorService executor;
	ScheduledExecutorService scheduledExecutorService;

	public void createAndSetupScheduler(
			DistributionPattern distributionPattern,
			ResultPartitionType resultPartitionType,
			ScheduleMode scheduleMode,
			ExecutionMode executionMode) throws Exception {

		jobVertices = createDefaultJobVertices(
				PARALLELISM,
				distributionPattern,
				resultPartitionType);

		jobGraph = createJobGraph(
				jobVertices,
				scheduleMode,
				executionMode);

		taskDeploymentDescriptors = new ArrayBlockingQueue<>(PARALLELISM * 2);
		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		taskManagerGateway.setSubmitConsumer(taskDeploymentDescriptors::offer);
		final SlotProvider slotProvider = new SimpleSlotProvider(
				PARALLELISM * 2,
				taskManagerGateway);

		executor = Executors.newSingleThreadExecutor();
		scheduledExecutorService = new DirectScheduledExecutorService();

		scheduler = createScheduler(
				jobGraph,
				slotProvider,
				executor,
				scheduledExecutorService);
	}

	public void shutdownScheduler() {
		scheduler.suspend(new Exception("End of test."));

		// Shutdown the default executor launched by initialization of DefaultSchedulerBuilder
		ExecutorUtils.gracefulShutdown(1000, TimeUnit.MILLISECONDS, TestingUtils.defaultExecutor());

		if (scheduledExecutorService != null) {
			ExecutorUtils.gracefulShutdown(1000, TimeUnit.MILLISECONDS, scheduledExecutorService);
		}

		if (executor != null) {
			ExecutorUtils.gracefulShutdown(1000, TimeUnit.MILLISECONDS, executor);
		}

		jobVertices = null;
		jobGraph = null;
		scheduler = null;
		taskDeploymentDescriptors = null;
		executor = null;
		scheduledExecutorService = null;

		System.gc();
	}
}

