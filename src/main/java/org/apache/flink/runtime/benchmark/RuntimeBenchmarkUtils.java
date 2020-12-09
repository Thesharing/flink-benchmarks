package org.apache.flink.runtime.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

public class RuntimeBenchmarkUtils {

	private static final Logger LOG = LoggerFactory.getLogger(RuntimeBenchmarkUtils.class);
	private static final Time TIMEOUT = Time.seconds(10L);

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

	public static DefaultScheduler createScheduler(
			final JobGraph jobGraph,
			final SlotProvider slotProvider,
			final ExecutorService executor,
			final ScheduledExecutorService scheduledExecutorService) throws Exception {

		final SchedulingStrategyFactory schedulingStrategyFactory =
				jobGraph.getScheduleMode() == ScheduleMode.LAZY_FROM_SOURCES ?
						new LazyFromSourcesSchedulingStrategy.Factory() :
						new EagerSchedulingStrategy.Factory();

		final Configuration configuration = new Configuration();
		final TestRestartBackoffTimeStrategy testRestartBackoffTimeStrategy =
				new TestRestartBackoffTimeStrategy(true, 0);

		final ExecutionVertexVersioner executionVertexVersioner = new ExecutionVertexVersioner();

		return SchedulerTestingUtils.newSchedulerBuilderWithDefaultSlotAllocator(
				jobGraph,
				slotProvider,
				TIMEOUT)
				.setLogger(LOG)
				.setIoExecutor(executor)
				.setJobMasterConfiguration(configuration)
				.setFutureExecutor(scheduledExecutorService)
				.setSchedulingStrategyFactory(schedulingStrategyFactory)
				.setRestartBackoffTimeStrategy(testRestartBackoffTimeStrategy)
				.setExecutionVertexVersioner(executionVertexVersioner)
				.build();
	}

	public static void startScheduling(final SchedulerNG scheduler) {
		scheduler.setMainThreadExecutor(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();
	}

	public static void waitForAllTaskSubmitted(
			BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors,
			int desiredCount,
			long maxWaitMillis) throws TimeoutException {
		// this is a poor implementation - we may want to improve it eventually
		final long deadline =
				maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (taskDeploymentDescriptors.size() < desiredCount && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {
			}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException();
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

	public static void transitionAllTaskStatus(
			DefaultScheduler scheduler,
			AccessExecutionJobVertex vertex,
			ExecutionState executionState) {

		for (AccessExecutionVertex ev : vertex.getTaskVertices()) {
			ExecutionAttemptID attemptId = ev.getCurrentExecutionAttempt().getAttemptId();
			scheduler.updateTaskExecutionState(
					new TaskExecutionState(
							scheduler.getExecutionGraph().getJobID(),
							attemptId,
							executionState));
		}
	}
}
