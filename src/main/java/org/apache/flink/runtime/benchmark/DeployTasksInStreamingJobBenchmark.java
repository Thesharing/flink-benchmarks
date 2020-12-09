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
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.ExecutorUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

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
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.startScheduling;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.waitForAllTaskSubmitted;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class DeployTasksInStreamingJobBenchmark extends RuntimeBenchmarkBase {

	DefaultScheduler scheduler;
	BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors;
	ExecutorService executor;
	ScheduledExecutorService scheduledExecutorService;


	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + DeployTasksInStreamingJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setup() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
				PARALLELISM,
				DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = createJobGraph(
				jobVertices,
				ScheduleMode.EAGER,
				ExecutionMode.PIPELINED);

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

	@TearDown(Level.Iteration)
	public void teardown() {
		scheduler.suspend(new Exception("End of test."));

		if (scheduledExecutorService != null) {
			ExecutorUtils.gracefulShutdown(1000, TimeUnit.MILLISECONDS, scheduledExecutorService);
		}

		if (executor != null) {
			ExecutorUtils.gracefulShutdown(1000, TimeUnit.MILLISECONDS, executor);
		}
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	@Fork(value = 10, jvmArgsAppend = {
			"-Djava.rmi.server.hostname=127.0.0.1",
			"-Dcom.sun.management.jmxremote.authenticate=false",
			"-Dcom.sun.management.jmxremote.ssl=false",
			"-Dcom.sun.management.jmxremote.ssl"
	})
	public void deployTaskInStreamingJob() throws Exception {
		startScheduling(scheduler);
		waitForAllTaskSubmitted(taskDeploymentDescriptors, PARALLELISM * 2, TIMEOUT);
	}
}
