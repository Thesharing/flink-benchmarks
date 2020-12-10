package org.apache.flink.runtime.benchmark;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ScheduleMode;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.startScheduling;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.waitForAllTaskSubmitted;

public class DeploySourceTasksInBatchJobBenchmark extends SchedulerBenchmarkBase {

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + DeploySourceTasksInBatchJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setup() throws Exception {
		createAndSetupScheduler(DistributionPattern.ALL_TO_ALL,
								ResultPartitionType.BLOCKING,
								ScheduleMode.LAZY_FROM_SOURCES,
								ExecutionMode.BATCH);
	}

	@TearDown(Level.Iteration)
	public void teardown() {
		shutdownScheduler();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void deploySourceTasksInBatchJob() throws Exception {
		startScheduling(scheduler);
		waitForAllTaskSubmitted(taskDeploymentDescriptors, PARALLELISM, TIMEOUT);
	}
}
