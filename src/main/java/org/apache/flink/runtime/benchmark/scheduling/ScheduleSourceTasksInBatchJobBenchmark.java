package org.apache.flink.runtime.benchmark.scheduling;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;

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

public class ScheduleSourceTasksInBatchJobBenchmark extends SchedulingBenchmarkBase {

	private PipelinedRegionSchedulingStrategy schedulingStrategy;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + ScheduleSourceTasksInBatchJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setupIteration() {
		initSchedulingTopology(ResultPartitionState.CREATED, ResultPartitionType.BLOCKING);
		schedulingStrategy = new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
	}

	@TearDown(Level.Iteration)
	public void teardownIteration() {
		clearVariables();
		schedulingStrategy = null;
		System.gc();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void scheduleTasks() {
		schedulingStrategy.startScheduling();
	}
}
