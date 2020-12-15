package org.apache.flink.runtime.benchmark.failover;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;

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

import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.verifyListSize;

public class RegionToRestartInStreamingJobBenchmark extends FailoverBenchmarkBase {

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + RegionToRestartInStreamingJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setupIteration() {
		initRestartPipelinedRegionFailoverStrategy(ResultPartitionState.CREATED, ResultPartitionType.PIPELINED_BOUNDED);
		for (TestingSchedulingExecutionVertex vertex : source) {
			vertex.setState(ExecutionState.RUNNING);
		}
		for (TestingSchedulingExecutionVertex vertex : sink) {
			vertex.setState(ExecutionState.RUNNING);
		}
		strategy = new RestartPipelinedRegionFailoverStrategy(schedulingTopology);
	}

	@TearDown(Level.Iteration)
	public void teardownIteration() {
		verifyListSize(tasks, PARALLELISM * 2);
		clearVariables();
		System.gc();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void calculateRegionToRestart() {
		tasks = strategy.getTasksNeedingRestart(source.get(0).getId(), new Exception("For test."));
	}
}
