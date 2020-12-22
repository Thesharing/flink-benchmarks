package org.apache.flink.runtime.benchmark.topology;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.benchmark.ColdStartRuntimeBenchmarkBase;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

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

import java.util.List;

import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createExecutionGraph;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.createJobGraph;

public class BuildExecutionGraphInStreamingJobBenchmark extends ColdStartRuntimeBenchmarkBase {

	JobGraph jobGraph;
	ExecutionGraph executionGraph;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + BuildExecutionGraphInStreamingJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setupIteration() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
				PARALLELISM,
				DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.PIPELINED);
		jobGraph = createJobGraph(
				jobVertices,
				ScheduleMode.EAGER,
				ExecutionMode.PIPELINED);
		final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

		executionGraph = createExecutionGraph(jobGraph, slotProvider);
	}

	@TearDown(Level.Iteration)
	public void teardownIteration() {
		jobGraph = null;
		executionGraph = null;
		System.gc();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void buildTopology() throws Exception {
		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
	}
}
