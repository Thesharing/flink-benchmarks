package org.apache.flink.runtime.benchmark.deploying;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
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

import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.waitForListFulfilled;

public class DeploySinkTasksInBatchJobBenchmark extends DeployTaskBenchmarkBase {

	ExecutionVertex[] vertices;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + DeploySinkTasksInBatchJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setupIteration() throws Exception {
		createAndSetupExecutionGraph(DistributionPattern.ALL_TO_ALL,
									 ResultPartitionType.BLOCKING,
									 ScheduleMode.LAZY_FROM_SOURCES,
									 ExecutionMode.BATCH);

		JobVertex source = jobVertices.get(0);

		for (ExecutionVertex ev : executionGraph.getJobVertex(source.getID()).getTaskVertices()) {
			Execution execution = ev.getCurrentExecutionAttempt();
			execution.deploy();
		}

		JobVertex sink = jobVertices.get(1);

		vertices = executionGraph.getJobVertex(sink.getID()).getTaskVertices();
	}

	@TearDown(Level.Iteration)
	public void teardownIteration() throws Exception {
		waitForListFulfilled(taskDeploymentDescriptors, PARALLELISM * 2, 1000L);
		clearVariables();
		vertices = null;
		System.gc();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void deploySinkTasks() throws Exception {
		for (ExecutionVertex ev : vertices) {
			Execution execution = ev.getCurrentExecutionAttempt();
			execution.deploy();
		}
	}
}
