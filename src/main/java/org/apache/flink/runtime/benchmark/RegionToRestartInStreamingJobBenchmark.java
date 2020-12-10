package org.apache.flink.runtime.benchmark;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

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

import java.util.function.Predicate;

import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.startScheduling;
import static org.apache.flink.runtime.benchmark.RuntimeBenchmarkUtils.transitionAllTaskStatus;

public class RegionToRestartInStreamingJobBenchmark extends SchedulerBenchmarkBase {

	JobVertex source;
	JobVertex sink;
	ExecutionVertex ev11;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + RegionToRestartInStreamingJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Trial)
	public void setup() throws Exception {
		createAndSetupScheduler(DistributionPattern.ALL_TO_ALL,
								ResultPartitionType.PIPELINED,
								ScheduleMode.EAGER,
								ExecutionMode.PIPELINED);

		source = jobVertices.get(0);
		sink = jobVertices.get(1);

		startScheduling(scheduler);
	}

	@Setup(Level.Iteration)
	public void setupIteration() throws Exception {
		transitionToRunning();
		ev11 = scheduler.getExecutionJobVertex(source.getID()).getTaskVertices()[0];
	}

	@TearDown(Level.Iteration)
	public void teardownIteration() {
		for (ExecutionVertex ev :
				scheduler.getExecutionJobVertex(source.getID()).getTaskVertices()) {
			ev.suspend();
		}

		for (ExecutionVertex ev : scheduler.getExecutionJobVertex(sink.getID()).getTaskVertices()) {
			ev.suspend();
		}
	}

	@TearDown(Level.Trial)
	public void teardown() {
		shutdownScheduler();
	}

	public void transitionToRunning() throws Exception {
		Predicate<AccessExecution> isDeploying =
				ExecutionGraphTestUtils.isInExecutionState(ExecutionState.DEPLOYING);
		ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
				scheduler.getExecutionGraph(),
				isDeploying,
				TIMEOUT);

		final AccessExecutionJobVertex ejvSource =
				scheduler.requestJob().getAllVertices().get(source.getID());
		transitionAllTaskStatus(scheduler, ejvSource, ExecutionState.RUNNING);
		final AccessExecutionJobVertex ejvSink =
				scheduler.requestJob().getAllVertices().get(sink.getID());
		transitionAllTaskStatus(scheduler, ejvSink, ExecutionState.RUNNING);
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void calculateRegionToRestart() {
		scheduler.updateTaskExecutionState(
				new TaskExecutionState(
						jobGraph.getJobID(),
						ev11.getCurrentExecutionAttempt().getAttemptId(),
						ExecutionState.FAILED));
	}
}
