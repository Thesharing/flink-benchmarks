package org.apache.flink.runtime.benchmark.failover;

import org.apache.flink.runtime.benchmark.RuntimeBenchmarkBase;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;

import java.util.List;
import java.util.Set;

public class FailoverBenchmarkBase extends RuntimeBenchmarkBase {
	RestartPipelinedRegionFailoverStrategy strategy;
	TestingSchedulingTopology schedulingTopology;

	List<TestingSchedulingExecutionVertex> source;
	List<TestingSchedulingExecutionVertex> sink;

	Set<ExecutionVertexID> tasks;

	public void initRestartPipelinedRegionFailoverStrategy(
			ResultPartitionState resultPartitionState,
			ResultPartitionType resultPartitionType) {

		schedulingTopology = new TestingSchedulingTopology();
		source = schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();
		sink = schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();

		schedulingTopology
				.connectAllToAll(source, sink)
				.withResultPartitionState(resultPartitionState)
				.withResultPartitionType(resultPartitionType)
				.finish();
	}

	public void clearVariables() {
		strategy = null;
		schedulingTopology = null;
		source = null;
		sink = null;
		tasks = null;
	}
}
