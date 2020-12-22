package org.apache.flink.runtime.benchmark.scheduling;

import org.apache.flink.runtime.benchmark.ColdStartRuntimeBenchmarkBase;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;

import org.openjdk.jmh.infra.Blackhole;

import java.util.List;

public class SchedulingBenchmarkBase extends ColdStartRuntimeBenchmarkBase {

	TestingSchedulerOperations schedulerOperations;
	TestingSchedulingTopology schedulingTopology;

	List<TestingSchedulingExecutionVertex> source;
	List<TestingSchedulingExecutionVertex> sink;

	public void initSchedulingTopology(
			Blackhole blackhole,
			ResultPartitionState resultPartitionState,
			ResultPartitionType resultPartitionType) {

		schedulerOperations = new TestingSchedulerOperations();
		schedulingTopology = new TestingSchedulingTopology();

		source = schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();
		sink = schedulingTopology.addExecutionVertices().withParallelism(PARALLELISM).finish();

		schedulingTopology
				.connectAllToAll(source, sink)
				.withResultPartitionState(resultPartitionState)
				.withResultPartitionType(resultPartitionType)
				.finish();

		blackhole.consume(schedulingTopology.getPipelinedRegionOfVertex(source.get(0).getId()));
	}

	public void clearVariables() {
		schedulerOperations = null;
		schedulingTopology = null;
		source = null;
		sink = null;
	}
}
