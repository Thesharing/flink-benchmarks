/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

public class BuildExecutionGraphInBatchJobBenchmark extends ColdStartRuntimeBenchmarkBase {

	JobGraph jobGraph;
	ExecutionGraph executionGraph;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + BuildExecutionGraphInBatchJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setupIteration() throws Exception {
		final List<JobVertex> jobVertices = createDefaultJobVertices(
				PARALLELISM,
				DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.BLOCKING);
		jobGraph = createJobGraph(
				jobVertices,
				ScheduleMode.LAZY_FROM_SOURCES,
				ExecutionMode.BATCH);
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
