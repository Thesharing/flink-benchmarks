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
