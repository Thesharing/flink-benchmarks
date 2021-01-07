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

package org.apache.flink.scheduler.benchmark.failover;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.scheduler.benchmark.JobConfiguration;
import org.apache.flink.scheduler.benchmark.WarmUpSchedulerBenchmarkBase;

import java.util.List;
import java.util.Set;

import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createExecutionGraph;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createJobGraph;


public class FailoverBenchmarkBase extends WarmUpSchedulerBenchmarkBase {
	ExecutionGraph executionGraph;
	SchedulingTopology schedulingTopology;
	RestartPipelinedRegionFailoverStrategy strategy;

	JobVertex source;
	JobVertex sink;

	Set<ExecutionVertexID> tasks;

	public void createRestartPipelinedRegionFailoverStrategy(JobConfiguration jobConfiguration) throws Exception {

		final List<JobVertex> jobVertices = createDefaultJobVertices(
				PARALLELISM,
				jobConfiguration.distributionPattern,
				jobConfiguration.resultPartitionType);

		final JobGraph jobGraph = createJobGraph(
				jobVertices,
				jobConfiguration.scheduleMode,
				jobConfiguration.executionMode);

		final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

		executionGraph = createExecutionGraph(jobGraph, slotProvider);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		schedulingTopology = executionGraph.getSchedulingTopology();

		source = jobVertices.get(0);
		sink = jobVertices.get(1);

		strategy = new RestartPipelinedRegionFailoverStrategy(schedulingTopology);
	}

	public void clearVariables() {
		strategy = null;
		schedulingTopology = null;
		source = null;
		sink = null;
		tasks = null;
	}
}
