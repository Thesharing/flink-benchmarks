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

package org.apache.flink.scheduler.benchmark.scheduling;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;
import org.apache.flink.scheduler.benchmark.ColdStartSchedulerBenchmarkBase;
import org.apache.flink.scheduler.benchmark.JobConfiguration;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils;

import java.util.List;


public class SchedulingBenchmarkBase extends ColdStartSchedulerBenchmarkBase {

	TestingSchedulerOperations schedulerOperations;
	SchedulingTopology schedulingTopology;

	public void initSchedulingTopology(JobConfiguration jobConfiguration) throws Exception {

		schedulerOperations = new TestingSchedulerOperations();

		final List<JobVertex> jobVertices = SchedulerBenchmarkUtils.createDefaultJobVertices(
				PARALLELISM,
				jobConfiguration.distributionPattern,
				jobConfiguration.resultPartitionType);

		final JobGraph jobGraph = SchedulerBenchmarkUtils.createJobGraph(
				jobVertices,
				jobConfiguration.scheduleMode,
				jobConfiguration.executionMode);

		final SlotProvider slotProvider = new SimpleSlotProvider(2 * PARALLELISM);

		final ExecutionGraph eg = SchedulerBenchmarkUtils.createExecutionGraph(jobGraph, slotProvider);

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		schedulingTopology = eg.getSchedulingTopology();
	}

	public void clearVariables() {
		schedulerOperations = null;
		schedulingTopology = null;
	}
}
