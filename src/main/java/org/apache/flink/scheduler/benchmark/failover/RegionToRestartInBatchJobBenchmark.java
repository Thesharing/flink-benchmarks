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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.scheduler.benchmark.JobConfiguration;

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

import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.deployTasks;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.transitionTaskStatus;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.verifyListSize;


public class RegionToRestartInBatchJobBenchmark extends FailoverBenchmarkBase {
	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + RegionToRestartInBatchJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Trial)
	public void setupIteration() throws Exception {
		createRestartPipelinedRegionFailoverStrategy(JobConfiguration.BATCH);
		TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();
		deployTasks(executionGraph, source.getID(), slotBuilder, false);
		transitionTaskStatus(executionGraph, source.getID(), ExecutionState.FINISHED);
		if (!ExecutionState.DEPLOYING.equals(executionGraph.getJobVertex(sink.getID()).getTaskVertices()[0].getExecutionState())) {
			deployTasks(executionGraph, sink.getID(), slotBuilder, false);
		}
		transitionTaskStatus(executionGraph, sink.getID(), ExecutionState.RUNNING);
	}

	@TearDown(Level.Trial)
	public void teardownIteration() {
		verifyListSize(tasks, PARALLELISM + 1);
		clearVariables();
		System.gc();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void calculateRegionToRestart() {
		tasks = strategy.getTasksNeedingRestart(
				executionGraph.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
				new Exception("For test."));
	}
}
