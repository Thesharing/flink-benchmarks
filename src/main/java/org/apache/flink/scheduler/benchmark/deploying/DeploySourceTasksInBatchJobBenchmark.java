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

package org.apache.flink.scheduler.benchmark.deploying;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.scheduler.benchmark.JobConfiguration;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils;

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

public class DeploySourceTasksInBatchJobBenchmark extends DeployTaskBenchmarkBase {

	ExecutionVertex[] vertices;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + DeploySourceTasksInBatchJobBenchmark.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void setupIteration() throws Exception {
		createAndSetupExecutionGraph(JobConfiguration.BATCH);

		JobVertex source = jobVertices.get(0);

		vertices = executionGraph.getJobVertex(source.getID()).getTaskVertices();
	}

	@TearDown(Level.Iteration)
	public void teardownIteration() throws Exception {
		SchedulerBenchmarkUtils.waitForListFulfilled(taskDeploymentDescriptors, PARALLELISM, 1000L);
		clearVariables();
		vertices = null;
		System.gc();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void deploySourceTasks() throws Exception {
		for (ExecutionVertex ev : vertices) {
			Execution execution = ev.getCurrentExecutionAttempt();
			execution.deploy();
		}
	}
}
