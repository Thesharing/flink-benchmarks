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

package org.apache.flink.scheduler.benchmark;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ScheduleMode;

public enum JobConfiguration {

	STREAMING(DistributionPattern.ALL_TO_ALL,
			  ResultPartitionType.PIPELINED,
			  ScheduleMode.EAGER,
			  ExecutionMode.PIPELINED),

	BATCH(DistributionPattern.ALL_TO_ALL,
		  ResultPartitionType.BLOCKING,
		  ScheduleMode.LAZY_FROM_SOURCES,
		  ExecutionMode.BATCH);

	JobConfiguration(
			DistributionPattern distributionPattern,
			ResultPartitionType resultPartitionType,
			ScheduleMode scheduleMode,
			ExecutionMode executionMode) {
		this.distributionPattern = distributionPattern;
		this.resultPartitionType = resultPartitionType;
		this.scheduleMode = scheduleMode;
		this.executionMode = executionMode;
	}

	public final DistributionPattern distributionPattern;

	public final ResultPartitionType resultPartitionType;

	public final ScheduleMode scheduleMode;

	public final ExecutionMode executionMode;

}
