package org.apache.flink.runtime.benchmark;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.scheduler.DefaultScheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class RuntimeBenchmarkBase {
	final static int PARALLELISM = 8000;
	final static long TIMEOUT = 300_000L;

	DefaultScheduler scheduler;
	BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors;
	ExecutorService executor;
	ScheduledExecutorService scheduledExecutorService;
}
