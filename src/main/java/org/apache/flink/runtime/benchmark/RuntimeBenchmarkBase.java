package org.apache.flink.runtime.benchmark;

import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class RuntimeBenchmarkBase {

	@Param({"8000"})
	public int PARALLELISM;

	public final static long TIMEOUT = 300_000L;

	@TearDown
	public void teardown() {
		TestingUtils.defaultExecutor().shutdownNow();
	}
}
