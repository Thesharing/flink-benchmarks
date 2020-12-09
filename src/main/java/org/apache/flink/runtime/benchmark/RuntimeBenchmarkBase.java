package org.apache.flink.runtime.benchmark;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class RuntimeBenchmarkBase {

	@Param({"1000", "2000", "4000", "8000"})
	public int PARALLELISM;

	public final static long TIMEOUT = 300_000L;
}
