package org.apache.flink.runtime.benchmark;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 3, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false",
		"-Dcom.sun.management.jmxremote.ssl"
})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class RuntimeBenchmarkBase {

	@Param({"1000", "2000", "4000", "8000"})
	public int PARALLELISM;

	public final static long TIMEOUT = 300_000L;
}
