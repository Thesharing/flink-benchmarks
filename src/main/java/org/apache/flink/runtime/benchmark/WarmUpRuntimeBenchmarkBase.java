package org.apache.flink.runtime.benchmark;

import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false",
		"-Dcom.sun.management.jmxremote.ssl"
})
@Warmup(iterations = 10)
@Measurement(iterations = 5)
public class WarmUpRuntimeBenchmarkBase extends RuntimeBenchmarkBase {
}
