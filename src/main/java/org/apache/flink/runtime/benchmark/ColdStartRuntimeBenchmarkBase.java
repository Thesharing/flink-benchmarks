package org.apache.flink.runtime.benchmark;


import org.openjdk.jmh.annotations.Fork;

@Fork(value = 10, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false",
		"-Dcom.sun.management.jmxremote.ssl"
})
public class ColdStartRuntimeBenchmarkBase extends RuntimeBenchmarkBase {

}
