package service;

import com.google.protobuf.Timestamp;
import core.Process;
import core.ProcessStatus;

import java.time.Instant;

/**
 * Main class, response to console command
 */
public class Main {
    public static void main(String[] args) {
        Instant time = Instant.now();
        Process process = Process.newBuilder().setAddress("123").setPort(11).setStatus(ProcessStatus.ALIVE)
                .setTimestamp(Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano())).build();
        System.out.println(process);
    }
}
