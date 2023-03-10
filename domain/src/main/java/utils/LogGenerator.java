package utils;

import core.ProcessStatus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class LogGenerator {
    static FileOutputStream out;
    static File file = new File("logging.log");

    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-hh:mm:ss ")
            .withZone(ZoneId.systemDefault());

    static {
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            out = new FileOutputStream(file, true);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public LogGenerator() throws IOException {
    }

    public static void loggingStatus(LogType logType, String hostName, String timestamp, ProcessStatus status) throws IOException {
        String str = formatter.format(Instant.now()) + "[" + logType + "] " + hostName + "@" + timestamp;
        if (logType == LogType.UPDATE) {
            str += " to " + status;
        }
        str += "\n";
        out.write(str.getBytes(StandardCharsets.UTF_8));
    }

    public static void loggingCrashInfo(LogType logType, String informHost, String informTimestamp, String hostName, String timestamp) throws IOException {
        String str = formatter.format(Instant.now()) + "[" + logType + "] " + informHost + "@" + informTimestamp
                + " detected a crash on " + hostName + "@" + timestamp + "\n";
        out.write(str.getBytes(StandardCharsets.UTF_8));
    }

    public static void loggingTimestamp(String hostName, String oldTimestamp, String newTimestamp) throws IOException {
        String str = formatter.format(Instant.now()) + "[" + LogType.UPDATE + "] " + hostName + "@" + oldTimestamp
                + " is updated to " + hostName + "@" + newTimestamp + "\n";
        out.write(str.getBytes(StandardCharsets.UTF_8));
    }

    public static void loggingInfo(LogType logType, String message) throws IOException {
        String str = formatter.format(Instant.now()) + "[" + logType + "] " + message + "\n";
        System.out.println("[" + logType + "] " + message);
        out.write(str.getBytes(StandardCharsets.UTF_8));
    }

    public enum LogType {
        JOIN,
        LEAVE,
        CRASH,
        UPDATE,
        INFO,
        ERROR,
        WARNING,
        RECEIVING,
        SEND
    }
}
