package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class LogGenerator {
    static FileOutputStream out;
    static File file = new File("logging.log");

    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-hh:mm:ss ")
            .withZone(ZoneId.systemDefault());

    static {
        try {
            if (!file.exists()){
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

    public static void logging(LogType logType, String hostName, String timestamp) throws IOException {
        String str = formatter.format(Instant.now()) + "["+ logType +"] " + hostName + "@" + timestamp + "\n";
        out.write(str.getBytes(StandardCharsets.UTF_8));
    }
    public static void logging(LogType logType, String informHost, String informTimestamp, String hostName, String timestamp) throws IOException {
        String str = formatter.format(Instant.now()) + "["+ logType +"] " + hostName + "@" + timestamp + " from " + informHost + "@" + timestamp+ "\n";
        out.write(str.getBytes(StandardCharsets.UTF_8));
        out.close();
    }
    public enum LogType{
        JOIN,
        LEAVE,
        CRASH,
        UPDATE,
        FAILURE_INFORM,
        FAILURE_ACK
    }
}
