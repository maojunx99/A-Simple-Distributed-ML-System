package utils;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class EmptyDirectory {
    public static void execute(String path) throws IOException {
        File dir = new File(path);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (!file.delete()) {
                LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "fail to clean sdfsDirectory");
            }
        }
        LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "cleaned sdfsDirectory");
    }
}
