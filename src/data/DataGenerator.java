package data;

import framework.AdbmsSystem;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class DataGenerator {
    public static final String FILE_PREFIX = "benchmark_data";

    private final long fileSize;
    private File file;

    public DataGenerator(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getFilePath() throws IOException {
        if (file == null) {
            generate();
        }
        return file.getAbsolutePath();
    }

    private void generate() throws IOException {
        file = new File("/e/data/" + fileSize);
        if (file.exists()) {
//            long currSize = file.length();
//            if (currSize > fileSize) {
//                AdbmsSystem.executeShellCommandRedirect(file.getAbsolutePath(),
//                        "truncate", "-s", "" + fileSize);
//            } else {
//                AdbmsSystem.executeShellCommand(
//                        "/bin/sh", "-c", "head -c " + (fileSize - currSize) + " /dev/urandom | tee -a " + file.getAbsolutePath());
//            }
            return;
        }
        AdbmsSystem.executeShellCommandRedirect(file.getAbsolutePath(),
                "head", "-c", "" + fileSize, "/dev/urandom");
    }

    public static void main(String[] args) {
        DataGenerator gen = new DataGenerator(10);

    }
}
