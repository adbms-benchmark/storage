package data;

import framework.AdbmsSystem;
import java.io.File;
import java.io.IOException;
import util.IO;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class DataGenerator {
    public static final String FILE_PREFIX = "benchmark_data";

    private final long fileSize;
    private final String dataDir;
    private File file;

    public DataGenerator(long fileSize, String dataDir) {
        this.fileSize = fileSize;
        this.dataDir = dataDir;
    }

    public String getFilePath() throws IOException {
        if (file == null) {
            generate();
        }
        return file.getAbsolutePath();
    }

    private void generate() throws IOException {
        file = new File(IO.concatPaths(dataDir, String.valueOf(fileSize)));
        if (file.exists()) {
            return;
        }
        AdbmsSystem.executeShellCommandRedirect(file.getAbsolutePath(),
                "head", "-c", "" + fileSize, "/dev/urandom");
    }
}
