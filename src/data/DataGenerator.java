package data;

import framework.AdbmsSystem;
import java.io.File;
import java.io.IOException;
import util.IO;
import util.ProcessExecutor;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class DataGenerator {

    private final long fileSize;
    private final String dataDir;
    private final String fileName;
    private File file;

    public DataGenerator(long fileSize, String dataDir) {
        this.fileSize = fileSize;
        this.dataDir = dataDir;
        this.fileName = String.valueOf(fileSize);
    }

    public DataGenerator(long fileSize, String dataDir, String fileName) {
        this.fileSize = fileSize;
        this.dataDir = dataDir;
        this.fileName = fileName;
    }

    public String getFilePath() throws IOException {
        if (file == null) {
            generate();
        }
        return file.getAbsolutePath();
    }

    private void generate() throws IOException {
        String filePath = IO.concatPaths(dataDir, fileName);
        file = new File(filePath);
        if (file.exists()) {
            return;
        }
        ProcessExecutor.executeShellCommandRedirect(file.getAbsolutePath(),
                "head", "-c", "" + fileSize, "/dev/urandom");
    }
}
