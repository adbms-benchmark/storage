package data;

import framework.SystemController;
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
        if (file.exists())
            return;
        SystemController.executeShellCommandRedirect(file.getAbsolutePath(),
                "head", "-c", "" + fileSize, "/dev/urandom");
    }
}
