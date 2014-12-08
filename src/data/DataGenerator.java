package data;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 *
 * @author George Merticariu
 */
public class DataGenerator {
    public static final String FILE_PREFIX = "benchmark_data";
    private static final int CHUNK_SIZE = 1024 * 1024;

    private long fileSize;
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
        file = File.createTempFile(FILE_PREFIX, null);
        file.deleteOnExit();

        long fileSize = this.fileSize;

        try (PrintWriter fileWriter = new PrintWriter(file)) {
            while (fileSize > ((long) CHUNK_SIZE)) {
                fileSize -= ((long) CHUNK_SIZE);
                fileWriter.print(getRandomAsciiString(CHUNK_SIZE));
            }
            fileWriter.print(getRandomAsciiString(fileSize));
        }
    }

    private static String getRandomAsciiString(long size) {
        Random random = new Random(System.currentTimeMillis());
        StringBuilder sb = new StringBuilder();
        for (long i = 0; i < size; ++i) {
            sb.append((char) random.nextInt(128));
        }

        return sb.toString();
    }
}
