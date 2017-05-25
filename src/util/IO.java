package util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Dimitar Misev
 */
public class IO {

    public static final String HOME_DIR = System.getenv("HOME");

    public static byte[] readFile(String fileName) throws IOException {
        Path p = FileSystems.getDefault().getPath("", fileName);
        byte[] ret = Files.readAllBytes(p);
        return ret;
    }

    public static File getResultsDir() {
        String resultsDirPath = HOME_DIR + "/results/";
        File resultsDir = new File(resultsDirPath);
        resultsDir.mkdirs();
        return resultsDir;
    }

    public static String concatPaths(String dir, String path) {
        String ret = dir;
        if (ret != null) {
            if (!ret.endsWith(File.separator)) {
                ret += File.separator;
            }
            ret += path;
        }
        return ret;
    }

    public static boolean deleteFile(String fileName) {
        File f = new File(fileName);
        return f.delete();
    }

    public static boolean fileExists(String filePath) {
        File f = new File(filePath);
        return f.exists();
    }

    public static void appendLineToFile(String filePath, String line) throws IOException {
        try (PrintWriter printWriter = new PrintWriter(new FileWriter(filePath, true))) {
            printWriter.println(line);
        }
    }
}
