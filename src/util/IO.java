package util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * @author Dimitar Misev
 */
public class IO {

    public static byte[] readFile(String fileName) throws IOException {
        Path p = FileSystems.getDefault().getPath("", fileName);
        byte[] ret = Files.readAllBytes(p);
        return ret;
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
}
