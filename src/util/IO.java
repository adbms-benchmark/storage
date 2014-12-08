package util;

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
}
