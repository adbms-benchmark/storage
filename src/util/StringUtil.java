package util;

/**
 * @author Dimitar Misev
 */
public class StringUtil {

    public static String arrayToString(Object[] array) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(array[i]);
        }
        return sb.toString();
    }
}
