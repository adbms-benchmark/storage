package framework.context;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Dimitar Misev
 * @author George Merticariu
 */
public abstract class Context {

    private final Properties properties;
    private static final long INVALID_LONG_VALUE = -1l;
    private static final int INVALID_INT_VALUE = -1;

    public Context() {
        properties = new Properties();
    }

    public Context(String propertiesPath) throws IOException {
        this();
        InputStream propertiesFileAsStream = Context.class.getResourceAsStream(propertiesPath);
        properties.load(propertiesFileAsStream);
    }

    public String getValue(final String key) {
        String ret = properties.getProperty(key);
        if (ret == null) {
            ret = "";
        }
        return ret;
    }

    public long getValueLong(final String key) {
        try {
            return Long.parseLong(getValue(key));
        } catch (Exception ex) {
            return INVALID_LONG_VALUE;
        }
    }

    public int getValueInteger(final String key) {
        try {
            return Integer.parseInt(getValue(key));
        } catch (Exception ex) {
            return INVALID_INT_VALUE;
        }
    }


}
