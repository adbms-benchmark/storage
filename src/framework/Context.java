package framework;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Dimitar Misev
 */
public abstract class Context {
    
    private final Properties properties;
    public static final long INVALID_VALUE = -1;
    
    public Context() {
        properties = new Properties();
    }

    public Context(String propertiesPath) throws FileNotFoundException, IOException {
        this();
        properties.load(new FileInputStream(propertiesPath));
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
            return INVALID_VALUE;
        }
    }

}
