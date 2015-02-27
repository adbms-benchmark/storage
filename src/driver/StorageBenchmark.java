package driver;

import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class StorageBenchmark extends Driver {

    public static void main(String... args) throws Exception {
        SimpleJSAP jsap = getCmdLineConfig(StorageBenchmark.class);
        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }
        setupLogger(config.getBoolean("verbose"), StorageBenchmark.class);

        System.exit(runBenchmark(config));
    }

}
