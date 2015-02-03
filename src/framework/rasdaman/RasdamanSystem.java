package framework.rasdaman;

import framework.AdbmsSystem;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;
import util.Pair;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class RasdamanSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(RasdamanSystem.class);

    private static final String RASDL_BIN_KEY = "bin.rasdl";

    private final String rasdlCommand;

    public RasdamanSystem(String propertiesPath) throws IOException {
        super(propertiesPath, RASDAMAN_SYSTEM_NAME);

        String binDir = IO.concatPaths(installDir, "bin");
        String rasdlBin = getValue(RASDL_BIN_KEY);

        this.rasdlCommand = IO.concatPaths(binDir, rasdlBin);
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, startBin)};
        this.stopCommand = new String[]{IO.concatPaths(binDir, stopBin)};
    }

    @Override
    public void restartSystem() throws Exception {
        log.debug("restarting " + systemName);
        if (executeShellCommand(stopCommand) != 0) {
            throw new Exception("Failed to stop the system.");
        }

        if (executeShellCommand(startCommand) != 0) {
            throw new Exception("Failed to start the system.");
        }
    }

    public Pair<String, String> createRasdamanType(int noOfDimensions, String typeType) throws Exception {

        String mddTypeName = MessageFormat.format("B_MDD_{0}_{1}", typeType, noOfDimensions);
        String setTypeName = MessageFormat.format("B_SET_{0}_{1}", typeType, noOfDimensions);

        String mddTypeDefinition = MessageFormat.format("typedef marray <{0}, {1}> {2};", typeType, noOfDimensions, mddTypeName);
        String setTypeDefinition = MessageFormat.format("typedef set<{0}> {1};", mddTypeName, setTypeName);

        File typeFile = File.createTempFile("rasdaman_type", null);
        typeFile.deleteOnExit();

        try (PrintWriter pr = new PrintWriter(typeFile)) {
            pr.println(mddTypeDefinition);
            pr.println(setTypeDefinition);
        }

        if (executeShellCommand(rasdlCommand, "--insert", "--read", typeFile.getAbsolutePath()) != 0) {

        }

        return Pair.of(mddTypeName, setTypeName);
    }

    public void deleteRasdamanType(String mddTypeName, String setTypeName) {

        if (executeShellCommand(rasdlCommand, "--delsettype", setTypeName) != 0) {
            System.out.printf("Faild to delete set type");
        }

        if (executeShellCommand(rasdlCommand, "--delmddtype", mddTypeName) != 0) {
            System.out.printf("Failed to delete mdd type");
        }

    }

    public String getRasdlCommand() {
        return rasdlCommand;
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new RasdamanQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new RasdamanQueryExecutor(benchmarkContext, this);
    }

}
