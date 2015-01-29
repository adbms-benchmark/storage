package framework.rasdaman;

import framework.context.ConnectionContext;
import framework.SystemController;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import util.IO;
import util.Pair;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class RasdamanSystemController extends SystemController {

    public static final String KEY_RASDAMAN_HOME = "rasdaman.home";

    protected String rasdlBinary;
    protected String rasqlBinary;
    protected String rasdamanHome;

    public RasdamanSystemController(String propertiesPath, ConnectionContext connContext) throws IOException {
        super(propertiesPath, connContext);
        this.rasdamanHome = getValue(KEY_RASDAMAN_HOME);
        String rasdamanBinDir = IO.concatPaths(rasdamanHome, "bin");
        this.startSystemCommand = new String[]{rasdamanBinDir + "/start_rasdaman.sh"};
        this.stopSystemCommand = new String[]{rasdamanBinDir + "/stop_rasdaman.sh"};
        this.rasqlBinary = rasdamanBinDir + "/rasql";
        this.rasdlBinary = rasdamanBinDir + "/rasdl";
    }

    //TODO-GM: change order of the parameters
    public RasdamanSystemController(String[] startSystemCommand, String[] stopSystemCommand, String rasdlBinary) {
        super(startSystemCommand, stopSystemCommand, "rasdaman");
        this.rasdlBinary = rasdlBinary;
    }

    @Override
    public void restartSystem() throws Exception {
        if (executeShellCommand(stopSystemCommand) != 0) {
            throw new Exception("Failed to stop the system.");
        }

        if (executeShellCommand(startSystemCommand) != 0) {
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

        if (executeShellCommand(rasdlBinary, "--insert", "--read", typeFile.getAbsolutePath()) != 0) {
//            throw new Exception("Failed to create rasdaman type.");
        }

        return Pair.of(mddTypeName, setTypeName);
    }

    public void deleteRasdamanType(String mddTypeName, String setTypeName) {

        if (executeShellCommand(rasdlBinary, "--delsettype", setTypeName) != 0) {
            System.out.printf("Faild to delete set type");
        }

        if (executeShellCommand(rasdlBinary, "--delmddtype", mddTypeName) != 0) {
            System.out.printf("Failed to delete mdd type");
        }

    }

    public String getRasqlBinary() {
        return rasqlBinary;
    }

}
