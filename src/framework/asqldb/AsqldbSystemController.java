package framework.asqldb;

import framework.rasdaman.RasdamanSystemController;
import java.io.IOException;
import util.Pair;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbSystemController extends RasdamanSystemController {

    public AsqldbSystemController(String propertiesPath) throws IOException {
        super(propertiesPath);
        systemName = "ASQLDB";
    }

    @Override
    public Pair<String, String> createRasdamanType(int noOfDimensions, String typeType) throws Exception {
        throw new UnsupportedOperationException("Default rasdaman datatypes (1-3D) only supported.");
    }

    @Override
    public void deleteRasdamanType(String mddTypeName, String setTypeName) {
        throw new UnsupportedOperationException("Default rasdaman datatypes (1-3D) only supported.");
    }

    @Override
    public void restartSystem() throws Exception {
        // nop
    }

}
