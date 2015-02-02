package framework.asqldb;

import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import framework.rasdaman.RasdamanSystem;
import java.io.IOException;

import util.Pair;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbSystem extends RasdamanSystem {

    public AsqldbSystem(String propertiesPath) throws IOException {
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
//        AsqldbConnection.close();
        // nop
//        AsqldbConnection.open(connContext.getUrl());
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new AsqldbQueryGenerator(benchmarkContext);
    }

}