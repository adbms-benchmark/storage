package benchmark.sqlmda;

import benchmark.caching.*;
import benchmark.BenchmarkContext;

/**
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SqlMdaBenchmarkContext extends BenchmarkContext {

    protected String baseType;
    protected BenchmarkContext join;
    
    public SqlMdaBenchmarkContext(int repeatNumber, String dataDir, int timeout) {
        super(repeatNumber, dataDir, timeout, TYPE_SQLMDA);
        arrayDimensionality = 2;
    }

    public String getBaseType() {
        return baseType;
    }

    public void setBaseType(String baseType) {
        this.baseType = baseType;
    }

    public BenchmarkContext getJoin() {
        return join;
    }

    public void setJoin(BenchmarkContext join) {
        this.join = join;
    }
    
}
