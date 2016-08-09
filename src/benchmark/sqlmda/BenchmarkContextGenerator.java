package benchmark.sqlmda;

import benchmark.BenchmarkContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Generate benchmark contexts starting from an initial context.
 *
 * @author Dimitar Misev
 */
public class BenchmarkContextGenerator {

    public static List<BenchmarkContext> generate(BenchmarkContext initial) {
        if (initial.isSqlMdaBenchmark()) {
            return generateSqlMda(initial);
        } else if (initial.isStorageBenchmark()) {
            return Collections.singletonList(initial);
        } else {
            return Collections.singletonList(initial);
        }
    }

    protected static List<BenchmarkContext> generateSqlMda(BenchmarkContext initial) {
        List<BenchmarkContext> ret = new ArrayList<>();

        // query 1:
        //SELECT ADD_CELLS(
        // POW(STDDEV_POP(z.image) -
        //     STDDEV_SAMP(d.image), 2
        // )[t($time)]
        //) as costFunction
        //FROM Dynamics AS d, Zygotic AS z,
        //     embryo_blastoderm AS eb
        //WHERE eb.zygotic_name = '$zygoticName' AND
        //      eb.id = z.id AND d.id = eb.id
        initial.setArrayDimensionality(2);
        ((SqlMdaBenchmarkContext)initial).setBaseType("TINYINT");
        initial.updateArrayName();
        BenchmarkContext costFunctionDynamic = initial.clone();
        costFunctionDynamic.setArrayName(initial.getArrayName() + "_Dynamic");
        BenchmarkContext costFunctionZygotic = initial.clone();
        costFunctionZygotic.setArrayName(initial.getArrayName() + "_Zygotic");
        BenchmarkContextJoin costFunction = new BenchmarkContextJoin(costFunctionDynamic, costFunctionZygotic);
        ret.add(costFunction);

        return ret;
    }
}
