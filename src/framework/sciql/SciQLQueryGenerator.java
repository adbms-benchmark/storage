package framework.sciql;

import data.QueryDomainGenerator;
import framework.context.BenchmarkContext;
import framework.QueryGenerator;
import framework.context.TableContext;
import java.util.ArrayList;
import java.util.List;
import util.Pair;

/**
 *
 * @author Dimitar Misev
 */
public class SciQLQueryGenerator extends QueryGenerator {

    private final QueryDomainGenerator queryDomainGenerator;
    private final BenchmarkContext benchContext;

    public SciQLQueryGenerator(BenchmarkContext benchContext, int noOfDimensions, int noOfQueries) {
        this.queryDomainGenerator = new QueryDomainGenerator(benchContext, noOfDimensions, noOfQueries);
        this.benchContext = benchContext;
    }

    @Override
    public List<String> getBenchmarkQueries() {
        List<String> queries = new ArrayList<>();
        for (TableContext tableContext : BenchmarkContext.dataSizes) {
            String query = "select count(*) from " + tableContext.sciqlTable1 + " as a, "
                    + tableContext.sciqlTable2 + " as b where a.x = b.x and a.y = b.y and a.intensity >= 30 and b.intensity < 30";
            queries.add(query);
        }
        return queries;
    }

    public static String convertToSciqlDomain(List<Pair<Long, Long>> domain) {
        StringBuilder ret = new StringBuilder();
        for (Pair<Long, Long> axisDomain : domain) {
            ret.append('[').append(axisDomain.getFirst()).append(':').append(axisDomain.getSecond()).append(']');
        }
        return ret.toString();
    }
}