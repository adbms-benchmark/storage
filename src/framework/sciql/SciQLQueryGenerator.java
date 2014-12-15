package framework.sciql;

import data.QueryDomainGenerator;
import framework.BenchmarkContext;
import framework.QueryGenerator;
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