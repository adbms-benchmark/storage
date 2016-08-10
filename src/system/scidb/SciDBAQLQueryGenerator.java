package system.scidb;

import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.BenchmarkSession;

import java.text.MessageFormat;
import java.util.List;

import util.Pair;

/**
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBAQLQueryGenerator extends QueryGenerator {

    public SciDBAQLQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public Benchmark getStorageBenchmark() {

        Benchmark queries = new Benchmark();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
            queries.add(BenchmarkQuery.size(generateSciDBQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
            queries.add(BenchmarkQuery.position(generateSciDBQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
            queries.add(BenchmarkQuery.shape(generateSciDBQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            queries.add(BenchmarkQuery.multipleSelect(generateMultiDomainQuery(multiAccessDomains.getFirst(), multiAccessDomains.getSecond()), benchmarkContext.getArrayDimensionality()));
        }
        
        List<Pair<Long, Long>> middlePointQueryDomain = queryDomainGenerator.getMiddlePointQueryDomain();
        queries.add(BenchmarkQuery.middlePoint(generateSciDBQuery(middlePointQueryDomain), benchmarkContext.getArrayDimensionality()));

        return queries;
    }

    private String generateSciDBQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT * FROM consume((SELECT a FROM {0} WHERE {1}))", benchmarkContext.getArrayName(), convertToSciDBDomain(domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT * FROM consume((SELECT count(a) FROM {0} WHERE {1}))", benchmarkContext.getArrayName(), convertToSciDBDomain(domain1, domain2));
    }

    public static String convertToSciDBDomain(List<Pair<Long, Long>> domain) {
        StringBuilder scidbDomain = new StringBuilder();

        boolean isFirst = true;
        int i = 0;
        for (Pair<Long, Long> axisDomain : domain) {
            if (!isFirst) {
                scidbDomain.append(" AND ");
            }

            scidbDomain.append("axis");
            scidbDomain.append(i);
            scidbDomain.append(">=");
            scidbDomain.append(axisDomain.getFirst());
            scidbDomain.append(" AND ");

            scidbDomain.append("axis");
            scidbDomain.append(i);
            scidbDomain.append("<=");
            scidbDomain.append(axisDomain.getSecond());

            isFirst = false;
            ++i;
        }

        return scidbDomain.toString();
    }

    public static String convertToSciDBDomain(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        StringBuilder scidbDomain = new StringBuilder();
        scidbDomain.append("(");
        scidbDomain.append(convertToSciDBDomain(domain1));
        scidbDomain.append(")");

        scidbDomain.append(" OR ");

        scidbDomain.append("(");
        scidbDomain.append(convertToSciDBDomain(domain2));
        scidbDomain.append(")");

        return scidbDomain.toString();
    }
}
