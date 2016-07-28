package framework.scidb;

import data.Benchmark;
import data.BenchmarkQuery;
import data.QueryDomainGenerator;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author George Merticariu
 */
public class SciDBAFLQueryGenerator extends QueryGenerator {

    public SciDBAFLQueryGenerator(BenchmarkContext benchmarkContext) {
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
        return MessageFormat.format("SELECT * FROM consume((SELECT * FROM {0}))", convertToSciDBBetween(benchmarkContext.getArrayName(), domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT * FROM consume((SELECT * FROM {0}))", convertToSciDBDomain(benchmarkContext.getArrayName(), domain1, domain2));
    }

    public static String convertToSciDBBetween(String collectionName, List<Pair<Long, Long>> domain) {
        StringBuilder lowerBounds = new StringBuilder();
        StringBuilder upperBounds = new StringBuilder();

        boolean isFirst = true;
        int i = 0;
        for (Pair<Long, Long> axisDomain : domain) {
            if (!isFirst) {
                lowerBounds.append(",");
                upperBounds.append(",");
            }
            lowerBounds.append(axisDomain.getFirst());
            upperBounds.append(axisDomain.getSecond());

            isFirst = false;
            ++i;
        }

        return String.format("between(%s,%s,%s)", collectionName, lowerBounds.toString(), upperBounds.toString());
    }

    public static String convertToSciDBDomain(String collectionName, List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return String.format("merge(aggregate(%s, count(*)),aggregate(%s, count(*)))", convertToSciDBBetween(collectionName, domain1), convertToSciDBBetween(collectionName, domain2));
    }
}
