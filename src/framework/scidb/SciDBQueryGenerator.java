package framework.scidb;

import data.QueryDomainGenerator;
import framework.BenchmarkContext;
import framework.QueryGenerator;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import util.Pair;

/**
 *
 * @author George Merticariu
 */
public class SciDBQueryGenerator extends QueryGenerator {

    private QueryDomainGenerator queryDomainGenerator;

    public SciDBQueryGenerator(long collectionSize, int noOfDimensions, long maxSelectSize, int noOfQueries) {
        queryDomainGenerator = new QueryDomainGenerator(collectionSize, noOfDimensions, maxSelectSize, noOfQueries);
    }

    @Override
    public List<String> getBenchmarkQueries() {

        List<String> queries = new ArrayList<>();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

//        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
//            queries.add(generateSciDBQuery(queryDomain));
//        }
//
//        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
//            queries.add(generateSciDBQuery(queryDomain));
//        }
//
//        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
//            queries.add(generateSciDBQuery(queryDomain));
//        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            queries.add(generateMultiDomainQuery(multiAccessDomains.getFirst(), multiAccessDomains.getSecond()));
        }

        return queries;
    }

    private String generateSciDBQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT {0} FROM {0} WHERE {1}", BenchmarkContext.COLLECTION_NAME, convertToSciDBDomain(domain));
    }

    private static String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT count({0}) FROM {0} WHERE {1} ", BenchmarkContext.COLLECTION_NAME, convertToSciDBDomain(domain1, domain2));
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