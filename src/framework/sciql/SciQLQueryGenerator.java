package framework.sciql;

import data.QueryDomainGenerator;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.text.MessageFormat;
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

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
            queries.add(generateSciQLQuery(queryDomain));
        }

        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
            queries.add(generateSciQLQuery(queryDomain));
        }

        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
            queries.add(generateSciQLQuery(queryDomain));
        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            queries.add(generateMultiDomainQuery(multiAccessDomains.getFirst(), multiAccessDomains.getSecond()));
        }

        return queries;
    }

    private String generateSciQLQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT {0} FROM {0} WHERE {1}", benchContext.getCollName1(), convertToSciQLDomain(domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT count(*) FROM {0} WHERE {1}", benchContext.getCollName1(), convertToSciQLDomain(domain1, domain2));
    }

    public static String convertToSciQLDomain(List<Pair<Long, Long>> domain) {
        StringBuilder ret = new StringBuilder();

        int i = 0;
        for (Pair<Long, Long> axisDomain : domain) {
            if (i > 0) {
                ret.append(" AND ");
            }

            ret.append("axis");
            ret.append(i);
            ret.append(" >= ");
            ret.append(axisDomain.getFirst());
            ret.append(" AND ");

            ret.append("axis");
            ret.append(i);
            ret.append("<=");
            ret.append(axisDomain.getSecond());

            ++i;
        }

        return ret.toString();
    }

    public static String convertToSciQLDomain(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        StringBuilder ret = new StringBuilder();
        ret.append("(");
        ret.append(convertToSciQLDomain(domain1));
        ret.append(")");

        ret.append(" OR ");

        ret.append("(");
        ret.append(convertToSciQLDomain(domain2));
        ret.append(")");

        return ret.toString();
    }

//    public static String convertToSciqlDomain(List<Pair<Long, Long>> domain) {
//        StringBuilder ret = new StringBuilder();
//        for (Pair<Long, Long> axisDomain : domain) {
//            ret.append('[').append(axisDomain.getFirst()).append(':').append(axisDomain.getSecond()).append(']');
//        }
//        return ret.toString();
//    }
}