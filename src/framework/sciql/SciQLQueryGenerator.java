package framework.sciql;

import data.BenchmarkQuery;
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

    public SciQLQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public List<BenchmarkQuery> getBenchmarkQueries() {
        List<BenchmarkQuery> queries = new ArrayList<>();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
            queries.add(BenchmarkQuery.size(generateSciQLQuery(queryDomain), noOfDimensions));
        }

        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
            queries.add(BenchmarkQuery.position(generateSciQLQuery(queryDomain), noOfDimensions));
        }

        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
            queries.add(BenchmarkQuery.shape(generateSciQLQuery(queryDomain), noOfDimensions));
        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            queries.add(BenchmarkQuery.multipleSelect(generateMultiDomainQuery(multiAccessDomains.getFirst(), multiAccessDomains.getSecond()), noOfDimensions));
        }

        return queries;
    }

    @Override
    public BenchmarkQuery getMiddlePointQuery() {
        List<Pair<Long, Long>> middlePointQueryDomain = queryDomainGenerator.getMiddlePointQueryDomain();
        return BenchmarkQuery.middlePoint(generateSciQLQuery(middlePointQueryDomain), noOfDimensions);
    }

    private String generateSciQLQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT * FROM {0} WHERE {1}", benchContext.getArrayName(), convertToSciQLDomain(domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT count(*) FROM {0} WHERE {1}", benchContext.getArrayName(), convertToSciQLDomain(domain1, domain2));
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