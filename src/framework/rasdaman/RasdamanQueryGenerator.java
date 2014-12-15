package framework.rasdaman;


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
public class RasdamanQueryGenerator extends QueryGenerator {

    private final QueryDomainGenerator queryDomainGenerator;
    private final BenchmarkContext benchContext;

    public RasdamanQueryGenerator(BenchmarkContext benchContext, int noOfDimensions, int noOfQueries) {
        queryDomainGenerator = new QueryDomainGenerator(benchContext, noOfDimensions, noOfQueries);
        this.benchContext = benchContext;
    }

    @Override
    public List<String> getBenchmarkQueries() {

        List<String> queries = new ArrayList<>();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

//        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
//            queries.add(generateRasdamanQuery(queryDomain));
//        }
//
//        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
//            queries.add(generateRasdamanQuery(queryDomain));
//        }
//
//        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
//            queries.add(generateRasdamanQuery(queryDomain));
//        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            queries.add(generateMultiDomainQuery(multiAccessDomains.getFirst(), multiAccessDomains.getSecond()));
        }


        return queries;
    }

    public static String convertToRasdamanDomain(List<Pair<Long, Long>> domain) {
        StringBuilder rasdamanDomain = new StringBuilder();
        rasdamanDomain.append('[');

        boolean isFirst = true;
        for (Pair<Long, Long> axisDomain : domain) {
            if (!isFirst) {
                rasdamanDomain.append(",");
            }

            rasdamanDomain.append(axisDomain.getFirst());
            rasdamanDomain.append(':');
            rasdamanDomain.append(axisDomain.getSecond());
            isFirst = false;
        }

        rasdamanDomain.append(']');

        return rasdamanDomain.toString();
    }


    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT count_cells({0}{1} >= 0) + count_cells({0}{2} >= 0) FROM {0}", benchContext.getCollName1(), convertToRasdamanDomain(domain1), convertToRasdamanDomain(domain2));
    }

    private String generateRasdamanQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT {0}{1} FROM {0}", benchContext.getCollName1(), convertToRasdamanDomain(domain));
    }
}