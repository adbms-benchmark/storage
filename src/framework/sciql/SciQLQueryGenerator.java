package framework.sciql;

import data.QueryDomainGenerator;
import framework.BenchmarkContext;
import framework.QueryGenerator;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import rasj.RasGMArray;
import rasj.RasMInterval;
import rasj.RasSInterval;
import util.IO;
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

    public static String convertToSciqlDomain(List<Pair<Long, Long>> domain) {
        StringBuilder ret = new StringBuilder();
        for (Pair<Long, Long> axisDomain : domain) {
            ret.append('[').append(axisDomain.getFirst()).append(':').append(axisDomain.getSecond()).append(']');
        }
        return ret.toString();
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("select (select count(c.v) from {0}{1} as c where c.v >= 0) + (select count(d.v) from {0}{2} as d where d.v >= 0)",
                benchContext.getCollName(), convertToSciqlDomain(domain1), convertToSciqlDomain(domain2));
    }

    private String generateSciqlQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT A{1} FROM {0}",
                benchContext.getCollName(), convertToSciqlDomain(domain));
    }
}