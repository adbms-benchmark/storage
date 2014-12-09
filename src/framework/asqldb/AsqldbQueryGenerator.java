package framework.asqldb;


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
public class AsqldbQueryGenerator extends QueryGenerator {

    private QueryDomainGenerator queryDomainGenerator;

    public AsqldbQueryGenerator(long collectionSize, int noOfDimensions, long maxSelectSize, int noOfQueries) {
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

    public static String convertToRasdamanMddType(int noOfDimensions) {
        switch (noOfDimensions) {
            case 1:
                return "GreyString";
            case 2:
                return "GreyImage";
            case 3:
                return "GreyCube";
            default:
                return null;
        }
    }

    public static RasGMArray convertToRasGMArray(List<Pair<Long, Long>> sdom, String filePath) throws IOException {
        int noOfDimensions = sdom.size();
        RasMInterval domain = new RasMInterval(noOfDimensions);
        for (int i = 0; i < noOfDimensions; i++) {
            Pair<Long, Long> p = sdom.get(i);
            try {
                domain.setItem(i, new RasSInterval(p.getFirst(), p.getSecond()));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        RasGMArray ret = new RasGMArray(domain, 1);
        ret.setObjectTypeName(convertToRasdamanMddType(noOfDimensions));
        ret.setArray(IO.readFile(filePath));
        return ret;
    }

    private static String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT count_cells(A{1} >= 0) + count_cells(A{2} >= 0) FROM {0}", BenchmarkContext.COLLECTION_NAME, convertToRasdamanDomain(domain1), convertToRasdamanDomain(domain2));
    }

    private static String generateRasdamanQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT A{1} FROM {0}", BenchmarkContext.COLLECTION_NAME, convertToRasdamanDomain(domain));
    }
}