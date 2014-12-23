package framework.asqldb;


import data.QueryDomainGenerator;
import framework.BenchmarkContext;
import framework.QueryGenerator;
import framework.TableContext;
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

    private final QueryDomainGenerator queryDomainGenerator;
    private final BenchmarkContext benchContext;

    public AsqldbQueryGenerator(BenchmarkContext benchContext, int noOfDimensions, int noOfQueries) {
        this.queryDomainGenerator = new QueryDomainGenerator(benchContext, noOfDimensions, noOfQueries);
        this.benchContext = benchContext;
    }

    @Override
    public List<String> getBenchmarkQueries() {
        List<String> queries = new ArrayList<>();
        for (TableContext tableContext : BenchmarkContext.dataSizes) {
            String query = "select count_cells(a >= 30 and b < 30) from " + tableContext.asqldbTable1 + " as a, "
                    + tableContext.asqldbTable2 + " as b";
            queries.add(query);
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
//        return MessageFormat.format("SELECT count_cells(A{1} >= 0) + count_cells(A{2} >= 0) FROM {0}",
        return MessageFormat.format("SELECT avg_cells(A{1}) + avg_cells(A{2}) FROM {0}",
                benchContext.getCollName1(), convertToRasdamanDomain(domain1), convertToRasdamanDomain(domain2));
    }

    private String generateRasdamanQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT A{1} FROM {0}",
                benchContext.getCollName1(), convertToRasdamanDomain(domain));
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
}