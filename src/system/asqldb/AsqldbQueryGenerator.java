package system.asqldb;

import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import data.DomainGenerator;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.sqlmda.BenchmarkContextGenerator;
import benchmark.sqlmda.BenchmarkContextJoin;
import benchmark.sqlmda.SqlMdaBenchmarkContext;
import java.io.IOException;
import java.text.MessageFormat;
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

    public AsqldbQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public Pair<String, BenchmarkContext> getCreateQuery(BenchmarkContext bc) {
        DomainGenerator domainGenerator = new DomainGenerator(bc.getArrayDimensionality());
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(bc.getArraySize());

        StringBuilder ret = new StringBuilder();
        ret.append("CREATE TABLE ");
        ret.append(bc.getArrayName());
        ret.append(" (v ").append(((SqlMdaBenchmarkContext)bc).getBaseType());
        ret.append(" MDARRAY[");

        for (int i = 0; i < domainBoundaries.size(); i++) {
            if (i > 0) {
                ret.append(", ");
            }
            ret.append("axis");
            ret.append(i);
            ret.append("(");
            Pair<Long, Long> axisDomain = domainBoundaries.get(i);
            ret.append(axisDomain.getFirst());
            ret.append(":");
            ret.append(axisDomain.getSecond());
            ret.append(")");
        }

        ret.append(']').append(')');
        return Pair.of(ret.toString(), bc);
    }

    @Override
    public Benchmark getStorageBenchmark() {
        Benchmark ret = new Benchmark();
        List<Pair<Long, Long>> middlePointQueryDomain = queryDomainGenerator.getMiddlePointQueryDomain();
        ret.add(BenchmarkQuery.middlePoint(generateRasdamanQuery(middlePointQueryDomain), benchmarkContext.getArrayDimensionality()));
        return ret;
    }

    @Override
    public Benchmark getSqlMdaBenchmark() {
        Benchmark ret = new Benchmark();

        List<BenchmarkContext> benchContexts = BenchmarkContextGenerator.generate(benchmarkContext);

        // query 1:
        //SELECT ADD_CELLS(
        // POW(STDDEV_POP(z.image) -
        //     STDDEV_SAMP(d.image), 2
        // )[t($time)]
        //) as costFunction
        //FROM Dynamics AS d, Zygotic AS z,
        //     embryo_blastoderm AS eb
        //WHERE eb.zygotic_name = '$zygoticName' AND
        //      eb.id = z.id AND d.id = eb.id
        BenchmarkContextJoin bc1 = (BenchmarkContextJoin) benchContexts.get(0);
        BenchmarkContext bcd1 = bc1.getBenchmarkContexts()[0];
        BenchmarkContext bcz1 = bc1.getBenchmarkContexts()[1];
        StringBuilder query1 = new StringBuilder();
        query1.append("select abs(")
                .append("add_cells(pow(z.v - avg_cells(z.v), 2))/(count_cells(z.v > -1))")
                .append(" - ")
                .append("add_cells(pow(d.v - avg_cells(d.v), 2))/(count_cells(z.v > -1) - 1)")
                .append(")")
                .append(" from ")
                .append(bcd1.getArrayName()).append(" as d, ")
                .append(bcz1.getArrayName()).append(" AS z");
        ret.add(BenchmarkQuery.unknown(query1.toString(), bcd1.getArrayDimensionality()));

        return ret;
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
                benchmarkContext.getArrayName(), convertToRasdamanDomain(domain1), convertToRasdamanDomain(domain2));
    }

    private String generateRasdamanQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT A{1} FROM {0}",
                benchmarkContext.getArrayName(), convertToRasdamanDomain(domain));
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
