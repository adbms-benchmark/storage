package system.sciql;

import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import data.DomainGenerator;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.sqlmda.BenchmarkContextGenerator;
import benchmark.sqlmda.BenchmarkContextJoin;
import benchmark.sqlmda.SqlMdaBenchmarkContext;
import java.text.MessageFormat;
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
    public Pair<String, BenchmarkContext> getCreateQuery(BenchmarkContext bc) {
        DomainGenerator domainGenerator = new DomainGenerator(bc.getArrayDimensionality());
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(bc.getArraySize());

        StringBuilder createArrayQuery = new StringBuilder();
        createArrayQuery.append("CREATE ARRAY ");
        createArrayQuery.append(bc.getArrayName());
        createArrayQuery.append(" (");

        for (int i = 0; i < domainBoundaries.size(); i++) {
            createArrayQuery.append("axis");
            createArrayQuery.append(i);
            createArrayQuery.append(" INT DIMENSION [");
            Pair<Long, Long> axisDomain = domainBoundaries.get(i);
            createArrayQuery.append(axisDomain.getFirst());
            createArrayQuery.append(":1:");
            createArrayQuery.append(axisDomain.getSecond());
            createArrayQuery.append("]");
            createArrayQuery.append(", ");
        }

        createArrayQuery.append(" v ").append(((SqlMdaBenchmarkContext)benchmarkContext).getBaseType());
        createArrayQuery.append(')');
        return Pair.of(createArrayQuery.toString(), bc);
    }

    @Override
    public Benchmark getStorageBenchmark() {
        Benchmark queries = new Benchmark();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
            queries.add(BenchmarkQuery.size(generateSciQLQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
            queries.add(BenchmarkQuery.position(generateSciQLQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
            queries.add(BenchmarkQuery.shape(generateSciQLQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            queries.add(BenchmarkQuery.multipleSelect(generateMultiDomainQuery(multiAccessDomains.getFirst(), multiAccessDomains.getSecond()), benchmarkContext.getArrayDimensionality()));
        }
        
        List<Pair<Long, Long>> middlePointQueryDomain = queryDomainGenerator.getMiddlePointQueryDomain();
        queries.add(BenchmarkQuery.middlePoint(generateSciQLQuery(middlePointQueryDomain), benchmarkContext.getArrayDimensionality()));

        return queries;
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
                .append("power(stddev_pop(z.v), 2)")
                .append(" - ")
                .append("power(stddev_samp(d.v), 2)")
                .append(")")
                .append(" from ")
                .append(bcd1.getArrayName()).append(" AS d, ")
                .append(bcz1.getArrayName()).append(" AS z where z.axis0 = d.axis0 and z.axis1 = d.axis1;");
        ret.add(BenchmarkQuery.unknown(query1.toString(), bcd1.getArrayDimensionality()));

        return ret;
    }

    private String generateSciQLQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT * FROM {0} WHERE {1}", benchmarkContext.getArrayName(), convertToSciQLDomain(domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT count(*) FROM {0} WHERE {1}", benchmarkContext.getArrayName(), convertToSciQLDomain(domain1, domain2));
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
}
