package system.scidb;

import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.BenchmarkSession;

import java.text.MessageFormat;
import java.util.List;

import util.Pair;

/**
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBAQLQueryGenerator extends QueryGenerator {

    public SciDBAQLQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public Benchmark getCachingBenchmark() {
        Benchmark ret = new Benchmark();
        
        BenchmarkSession domainBenchmark = new BenchmarkSession("domain benchmark session");
        
        // compute cloud-free pixel percent, returning a 1D array for all arrays
        String cloudCoverQuery = String.format("SELECT min_cells(MARRAY i IN [0:1] VALUES "
                + "((float) count_cells(c[i[0],*:*,*:*].att2 > 0 and c[i[0],*:*,*:*].att3 > 0 and c[i[0],*:*,*:*].att4 > 0)) / 64000000.0) "
                + "FROM %s AS c", benchmarkContext.getArrayName());
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(cloudCoverQuery));
        
        // run ndvi
        String ndviQueryFormat = "SELECT min_cells((((c[{0},*:*,*:*].att4 - c[{0},*:*,*:*].att3) / (c[{0},*:*,*:*].att4 + c[{0},*:*,*:*].att3)) > 0.2)"
                + " AND (((c[{0},*:*,*:*].att4 - c[{0},*:*,*:*].att3) / (c[{0},*:*,*:*].att4 + c[{0},*:*,*:*].att3)) < 0.4))"
                + " FROM {1} AS c";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat, 0, benchmarkContext.getArrayName())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat, 1, benchmarkContext.getArrayName())));
        
        // run ndvi2
        String ndviQueryFormat2 = "SELECT min_cells(((((float)c[{0},*:*,*:*].att4 - (float)c[{0},*:*,*:*].att3) / ((float)c[{0},*:*,*:*].att4 + (float)c[{0},*:*,*:*].att3)) > 0.22)"
                + " AND (((c[{0},*:*,*:*].att4 - c[{0},*:*,*:*].att3) / (c[{0},*:*,*:*].att4 + c[{0},*:*,*:*].att3)) < 0.45))"
                + " FROM {1} AS c";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat2, 0, benchmarkContext.getArrayName())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat2, 1, benchmarkContext.getArrayName())));
        
        // calculate SAVI on a spatial subset
        // savi = ((NIR-R) / (NIR + R + L)) * (1+L)
        String saviQueryFormat = "SELECT min_cells((((float)c[{0},*:2999,500:2999].att4 - (float)c[{0},*:2999,500:2999].att3) / "
                + "((float)c[{0},*:2999,500:2999].att4 + (float)c[{0},*:2999,500:2999].att3 + 0.5)) * 1.5) "
                + "FROM {1} AS c";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(saviQueryFormat, 0, benchmarkContext.getArrayName())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(saviQueryFormat, 1, benchmarkContext.getArrayName())));
        
        ret.add(domainBenchmark);
        
        
        BenchmarkSession lowerLeftToLowerRight = new BenchmarkSession("tile benchmark session lower left to lower right");
        String subsetQuery = "SELECT min_cells(c[0,%d:%d,%d:%d]) FROM %s as c";
        for (int i = 0; i < 10; i++) {
            int origin = i * 100;
            lowerLeftToLowerRight.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, origin, 3999 + origin, 0, 3999, benchmarkContext.getArrayName())));
        }
        ret.add(lowerLeftToLowerRight);
        
        BenchmarkSession lowerLeftToUpperRight = new BenchmarkSession("tile benchmark session lower left to upper right");
        for (int i = 0; i < 10; i++) {
            int origin = i * 100;
            lowerLeftToUpperRight.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, origin, 3999 + origin, origin, 3999 + origin, benchmarkContext.getArrayName())));
        }
        ret.add(lowerLeftToUpperRight);
        
        BenchmarkSession zoomIn = new BenchmarkSession("tile benchmark session zoom in");
        for (int i = 0; i < 3; i++) {
            int zoom = i * 500;
            zoomIn.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, zoom, 3999 - zoom, zoom, 3999 - zoom, benchmarkContext.getArrayName())));
        }
        ret.add(zoomIn);
        
        BenchmarkSession zoomOut = new BenchmarkSession("tile benchmark session zoom out");
        for (int i = 2; i >= 0; i--) {
            int zoom = i * 500;
            zoomOut.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, zoom, 3999 - zoom, zoom, 3999 - zoom, benchmarkContext.getArrayName())));
        }
        ret.add(zoomOut);
        
        return ret;
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
        return MessageFormat.format("SELECT * FROM consume((SELECT a FROM {0} WHERE {1}))", benchmarkContext.getArrayName(), convertToSciDBDomain(domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT * FROM consume((SELECT count(a) FROM {0} WHERE {1}))", benchmarkContext.getArrayName(), convertToSciDBDomain(domain1, domain2));
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
