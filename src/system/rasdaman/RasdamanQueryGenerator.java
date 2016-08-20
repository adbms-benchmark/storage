package system.rasdaman;


import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import benchmark.BenchmarkSession;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;

import java.text.MessageFormat;
import java.util.List;

import util.Pair;

/**
 * @author George Merticariu
 */
public class RasdamanQueryGenerator extends QueryGenerator {

    public RasdamanQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public Benchmark getCachingBenchmark() {
        Benchmark ret = new Benchmark();
        
        BenchmarkSession domainBenchmark = new BenchmarkSession("domain benchmark session");
        
        // count cloud-free pixels, returning a 1D array for all arrays
        String cloudCoverQuery = String.format("SELECT count_cells(c > 0 and d > 0) FROM %s AS c, %s AS d", 
                benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1());
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(cloudCoverQuery));
        
        // run ndvi
        String ndviQueryFormat = "SELECT count_cells((((nir - red) / (nir + red)) > %f) AND (((nir - red) / (nir + red)) < %f)) FROM %s AS nir, %s AS red";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                0.2, 0.4, benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                0.22, 0.45, benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                0.21, 0.38, benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
        
        // calculate SAVI on a spatial subset
        // savi = ((NIR-R) / (NIR + R + L)) * (1+L)
        
        String saviQueryFormat = "SELECT min_cells(((nir - red) / (nir + red + 0.5)) * 1.5) FROM %s AS nir, %s AS red";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(saviQueryFormat,
                benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
        
        ret.add(domainBenchmark);
        
        BenchmarkSession lowerLeftToLowerRight = new BenchmarkSession("tile benchmark session lower left to lower right");
        String subsetQuery = "SELECT min_cells(c[%d:%d,%d:%d]) FROM %s as c";
        for (int i = 0; i < 5; i++) {
            int origin = i * 500;
            lowerLeftToLowerRight.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, origin, 3999 + origin, 0, 3999, benchmarkContext.getArrayName0())));
        }
        ret.add(lowerLeftToLowerRight);
        
        BenchmarkSession lowerLeftToUpperRight = new BenchmarkSession("tile benchmark session lower left to upper right");
        for (int i = 0; i < 5; i++) {
            int origin = i * 500;
            lowerLeftToUpperRight.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, origin, 3999 + origin, origin, 3999 + origin, benchmarkContext.getArrayName0())));
        }
        ret.add(lowerLeftToUpperRight);
        
        BenchmarkSession zoomIn = new BenchmarkSession("tile benchmark session zoom in");
        for (int i = 0; i < 3; i++) {
            int zoom = i * 500;
            zoomIn.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, zoom, 3999 - zoom, zoom, 3999 - zoom, benchmarkContext.getArrayName0())));
        }
        ret.add(zoomIn);
        
        BenchmarkSession zoomOut = new BenchmarkSession("tile benchmark session zoom out");
        for (int i = 2; i >= 0; i--) {
            int zoom = i * 500;
            zoomOut.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, zoom, 3999 - zoom, zoom, 3999 - zoom, benchmarkContext.getArrayName0())));
        }
        ret.add(zoomOut);
        ret.clear();
        
        BenchmarkSession sqrt = new BenchmarkSession("repeated square root");
        String sqrtQuery = "SELECT min_cells(%s) FROM %s AS c";
        String sqrtExpr = "abs(c)";
        for (int i = 0; i < 10; i++) {
            sqrt.addBenchmarkQuery(new BenchmarkQuery(String.format(sqrtQuery, sqrtExpr, benchmarkContext.getArrayName0())));
            sqrtExpr = "sqrt(" + sqrtExpr + ")";
        }
        ret.add(sqrt);
        
        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("repeated multiplication");
            String query = "SELECT min_cells(%s) FROM %s AS c";
            String expr = "c";
            for (int i = 0; i < 10; i++) {
                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, benchmarkContext.getArrayName0())));
                expr = expr + " * c";
            }
            ret.add(benchmarkSession);
        }
        
//        {
//            BenchmarkSession benchmarkSession = new BenchmarkSession("repeated multiplication");
//            String query = "SELECT min_cells(pow(c, %d)) FROM %s AS c";
//            String expr = "power";
//            for (int i = 1; i < 11; i++) {
//                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, i, benchmarkContext.getArrayName0())));
//            }
//            ret.add(benchmarkSession);
//        }
        
        return ret;
    }

    @Override
    public Benchmark getStorageBenchmark() {
        Benchmark ret = new Benchmark();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
            ret.add(BenchmarkQuery.size(generateRasdamanQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
            ret.add(BenchmarkQuery.position(generateRasdamanQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
            ret.add(BenchmarkQuery.shape(generateRasdamanQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            ret.add(BenchmarkQuery.multipleSelect(generateMultiDomainQuery(multiAccessDomains.getFirst(), 
                    multiAccessDomains.getSecond()), benchmarkContext.getArrayDimensionality()));
        }
        
        List<Pair<Long, Long>> middlePointQueryDomain = queryDomainGenerator.getMiddlePointQueryDomain();
        ret.add(BenchmarkQuery.middlePoint(generateRasdamanQuery(middlePointQueryDomain), benchmarkContext.getArrayDimensionality()));

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
        return MessageFormat.format("SELECT count_cells({0}{1} >= 0) + count_cells({0}{2} >= 0) FROM {0}", 
                benchmarkContext.getArrayName(), convertToRasdamanDomain(domain1), convertToRasdamanDomain(domain2));
    }

    private String generateRasdamanQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT {0}{1} FROM {0}", benchmarkContext.getArrayName(), convertToRasdamanDomain(domain));
    }
}