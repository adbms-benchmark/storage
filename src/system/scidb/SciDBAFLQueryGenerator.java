package system.scidb;

import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.BenchmarkSession;
import util.Pair;

import java.text.MessageFormat;
import java.util.List;

/**
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBAFLQueryGenerator extends QueryGenerator {

    public SciDBAFLQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public Benchmark getCachingBenchmark() {
        Benchmark ret = new Benchmark();
        
        BenchmarkSession domainBenchmark = new BenchmarkSession("domain benchmark session");
        
        // count cloud-free pixels, returning a 1D array for all arrays
        String cloudCoverQuery = MessageFormat.format("aggregate( filter( join({0}, {1}), v0 > 0 and v1 > 0), count(*) );",
                benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1());
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(cloudCoverQuery));
        
        // run ndvi
        String ndviQueryFormat = "aggregate( filter( join(%s, %s), ((v0 - v1) / (v0 + v1)) > %f and ((v0 - v1) / (v0 + v1)) < %f), count(*));";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1(), 0.2, 0.4)));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1(), 0.22, 0.45)));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1(), 0.21, 0.38)));
        
        // calculate SAVI on a spatial subset
        // savi = ((NIR-R) / (NIR + R + L)) * (1+L)
        String saviQueryFormat = "aggregate( apply( join(%s, %s), v2, ((v0 - v1) / (v0 + v1 + 0.5)) * 1.5 ), min(v2));";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(saviQueryFormat,
                benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
        
        ret.add(domainBenchmark);
        
        BenchmarkSession lowerLeftToLowerRight = new BenchmarkSession("tile benchmark session lower left to lower right");
        String subsetQuery = "aggregate( between( %s, %d, %d, %d, %d ), min(v0) )";
        for (int i = 0; i < 5; i++) {
            int origin = i * 500;
            lowerLeftToLowerRight.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, benchmarkContext.getArrayName0(), origin, 0, 3999 + origin, 3999)));
        }
        ret.add(lowerLeftToLowerRight);
        
        BenchmarkSession lowerLeftToUpperRight = new BenchmarkSession("tile benchmark session lower left to upper right");
        for (int i = 0; i < 5; i++) {
            int origin = i * 500;
            lowerLeftToUpperRight.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, benchmarkContext.getArrayName0(), origin, origin, 3999 + origin, 3999 + origin)));
        }
        ret.add(lowerLeftToUpperRight);
        
        BenchmarkSession zoomIn = new BenchmarkSession("tile benchmark session zoom in");
        for (int i = 0; i < 3; i++) {
            int zoom = i * 500;
            zoomIn.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 3999 - zoom, 3999 - zoom)));
        }
        ret.add(zoomIn);
        
        BenchmarkSession zoomOut = new BenchmarkSession("tile benchmark session zoom out");
        for (int i = 2; i >= 0; i--) {
            int zoom = i * 500;
            zoomOut.addBenchmarkQuery(new BenchmarkQuery(String.format(
                    subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 3999 - zoom, 3999 - zoom)));
        }
        ret.add(zoomOut);
        
        BenchmarkSession sqrt = new BenchmarkSession("repeated square root");
        String sqrtQuery = "aggregate( apply( %s, v1, %s ), min(v1) );";
        String sqrtExpr = "abs(v0)";
        for (int i = 0; i < 10; i++) {
            sqrt.addBenchmarkQuery(new BenchmarkQuery(String.format(sqrtQuery, benchmarkContext.getArrayName0(), sqrtExpr)));
            sqrtExpr = "sqrt(" + sqrtExpr + ")";
        }
        ret.add(sqrt);
        
        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("repeated multiplication");
            String query = "aggregate( apply( %s, v1, %s ), min(v1) );";
            String expr = "v0";
            for (int i = 0; i < 10; i++) {
                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, benchmarkContext.getArrayName0())));
                expr = expr + " * v0";
            }
            ret.add(benchmarkSession);
        }
        
        return ret;
    }
    
    public static String getAttributes(int n, String baseType) {
        StringBuilder att = new StringBuilder("");
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                att.append(",");
            }
            att.append("att").append(i).append(":").append(baseType);
        }
        return att.toString();
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
        return MessageFormat.format("SELECT * FROM consume((SELECT * FROM {0}))", convertToSciDBBetween(benchmarkContext.getArrayName(), domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT * FROM consume((SELECT * FROM {0}))", convertToSciDBDomain(benchmarkContext.getArrayName(), domain1, domain2));
    }

    public static String convertToSciDBBetween(String collectionName, List<Pair<Long, Long>> domain) {
        StringBuilder lowerBounds = new StringBuilder();
        StringBuilder upperBounds = new StringBuilder();

        boolean isFirst = true;
        int i = 0;
        for (Pair<Long, Long> axisDomain : domain) {
            if (!isFirst) {
                lowerBounds.append(",");
                upperBounds.append(",");
            }
            lowerBounds.append(axisDomain.getFirst());
            upperBounds.append(axisDomain.getSecond());

            isFirst = false;
            ++i;
        }

        return String.format("between(%s,%s,%s)", collectionName, lowerBounds.toString(), upperBounds.toString());
    }

    public static String convertToSciDBDomain(String collectionName, List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return String.format("merge(aggregate(%s, count(*)),aggregate(%s, count(*)))", convertToSciDBBetween(collectionName, domain1), convertToSciDBBetween(collectionName, domain2));
    }
}
