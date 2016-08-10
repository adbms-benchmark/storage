package system.scidb;

import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.BenchmarkSession;
import benchmark.caching.CachingBenchmarkDataManager;
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
        String cloudCoverQuery = MessageFormat.format("aggregate( filter( project( between( {0}, 0, 0, 0, 1, null, null), "
                + "att2, att3, att4), att2 > 0 and att3 > 0 and att4 > 0), count(*), d0 );", benchmarkContext.getArrayName());
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(cloudCoverQuery));
        
        // run ndvi
        String ndviQueryFormat = "aggregate( filter( cast( project( slice( {1}, d0, {0} ), att3, att4 ), "
                + "<att3:float, att4:float>[a=0:7999,500,0, b=0:7999,500,0] ), "
                + "((att4 - att3) / (att4 + att3)) > 0.2 and ((att4 - att3) / (att4 + att3)) < 0.4), count(*));";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat, 0, benchmarkContext.getArrayName())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat, 1, benchmarkContext.getArrayName())));
        
        // run ndvi2
        String ndviQueryFormat2 = "aggregate( filter( cast( project( slice( {1}, d0, {0} ), att3, att4 ), "
                + "<att3:float, att4:float>[a=0:7999,500,0, b=0:7999,500,0] ), "
                + "((att4 - att3) / (att4 + att3)) > 0.22 and ((att4 - att3) / (att4 + att3)) < 0.45), count(*));";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat2, 0, benchmarkContext.getArrayName())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(ndviQueryFormat2, 1, benchmarkContext.getArrayName())));
        
        // calculate SAVI on a spatial subset
        // savi = ((NIR-R) / (NIR + R + L)) * (1+L)
        String saviQueryFormat = "aggregate( project( apply( between( cast( project( slice( {1}, d0, {0} ), att3, att4 ), "
                + "<att3:float, att4:float>[a=0:7999,500,0, b=0:7999,500,0] ), null, 500, 2999, 2999 ), "
                + "att5, ((att4 - att3) / (att4 + att3 + 0.5)) * 1.5 ), att5 ), min(att5));";
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(saviQueryFormat, 0, benchmarkContext.getArrayName())));
        domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(saviQueryFormat, 1, benchmarkContext.getArrayName())));
        
        ret.add(domainBenchmark);
        
        BenchmarkSession lowerLeftToLowerRight = new BenchmarkSession("tile benchmark session lower left to lower right");
        String subsetQuery = "aggregate( between( slice( {4}, d0, 0 ), {0}, {2}, {1}, {3} ), min(att1) )";
        for (int i = 0; i < 10; i++) {
            int origin = i * 100;
            lowerLeftToLowerRight.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(
                    subsetQuery, origin, 3999 + origin, 0, 3999, benchmarkContext.getArrayName())));
        }
        ret.add(lowerLeftToLowerRight);
        
        BenchmarkSession lowerLeftToUpperRight = new BenchmarkSession("tile benchmark session lower left to upper right");
        for (int i = 0; i < 10; i++) {
            int origin = i * 100;
            lowerLeftToUpperRight.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(
                    subsetQuery, origin, 3999 + origin, origin, 3999 + origin, benchmarkContext.getArrayName())));
        }
        ret.add(lowerLeftToUpperRight);
        
        BenchmarkSession zoomIn = new BenchmarkSession("tile benchmark session zoom in");
        for (int i = 0; i < 3; i++) {
            int zoom = i * 500;
            zoomIn.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(
                    subsetQuery, zoom, 3999 - zoom, zoom, 3999 - zoom, benchmarkContext.getArrayName())));
        }
        ret.add(zoomIn);
        
        BenchmarkSession zoomOut = new BenchmarkSession("tile benchmark session zoom out");
        for (int i = 2; i >= 0; i--) {
            int zoom = i * 500;
            zoomOut.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(
                    subsetQuery, zoom, 3999 - zoom, zoom, 3999 - zoom, benchmarkContext.getArrayName())));
        }
        ret.add(zoomOut);
        
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
