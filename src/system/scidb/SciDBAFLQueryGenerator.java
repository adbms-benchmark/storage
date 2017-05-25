package system.scidb;

import benchmark.Benchmark;
import benchmark.BenchmarkQuery;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.BenchmarkSession;
import data.DomainGenerator;
import util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBAFLQueryGenerator extends QueryGenerator {

    public SciDBAFLQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }


    private String getQuery(int dimension, String arrayName, String function, int start, long stop) {
        String subsetQuery;
        List<String> logicalFunctions = new ArrayList<>(Arrays.asList("and", "or", "xor", "not"));
        List<String> aggregateFunctions = new ArrayList<>(Arrays.asList("min_cells", "max_cells", "add_cells", "avg_cells"));

        if (aggregateFunctions.contains(function)) {
            switch (dimension) {
                case 1:
                    subsetQuery = "SELECT " + function + "(c[%d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, arrayName);
                case 2:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
                case 3:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, arrayName);
                case 4:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop, arrayName);
                default:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
            }
        }
        else {
            switch (dimension) {
                case 1:
                    subsetQuery = "SELECT (c[%d:%d]) " + function + "(c[%d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
                case 2:
                    subsetQuery = "SELECT (c[%d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop, arrayName);
                case 3:
                    subsetQuery = "SELECT (c[%d:%d, %d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop, start, stop, start, stop, arrayName);
                case 4:
                    subsetQuery = "SELECT ((c[%d:%d, %d:%d, %d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d, %d:%d, %d:%d])) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop,
                            start, stop, start, stop, start, stop, start, stop, arrayName);
                default:
                    subsetQuery = "SELECT (c[%d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
            }
        }
    }

    @Override
    public Benchmark getOperationsBenchmark() {
        Benchmark ret = new Benchmark();
        int arrayDimensionality = benchmarkContext.getArrayDimensionality();
        String arrayName = benchmarkContext.getArrayName();
        DomainGenerator domainGenerator = new DomainGenerator(arrayDimensionality);
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize());
        long upperBoundary = domainBoundaries.get(0).getSecond();

        //
//        //SELECT
//        {
////            System.out.println("Start test SELECT operation");
//            BenchmarkSession benchmarkSession = new BenchmarkSession("SELECT");
//            String query = "SELECT c FROM %s AS c, %s as d";
//            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
//            ret.add(benchmarkSession);
////            System.out.println("Stop test SELECT operation");
//        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("SELECT");
            String query = "SELECT c FROM %s AS c";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);
        }

        {
            String[] aggregateFuncs = {"min", "max", "sum", "avg"};
            for (String aggregateFunc : aggregateFuncs) {
                String subsetQuery = "aggregate( between( %s, %d, %d, %d, %d ), " + aggregateFunc + "(v0) )";
                BenchmarkSession benchmarkSession = new BenchmarkSession("subset window lower left to lower right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), origin, 0, 3999 + origin, 3999)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window lower left to upper right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), origin, origin, 3999 + origin, 3999 + origin)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom in, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 7999 - zoom, 7999 - zoom)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom out, " + aggregateFunc);
                for (int i = 7; i >= 0; i--) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 7999 - zoom, 7999 - zoom)));
                }
                ret.add(benchmarkSession);
            }
        }

//        //TODO joins, nested queries, sorting.
//
//        //SELECT
//        {
////            System.out.println("Start test SELECT operation");
//            BenchmarkSession benchmarkSession = new BenchmarkSession("SELECT");
//            String query = "SELECT c FROM %s AS c, %s as d";
//            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
//            ret.add(benchmarkSession);
////            System.out.println("Stop test SELECT operation");
//        }
//
//        //CASTING
//        {
////            System.out.println("Start test SELECT operation");
//            BenchmarkSession benchmarkSession = new BenchmarkSession("CASTING");
//            String query = "SELECT (float)c FROM %s AS c";
//            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
//            ret.add(benchmarkSession);
////            System.out.println("Stop test SELECT operation");
//        }
//
//        {
////            System.out.println("Start test aggregate min, max, add, avg operation");
//            String[] aggregateFuncs = {"min_cells", "max_cells", "add_cells", "avg_cells"};
//            for (String aggregateFunc : aggregateFuncs) {
////                long upperBoundary = domainBoundaries.get(0).getSecond();
////                System.out.println(domainBoundaries.get(0));
//
//                BenchmarkSession benchmarkSession = new BenchmarkSession(
//                        String.format("subset [0, i], with i = [0, %d] and aggregate function: %s", upperBoundary, aggregateFunc));
//                for (int i = 0; i <= upperBoundary; i++) {
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(getQuery(arrayDimensionality, arrayName, aggregateFunc, 0, i)));
//                }
//                ret.add(benchmarkSession);
//
//                benchmarkSession = new BenchmarkSession(
//                        String.format("subset [i, %d], with i = [0, %d] and aggregate function: %s", arrayDimensionality, arrayDimensionality, aggregateFunc));
//                for (int i = 0; i <= upperBoundary; i++) {
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(getQuery(arrayDimensionality, arrayName, aggregateFunc, i, upperBoundary)));
//                }
//                ret.add(benchmarkSession);
//
//            }
////            System.out.println("Stop test aggregate min, max, add, avg operation");
//        }
////*/
//        {
////            System.out.println("Start test trigonometric operations");
//            String[][] unaryFuncs = {{"sqrt", "abs(c)"}, {"sin", "c"}, {"cos", "c"}, {"tan", "c"},
////            {"arccos", "c"}, {"acos", "c"}, {"asin", "c"}
//            };
//            for (String[] unaryFunc : unaryFuncs) {
//                String func = unaryFunc[0];
//                BenchmarkSession benchmarkSession = new BenchmarkSession(func);
//                String query = "SELECT %s FROM %s AS c";
//                String expr = unaryFunc[1];
//                for (int i = 0; i < 5; i++) {
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, arrayName)));
//                    expr = func + "(" + expr + ")";
//                }
//                ret.add(benchmarkSession);
//            }
////            System.out.println("Stop test trigonometric operations");
//        }
//
//        {
////            System.out.println("Start test logical operations");
//            String[][] logicalFuncs = {{"Logical AND", "and"}, {"Logical OR", "or"}, {"Logical XOR", "xor"}}; //not
//            for (String[] logicalFunc : logicalFuncs) {
//                String func = logicalFunc[0];
//                BenchmarkSession benchmarkSession = new BenchmarkSession(func);
//                String expr = logicalFunc[1];
//                for (int i = 0; i <= upperBoundary; i++) {
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(getQuery(arrayDimensionality, arrayName, expr, 0, i)));
//                }
//                ret.add(benchmarkSession);
//            }
////            System.out.println("Stop test logical operations");
//        }
//
//        {
////            System.out.println("Start test algebraic operations");
//            String[][] binaryFuncs = {{"multiplication", "*"}, {"division", "/"}, {"addition", "+"}, {"subtraction", "-"}};
//            for (String[] binaryFunc : binaryFuncs) {
//                BenchmarkSession benchmarkSession = new BenchmarkSession(binaryFunc[0]);
//                String query = "SELECT min_cells(%s) FROM %s AS c";
//                String expr = "c";
//                String op = binaryFunc[1];
//                for (int i = 0; i < 10; i++) {
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, arrayName)));
//                    expr = expr + op + "c";
//                }
//                ret.add(benchmarkSession);
//            }
////            System.out.println("Stop test algebraic operations");
//        }
//
//        {
////            System.out.println("Start test comparison operation");
//            String[][] comparisonFuncs = {{"less than", "<"}, {"greater than", ">"}, {"less than or equal to", "<="},
//                    {"greater than or equal to", ">="}, {"equal to", "="}, {"not equal to", "!="}};
//            for (String[] comparisonFunc : comparisonFuncs) {
//                BenchmarkSession benchmarkSession = new BenchmarkSession(comparisonFunc[0]);
//                String query = "SELECT count_cells(%s) FROM %s AS c";
//                String expr = "";
//                String op = comparisonFunc[1];
//                for (int i = 0; i < 10; i++) {
//                    String currExpr = "(c" + op + i + ")";
//                    if (expr.isEmpty()) {
//                        expr = currExpr;
//                    } else {
//                        expr = expr + " and " + currExpr;
//                    }
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, arrayName)));
//                }
//                ret.add(benchmarkSession);
//            }
////            System.out.println("Stop test comparison operation");
//        }

        return ret;
    }

    @Override
    public Benchmark getCachingBenchmark() {
        Benchmark ret = new Benchmark();
        
        {
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
//            ret.add(domainBenchmark);
        }
        
        {
            String[] aggregateFuncs = {"min", "max", "sum", "avg"};
            for (String aggregateFunc : aggregateFuncs) {
                String subsetQuery = "aggregate( between( %s, %d, %d, %d, %d ), " + aggregateFunc + "(v0) )";
                BenchmarkSession benchmarkSession = new BenchmarkSession("subset window lower left to lower right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), origin, 0, 3999 + origin, 3999)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window lower left to upper right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), origin, origin, 3999 + origin, 3999 + origin)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom in, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 7999 - zoom, 7999 - zoom)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom out, " + aggregateFunc);
                for (int i = 7; i >= 0; i--) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 7999 - zoom, 7999 - zoom)));
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[] aggregateFuncs = {"count", "min", "max"};
            for (String aggregateFunc : aggregateFuncs) {
                String subsetQuery = "aggregate( filter( between( %s, %d, %d, %d, %d ), v0 > 0.0 and v0 < 100.0), " + aggregateFunc + "(v0) )";
                BenchmarkSession benchmarkSession = new BenchmarkSession("subset window lower left to lower right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), origin, 0, 3999 + origin, 3999)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window lower left to upper right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), origin, origin, 3999 + origin, 3999 + origin)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom in, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 7999 - zoom, 7999 - zoom)));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom out, " + aggregateFunc);
                for (int i = 7; i >= 0; i--) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, benchmarkContext.getArrayName0(), zoom, zoom, 7999 - zoom, 7999 - zoom)));
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[][] comparisonFuncs = {{"less than", "<"}, {"greater than", ">"}, {"less than or equal to", "<="},
            {"greater than or equal to", ">="}, {"equal to", "="}, {"not equal to", "!="}};
            for (String[] comparisonFunc : comparisonFuncs) {
                BenchmarkSession benchmarkSession = new BenchmarkSession(comparisonFunc[0]);
                String query = "aggregate( filter( %s, %s ), count(*) );";
                String expr = "";
                String op = comparisonFunc[1];
                for (int i = 0; i < 10; i++) {
                    String currExpr = "(v0" + op + i + ")";
                    if (expr.isEmpty()) {
                        expr = currExpr;
                    } else {
                        expr = expr + " and " + currExpr;
                    }
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, benchmarkContext.getArrayName0(), expr)));
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[][] unaryFuncs = {{"sqrt", "abs(v0)"}, {"sin", "v0"}, {"cos", "v0"}};
            for (String[] unaryFunc : unaryFuncs) {
                String func = unaryFunc[0];
                BenchmarkSession benchmarkSession = new BenchmarkSession(func);
                String query = "aggregate( apply( %s, v1, %s ), min(v1) );";
                String expr = unaryFunc[1];
                for (int i = 0; i < 10; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, benchmarkContext.getArrayName0(), expr)));
                    expr = func + "(" + expr + ")";
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[][] binaryFuncs = {{"multiplication", "*"}, {"division", "/"}, {"addition", "+"}, {"subtraction", "-"}};
            for (String[] binaryFunc : binaryFuncs) {
                BenchmarkSession benchmarkSession = new BenchmarkSession(binaryFunc[0]);
                String query = "aggregate( apply( %s, v1, %s ), min(v1) );";
                String expr = "v0";
                String op = binaryFunc[1];
                for (int i = 0; i < 10; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, benchmarkContext.getArrayName0(), expr)));
                    expr = expr + op + "v0";
                }
                ret.add(benchmarkSession);
            }
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
