package system.scidb;

import benchmark.*;
import benchmark.operations.OperationsBenchmarkContext;
import util.Pair;

import java.text.MessageFormat;
import java.util.List;

/**
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 * @author Danut Rusu
 */
public class SciDBAQLQueryGenerator extends QueryGenerator {

    private static final int comparisonNumber = 500;

    public SciDBAQLQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public Benchmark getOperationsBenchmark() {
        String[] algebraicFuncs1 = {"sqrt(abs", "abs"};
        String[] algebraicFuncs2 = {"+", "-", "*", "/"};
        String[] comparisonFuncs = {"<", "<=", "<>", "=", ">", ">="};
        String[] trigonometricFuncs = {"sin", "cos", "tan", "atan"};
        String[] logicalFuncs = {"and", "or"};
        String[] aggregateFuncs = {"min", "max", "sum", "avg"};

        Benchmark ret = new Benchmark();
        int arrayDimensionality = benchmarkContext.getArrayDimensionality();
        String arrayName = benchmarkContext.getArrayName();
        String dataType = ((OperationsBenchmarkContext)benchmarkContext).getDataType();
        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("SELECT (%dD)");
            String query = "SELECT * FROM %s AS c";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("CASTING (%dD)");
            String query = "SELECT float(v) FROM %s AS c";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);
        }

        {
            if (arrayDimensionality >= 2) {
                BenchmarkSession benchmarkSession = new BenchmarkSession(
                        String.format("AGGREGATE FUNCTIONS (min, max, sum, avg ) (%dD)"
                                , arrayDimensionality));

                for (String aggregateFunc : aggregateFuncs) {
                    if (dataType.equals("char") && aggregateFunc.equals("avg")) {
                        continue;
                    }
                    String query = String.format("SELECT %s(v) FROM %s", aggregateFunc, arrayName);
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
                }
                ret.add(benchmarkSession);
            }
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession(
                    String.format("ALGEBRAIC FUNCTIONS (sqrt(abs), abs, +, -, *, /) (%dD)"
                            , arrayDimensionality));
            for (String algebraicFunc : algebraicFuncs1) {

                String query = String.format("SELECT %s(v) FROM %s", algebraicFunc, arrayName);
                if (algebraicFunc.equals("sqrt(abs"))
                    query = String.format("SELECT %s(v)) FROM %s", algebraicFunc, arrayName);

                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
            }

            for (String algebraicFunc : algebraicFuncs2) {
                    String query = String.format("SELECT v %s v FROM %s WHERE v <> 0", algebraicFunc, arrayName);

                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
            }
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession(
                    String.format("LOGICAL OPERATORS (%dD)",arrayDimensionality));
            for (String logicalFunc : logicalFuncs) {

                String query = String.format("SELECT v > 0 %s v < 500 FROM %s", logicalFunc, arrayName);
                if (logicalFunc.equals("not"))
                    query = String.format("SELECT %s(v < 500) FROM %s", logicalFunc, arrayName);

                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
            }
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession(
                    String.format("COMPARISON OPERATORS (%dD)", arrayDimensionality));
            for (String comparisonFunc : comparisonFuncs) {
                String query = String.format("SELECT v %s %d FROM %s", comparisonFunc, comparisonNumber, arrayName);
                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
            }
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession(
                    String.format("TRIGONOMETRIC OPERATORS (%dD)", arrayDimensionality));
            for (String trigonometricFunc : trigonometricFuncs) {
                String query = String.format("SELECT %s(v) FROM %s", trigonometricFunc, arrayName);
                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
            }
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession(
                    String.format("STACK ALGEBRAIC FUNCTIONS (+, -, *, / and remainder) FOR CACHING (%dD)", arrayDimensionality));

            for (String algebraicFunc : algebraicFuncs2) {
                String expr = "v";
                for (int i = 0; i < 10; i++) {
                    String query = String.format("SELECT %s FROM %s WHERE v <> 0", expr, arrayName);
                    expr += algebraicFunc + "v";
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));

                }
            }
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession;
            String query;
            benchmarkSession = new BenchmarkSession("SIMPLE SELECT 2 DIMENSIONS");
            query = "SELECT v FROM %s";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);

            benchmarkSession = new BenchmarkSession("SIMPLE SELECT 2 DIMENSIONS");
            query = "SELECT v + v FROM %s";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);

            benchmarkSession = new BenchmarkSession("SELECT 2 DIMENSIONS with AGGREGATE FUNC");
            for (String aggregateFunc : aggregateFuncs) {
                if (dataType.equals("char") && aggregateFunc.equals("avg")) {
                    continue;
                }
                query = "SELECT %s(v) + %s(v) FROM %s";
                benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, aggregateFunc, aggregateFunc, arrayName)));
            }
            ret.add(benchmarkSession);

            benchmarkSession = new BenchmarkSession("SELECT 2 DIMENSIONS with AGGREGATE FUNC and COMPARISON");
            for (String aggregateFunc : aggregateFuncs) {
                for (String comparisonFunc : comparisonFuncs) {
                    if (dataType.equals("char") && aggregateFunc.equals("avg")) {
                        continue;
                    }
                    query = "SELECT %s(v) %s %s(v) FROM %s";
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, aggregateFunc, comparisonFunc, aggregateFunc, arrayName)));
                }
            }
            ret.add(benchmarkSession);

            benchmarkSession = new BenchmarkSession("SELECT 2 DIMENSIONS with TRIGONOMETRIC FUNC and COMPARISON");
            for (String trigonometricFunc : trigonometricFuncs) {
                for (String comparisonFunc : comparisonFuncs) {
                    query = "SELECT %s(v) %s %s(v) FROM %s";
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, trigonometricFunc, comparisonFunc, trigonometricFunc, arrayName)));
                }
            }
            ret.add(benchmarkSession);
        }

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
