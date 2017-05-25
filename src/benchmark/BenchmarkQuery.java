package benchmark;

/**
 * Wraps a benchmark query including the query string, type and dimension.
 * 
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class BenchmarkQuery {

    public enum QueryType {
        SIZE,
        POSITION,
        SHAPE,
        MULTIPLE_SELECT,
        MIDDLE_POINT,
        UNKNOWN
    }

    private final String queryString;
    private final int dimensionality;
    private final QueryType queryType;

    public BenchmarkQuery(String queryString, int dimensionality, QueryType queryType) {
        this.queryString = queryString;
        this.dimensionality = dimensionality;
        this.queryType = queryType;
    }

    public BenchmarkQuery(String queryString) {
        this.queryString = queryString;
        this.dimensionality = 3;
        this.queryType = QueryType.UNKNOWN;
    }


    public static BenchmarkQuery size(String queryString, int dimensionality) {
        return new BenchmarkQuery(queryString, dimensionality, QueryType.SIZE);
    }

    public static BenchmarkQuery position(String queryString, int dimensionality) {
        return new BenchmarkQuery(queryString, dimensionality, QueryType.POSITION);
    }

    public static BenchmarkQuery shape(String queryString, int dimensionality) {
        return new BenchmarkQuery(queryString, dimensionality, QueryType.SHAPE);
    }

    public static BenchmarkQuery multipleSelect(String queryString, int dimensionality) {
        return new BenchmarkQuery(queryString, dimensionality, QueryType.MULTIPLE_SELECT);
    }

    public static BenchmarkQuery unknown(String queryString, int dimensionality) {
        return new BenchmarkQuery(queryString, dimensionality, QueryType.UNKNOWN);
    }

    public static BenchmarkQuery middlePoint(String queryString, int dimensionality) {
        return new BenchmarkQuery(queryString, dimensionality, QueryType.MIDDLE_POINT);
    }


    public String getQueryString() {
        return queryString;
    }

    public int getDimensionality() {
        return dimensionality;
    }

    public QueryType getQueryType() {
        return queryType;
    }
}
