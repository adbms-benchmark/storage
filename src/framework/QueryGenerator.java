package framework;

import data.BenchmarkQuery;
import util.Pair;

import java.util.List;
import java.util.Map;

/**
 *
 * @author George Merticariu
 */
public abstract class QueryGenerator {

    protected int noOfDimensions;

    public abstract List<BenchmarkQuery> getBenchmarkQueries();

    public abstract BenchmarkQuery getMiddlePointQuery();
}