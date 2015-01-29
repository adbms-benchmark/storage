package framework;

import data.BenchmarkQuery;

import java.util.List;

/**
 *
 * @author George Merticariu
 */
public abstract class QueryGenerator {

    protected int noOfDimensions;

    public abstract List<BenchmarkQuery> getBenchmarkQueries();
}