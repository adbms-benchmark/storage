package benchmark;

import java.util.ArrayList;
import java.util.List;

/**
 * A collection of queries to be benchmarked in a single session.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class BenchmarkSession {
    
    private String description;
    private List<BenchmarkQuery> queries;

    public BenchmarkSession(String description) {
        this.queries = new ArrayList<>();
        this.description = description;
    }
    
    public BenchmarkSession(String description, BenchmarkQuery query) {
        this(description);
        this.queries.add(query);
    }

    public String getDescription() {
        return description;
    }
    
    public List<BenchmarkQuery> getBenchmarkQueries() {
        return queries;
    }
    
    public void addBenchmarkQuery(BenchmarkQuery query) {
        this.queries.add(query);
    }
}
