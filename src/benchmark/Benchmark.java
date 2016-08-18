/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmark;

import java.util.ArrayList;
import java.util.List;

/**
 * A list of BenchmarkSessions to be executed in a single Benchmark.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class Benchmark {
    
    private List<BenchmarkSession> sessions;

    public Benchmark() {
        this.sessions = new ArrayList<>();
    }
    
    public Benchmark(BenchmarkSession session) {
        this();
        this.sessions.add(session);
    }
    
    public Benchmark(BenchmarkQuery query) {
        this();
        add(query);
    }

    public List<BenchmarkSession> getBenchmarkSessions() {
        return sessions;
    }
    
    public void add(BenchmarkQuery query) {
        String description = String.valueOf(sessions.size() + 1);
        add(new BenchmarkSession(description, query));
    }
    
    public void add(BenchmarkSession session) {
        this.sessions.add(session);
    }
    
    public void clear() {
        this.sessions.clear();
    }
    
}
