/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system.sciql;

import data.RandomDataGenerator;
import data.DomainGenerator;
import benchmark.DataManager;
import benchmark.QueryExecutor;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.sqlmda.BenchmarkContextGenerator;
import benchmark.sqlmda.BenchmarkContextJoin;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static util.DomainUtil.SIZE_100MB;
import util.IO;
import util.Pair;
import util.ProcessExecutor;
import util.StopWatch;

/**
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciQLSqlMdaBenchmarkDataManager extends DataManager<SciQLSystem> {
    
    private static final Logger log = LoggerFactory.getLogger(SciQLSqlMdaBenchmarkDataManager.class);
    
    private final List<SciQLInputData> inputs;
    private SciQLInputData input;
    private byte[] data;
    private int dataIndex;

    public SciQLSqlMdaBenchmarkDataManager(SciQLSystem systemController, 
            QueryExecutor<SciQLSystem> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);;
        this.inputs = new ArrayList<>();
    }
    

    /**
     * Array is created as:
     *
     * CREATE ARRAY a_SizeInBytesB_dimensionD (d0 INT DIMENSION[..], d1 ..., v
     * TINYINT)
     *
     * @throws Exception
     */
    @Override
    public long loadData() throws Exception {
        QueryGenerator queryGenerator = systemController.getQueryGenerator(benchmarkContext);
        List<Pair<String, BenchmarkContext>> createQueries = queryGenerator.getCreateQueries();
        long totalTime = 0;

        for (Pair<String, BenchmarkContext> createQuery : createQueries) {
            BenchmarkContext bc = createQuery.getSecond();
            String arrayName = bc.getArrayName();
            if (SciQLConnection.tableExists(arrayName)) {
                log.info("Array " + arrayName + " found, not reingesting.");
                return totalTime;
            }
            log.info("Creating array " + arrayName);

            DomainGenerator dg = new DomainGenerator(bc.getArrayDimensionality());
            List<Pair<Long, Long>> domainBoundaries = dg.getDomainBoundaries(bc.getArraySize());
            long fileSize = dg.getFileSize(domainBoundaries);

            queryExecutor.executeTimedQueryUpdate(createQuery.getFirst(), null);

            String outFilePath = IO.concatPaths(bc.getDataDir(), arrayName + "-1.sql");
            File outFile = new File(outFilePath);
            if (!outFile.exists()) {
                log.debug("Generating random data...");

                updateWriters(bc, fileSize);
                input = new SciQLInputData(null, 0, -1, -1, "");

                int j = 0;
                if (bc.getArrayDimensionality() == 1) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        write(j++, i1);
                    }
                } else if (bc.getArrayDimensionality() == 2) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            write(j++, i1, i2);
                        }
                    }
                } else if (bc.getArrayDimensionality() == 3) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    Pair<Long, Long> d3 = domainBoundaries.get(2);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                write(j++, i1, i2, i3);
                            }
                        }
                    }
                } else if (bc.getArrayDimensionality() == 4) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    Pair<Long, Long> d3 = domainBoundaries.get(2);
                    Pair<Long, Long> d4 = domainBoundaries.get(3);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                for (long i4 = d4.getFirst(); i4 <= d4.getSecond(); i4++) {
                                    write(j++, i1, i2, i3, i4);
                                }
                            }
                        }
                    }
                } else if (bc.getArrayDimensionality() == 5) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    Pair<Long, Long> d3 = domainBoundaries.get(2);
                    Pair<Long, Long> d4 = domainBoundaries.get(3);
                    Pair<Long, Long> d5 = domainBoundaries.get(4);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                for (long i4 = d4.getFirst(); i4 <= d4.getSecond(); i4++) {
                                    for (long i5 = d5.getFirst(); i5 <= d5.getSecond(); i5++) {
                                        write(j++, i1, i2, i3, i4, i5);
                                    }
                                }
                            }
                        }
                    }
                } else if (bc.getArrayDimensionality() == 6) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    Pair<Long, Long> d3 = domainBoundaries.get(2);
                    Pair<Long, Long> d4 = domainBoundaries.get(3);
                    Pair<Long, Long> d5 = domainBoundaries.get(4);
                    Pair<Long, Long> d6 = domainBoundaries.get(5);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                for (long i4 = d4.getFirst(); i4 <= d4.getSecond(); i4++) {
                                    for (long i5 = d5.getFirst(); i5 <= d5.getSecond(); i5++) {
                                        for (long i6 = d6.getFirst(); i6 <= d6.getSecond(); i6++) {
                                            write(j++, i1, i2, i3, i4, i5, i6);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                System.out.println("ok.");
            } else {
                updateWriters(bc, fileSize);
            }
            closeWriters();

            StopWatch timer = new StopWatch();
            SciQLConnection.close();
            for (SciQLInputData in : inputs) {
                ProcessExecutor executor = new ProcessExecutor(systemController.getMclientPath(), "-d", "benchmark");
                log.debug(" -> inserting file " + in.file);
                executor.executeRedirectInput(in.file);
            }
            SciQLConnection.open(systemController);
            queryExecutor.executeTimedQueryUpdate("DELETE FROM " + bc.getArrayName().toLowerCase() + " WHERE v is NULL", null);
            long insertTime = timer.getElapsedTime();
            totalTime += insertTime;
            System.out.println("time: " + insertTime + " ms");

            File resultsDir = IO.getResultsDir();
            File insertResultFile = new File(resultsDir.getAbsolutePath(), "SciQL_insert_results.csv");
            IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"",
                    bc.getArrayName(), fileSize, -1, bc.getArrayDimensionality(), insertTime));
            
        }
        return totalTime;
    }

    private void write(int j, long... indexes) throws IOException {
        if (j > input.to) {
            input = getWriter(j);
            dataGenerator = new RandomDataGenerator(input.size, benchmarkContext.getDataDir());
            String filePath = dataGenerator.getFilePath();
            data = IO.readFile(filePath);
            log.trace(" -> read file of size " + data.length);
            dataIndex = 0;
        }

        StringBuilder b = new StringBuilder();
        for (long index : indexes) {
            b.append(index).append(' ');
        }
        byte val = data[dataIndex++];
        if (val == -128) {
            val = -127;
        }
        b.append(val);
        input.writer.write(b.toString());
        input.writer.newLine();
    }

    private void updateWriters(BenchmarkContext benchContext, long fileSize) throws IOException {
        inputs.clear();
        long partsNo = fileSize / SIZE_100MB;
        log.trace("Updating writers, file size " + fileSize);
        if (partsNo == 0) {
            log.trace(" -> single part");
            String partFileName = IO.concatPaths(benchContext.getDataDir(), benchContext.getArrayName() + "-1.sql");
            File check = new File(partFileName);
            BufferedWriter writer = null;
            if (!check.exists()) {
                writer = Files.newBufferedWriter(Paths.get(partFileName), Charset.defaultCharset());
                writer.write("COPY " + fileSize + " RECORDS INTO \"sys\".\"" + benchContext.getArrayName().toLowerCase() + "\" FROM stdin USING DELIMITERS ' ','\\n','\"';");
                writer.newLine();
            }
            SciQLInputData input = new SciQLInputData(writer, 0, fileSize - 1, fileSize, partFileName);
            inputs.add(input);
        } else {
            log.trace(" -> " + partsNo + " number of files");
            for (int i = 1; i <= partsNo; i++) {
                String partFileName = IO.concatPaths(benchContext.getDataDir(), benchContext.getArrayName() + "-" + i + ".sql");
                File check = new File(partFileName);
                BufferedWriter writer = null;
                if (!check.exists()) {
                    writer = Files.newBufferedWriter(Paths.get(partFileName), Charset.defaultCharset());
                    writer.write("COPY " + SIZE_100MB + " RECORDS INTO \"sys\".\"" + benchContext.getArrayName().toLowerCase() + "\" FROM stdin USING DELIMITERS ' ','\\n','\"';");
                    writer.newLine();
                }
                long from = (i - 1) * SIZE_100MB;
                long to = from + SIZE_100MB - 1;
                long size = SIZE_100MB;
                if (i == partsNo && (fileSize % SIZE_100MB) > 0) {
                    to = fileSize - 1;
                    size += (fileSize % SIZE_100MB);
                }
                SciQLInputData input = new SciQLInputData(writer, from, to, size, partFileName);
                log.trace(" -> new input: " + input);
                inputs.add(input);
            }
        }
    }

    private void closeWriters() throws IOException {
        for (SciQLInputData input : inputs) {
            if (input.writer != null) {
                input.close();
            }
        }
    }

    private SciQLInputData getWriter(long size) {
        long partNo = size / SIZE_100MB;
        if (partNo == inputs.size()) {
            --partNo;
        }
        return inputs.get((int) partNo);
    }

    @Override
    public long dropData() {
        StopWatch timer = new StopWatch();
        if (benchmarkContext.isSqlMdaBenchmark()) {
            List<BenchmarkContext> benchContexts = BenchmarkContextGenerator.generate(benchmarkContext);
            for (BenchmarkContext bc : benchContexts) {
                if (bc instanceof BenchmarkContextJoin) {
                    BenchmarkContext[] joinedContexts = ((BenchmarkContextJoin) bc).getBenchmarkContexts();
                    for (BenchmarkContext joinedContext : joinedContexts) {
                        queryExecutor.executeTimedQueryUpdate("DROP ARRAY " + joinedContext.getArrayName(), null);
                    }
                } else {
                    queryExecutor.executeTimedQueryUpdate("DROP ARRAY " + bc.getArrayName(), null);
                }
            }
        }
        return timer.getElapsedTime();
    }
}

class SciQLInputData {

    public final BufferedWriter writer;
    public final long from;
    public final long to;
    public final long size;
    public final String file;

    public SciQLInputData(BufferedWriter writer, long from, long to, long size, String file) {
        this.writer = writer;
        this.from = from;
        this.to = to;
        this.size = size;
        this.file = file;
    }

    public void close() throws IOException {
        writer.close();
    }

    @Override
    public String toString() {
        return "SciQLInputData{ " + "from=" + from + ", to=" + to + ", size=" + size + ", file=" + file + " }";
    }
}
