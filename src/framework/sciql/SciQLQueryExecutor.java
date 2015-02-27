package framework.sciql;

import data.DataGenerator;
import data.DomainGenerator;
import framework.ProcessExecutor;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import framework.context.BenchmarkContextGenerator;
import framework.context.BenchmarkContextJoin;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.asqldb.util.TimerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static util.DomainUtil.SIZE_100MB;
import util.IO;
import util.Pair;

/**
 * @author Dimitar Misev
 */
public class SciQLQueryExecutor extends QueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(SciQLQueryExecutor.class);

    private final SciQLSystem systemController;
    //
    private final List<SciQLInputData> inputs;
    private SciQLInputData input;
    private byte[] data;
    private int dataIndex;

    public SciQLQueryExecutor(BenchmarkContext benchContext, SciQLSystem systemController) {
        super(systemController, benchContext);
        this.systemController = systemController;
        this.inputs = new ArrayList<>();
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        TimerUtil.startTimer("sciql query");
        SciQLConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("sciql query");
        TimerUtil.removeTimer("sciql query");
//        System.out.println("time: " + result + " ms");
        return result;
    }

    public long executeTimedQueryUpdate(String query) {
        TimerUtil.startTimer("sciql query update");
        SciQLConnection.executeUpdateQuery(query);
        long result = TimerUtil.getElapsedMilli("sciql query update");
        TimerUtil.removeTimer("sciql query update");
        System.out.println("time: " + result + " ms");
        return result;
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
    public void createCollection() throws Exception {

        QueryGenerator queryGenerator = systemController.getQueryGenerator(benchContext);
        List<Pair<String, BenchmarkContext>> createQueries = queryGenerator.getCreateQueries();

        for (Pair<String, BenchmarkContext> createQuery : createQueries) {
            BenchmarkContext bc = createQuery.getSecond();
            noOfDimensions = bc.getArrayDimensionality();
            String arrayName = bc.getArrayName();
            if (SciQLConnection.tableExists(arrayName)) {
                log.info("Array " + arrayName + " found, not reingesting.");
                return;
            }
            log.info("Creating array " + arrayName);

            DomainGenerator dg = new DomainGenerator(bc.getArrayDimensionality());
            List<Pair<Long, Long>> domainBoundaries = dg.getDomainBoundaries(bc.getArraySize());
            long fileSize = dg.getFileSize(domainBoundaries);

            executeTimedQueryUpdate(createQuery.getFirst());

            String outFilePath = IO.concatPaths(bc.getDataDir(), arrayName + "-1.sql");
            File outFile = new File(outFilePath);
            if (!outFile.exists()) {
                log.debug("Generating random data...");

                updateWriters(bc, fileSize);
                input = new SciQLInputData(null, 0, -1, -1, "");

                int j = 0;
                if (noOfDimensions == 1) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        write(j++, i1);
                    }
                } else if (noOfDimensions == 2) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            write(j++, i1, i2);
                        }
                    }
                } else if (noOfDimensions == 3) {
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
                } else if (noOfDimensions == 4) {
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
                } else if (noOfDimensions == 5) {
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
                } else if (noOfDimensions == 6) {
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

            TimerUtil.clearTimers();
            TimerUtil.startTimer("up");
            SciQLConnection.close();
            for (SciQLInputData in : inputs) {
                ProcessExecutor executor = new ProcessExecutor(systemController.getMclientPath(), "-d", "benchmark");
                log.debug(" -> inserting file " + in.file);
                executor.executeRedirectInput(in.file);
            }
            SciQLConnection.open(systemController);
            executeTimedQueryUpdate("DELETE FROM " + bc.getArrayName().toLowerCase() + " WHERE v is NULL");
            long insertTime = TimerUtil.getElapsedMilli("up");
            TimerUtil.clearTimers();
            System.out.println("time: " + insertTime + " ms");

            File resultsDir = IO.getResultsDir();
            File insertResultFile = new File(resultsDir.getAbsolutePath(), "SciQL_insert_results.csv");
            IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"",
                    bc.getArrayName(), fileSize, -1, bc.getArrayDimensionality(), insertTime));
        }
    }

    private void write(int j, long... indexes) throws IOException {
        if (j > input.to) {
            input = getWriter(j);
            dataGenerator = new DataGenerator(input.size, benchContext.getDataDir());
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
    public void dropCollection() {
        if (benchContext.isSqlMdaBenchmark()) {
            List<BenchmarkContext> benchContexts = BenchmarkContextGenerator.generate(benchContext);
            for (BenchmarkContext bc : benchContexts) {
                if (bc instanceof BenchmarkContextJoin) {
                    BenchmarkContext[] joinedContexts = ((BenchmarkContextJoin) bc).getBenchmarkContexts();
                    for (BenchmarkContext joinedContext : joinedContexts) {
                        executeTimedQueryUpdate("DROP ARRAY " + joinedContext.getArrayName());
                    }
                } else {
                    executeTimedQueryUpdate("DROP ARRAY " + bc.getArrayName());
                }
            }
        }
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
