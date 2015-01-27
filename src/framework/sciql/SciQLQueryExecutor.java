package framework.sciql;

import data.DataGenerator;
import data.DomainGenerator;
import framework.ProcessExecutor;
import framework.QueryExecutor;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.asqldb.util.TimerUtil;
import util.IO;
import util.Pair;

/**
 * @author Dimitar Misev
 */
public class SciQLQueryExecutor extends QueryExecutor {

    private DataGenerator dataGenerator;
    private final DomainGenerator domainGenerator;
    private final SciQLSystemController systemController;
    private final BenchmarkContext benchContext;
    private final int noOfDimensions;

    public SciQLQueryExecutor(ConnectionContext context, SciQLSystemController systemController,
            BenchmarkContext benchContext, int noOfDimensions) {
        super(context);
        this.domainGenerator = new DomainGenerator(noOfDimensions);
        this.systemController = systemController;
        this.benchContext = benchContext;
        this.noOfDimensions = noOfDimensions;
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("sciql query");
        SciQLConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("sciql query");
        TimerUtil.clearTimers();
//        System.out.println("time: " + result + " ms");
        return result;
    }

    public long executeTimedQueryUpdate(String query) {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("sciql query update");
        SciQLConnection.executeUpdateQuery(query);
        long result = TimerUtil.getElapsedMilli("sciql query update");
        TimerUtil.clearTimers();
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

        String collName = benchContext.getCollName1();
        if (SciQLConnection.tableExists(collName)) {
            System.out.println("Collection " + collName + " found, not reingesting.");
            return;
        }
        System.out.println("--------------------------------------------------------------------------");
        System.out.println("Creating collection " + collName);

        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchContext.getCollSize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        dataGenerator = new DataGenerator(fileSize);
        String filePath = dataGenerator.getFilePath();

        StringBuilder createArrayQuery = new StringBuilder();
        createArrayQuery.append("CREATE ARRAY ");
        createArrayQuery.append(collName);
        createArrayQuery.append(" (");

        for (int i = 0; i < domainBoundaries.size(); i++) {
            createArrayQuery.append("axis");
            createArrayQuery.append(i);
            createArrayQuery.append(" INT DIMENSION [");
            Pair<Long, Long> axisDomain = domainBoundaries.get(i);
            createArrayQuery.append(axisDomain.getFirst());
            createArrayQuery.append(":1:");
            createArrayQuery.append(axisDomain.getSecond());
            createArrayQuery.append("]");
            createArrayQuery.append(", ");
        }

        createArrayQuery.append(" v TINYINT");
        createArrayQuery.append(')');

        executeTimedQueryUpdate(createArrayQuery.toString());

        String outFilePath = IO.concatPaths(benchContext.getDataDir(), collName + ".sql");
        File outFile = new File(outFilePath);
        if (!outFile.exists()) {
            System.out.print("generating data... ");
            byte[] data = IO.readFile(filePath);

            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outFilePath), Charset.defaultCharset())) {
                writer.write("COPY " + fileSize + " RECORDS INTO \"sys\".\"" + collName + "\" FROM stdin USING DELIMITERS '\t','\\n','\"';");
                writer.newLine();
                if (noOfDimensions == 1) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    int j = 0;
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        StringBuilder b = new StringBuilder();
                        b.append(i1).append(' ')
                                .append(data[j++]);
                        writer.write(b.toString());
                        writer.newLine();
                    }
                } else if (noOfDimensions == 2) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    int j = 0;
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            StringBuilder b = new StringBuilder();
                            b.append(i1).append(' ')
                                    .append(i2).append(' ')
                                    .append(data[j++]);
                            writer.write(b.toString());
                            writer.newLine();
                        }
                    }
                } else if (noOfDimensions == 3) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    Pair<Long, Long> d3 = domainBoundaries.get(2);
                    int j = 0;
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                StringBuilder b = new StringBuilder();
                                b.append(i1).append(' ')
                                        .append(i2).append(' ')
                                        .append(i3).append(' ')
                                        .append(data[j++]);
                                writer.write(b.toString());
                                writer.newLine();
                            }
                        }
                    }
                } else if (noOfDimensions == 4) {
                    Pair<Long, Long> d1 = domainBoundaries.get(0);
                    Pair<Long, Long> d2 = domainBoundaries.get(1);
                    Pair<Long, Long> d3 = domainBoundaries.get(2);
                    Pair<Long, Long> d4 = domainBoundaries.get(3);
                    int j = 0;
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                for (long i4 = d4.getFirst(); i4 <= d4.getSecond(); i4++) {
                                    StringBuilder b = new StringBuilder();
                                    b.append(i1).append(' ')
                                            .append(i2).append(' ')
                                            .append(i3).append(' ')
                                            .append(i4).append(' ')
                                            .append(data[j++]);
                                    writer.write(b.toString());
                                    writer.newLine();
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
                    int j = 0;
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                for (long i4 = d4.getFirst(); i4 <= d4.getSecond(); i4++) {
                                    for (long i5 = d5.getFirst(); i5 <= d5.getSecond(); i5++) {
                                        StringBuilder b = new StringBuilder();
                                        b.append(i1).append(' ')
                                                .append(i2).append(' ')
                                                .append(i3).append(' ')
                                                .append(i4).append(' ')
                                                .append(i5).append(' ')
                                                .append(data[j++]);
                                        writer.write(b.toString());
                                        writer.newLine();
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
                    int j = 0;
                    for (long i1 = d1.getFirst(); i1 <= d1.getSecond(); i1++) {
                        for (long i2 = d2.getFirst(); i2 <= d2.getSecond(); i2++) {
                            for (long i3 = d3.getFirst(); i3 <= d3.getSecond(); i3++) {
                                for (long i4 = d4.getFirst(); i4 <= d4.getSecond(); i4++) {
                                    for (long i5 = d5.getFirst(); i5 <= d5.getSecond(); i5++) {
                                        for (long i6 = d6.getFirst(); i6 <= d6.getSecond(); i6++) {
                                            StringBuilder b = new StringBuilder();
                                            b.append(i1).append(' ')
                                                    .append(i2).append(' ')
                                                    .append(i3).append(' ')
                                                    .append(i4).append(' ')
                                                    .append(i5).append(' ')
                                                    .append(i6).append(' ')
                                                    .append(data[j++]);
                                            writer.write(b.toString());
                                            writer.newLine();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            System.out.println("ok.");
        }

        TimerUtil.clearTimers();
        TimerUtil.startTimer("sciql query update");
        ProcessExecutor executor = new ProcessExecutor(systemController.getMclientPath(), "-d", "benchmark");
        executor.executeRedirectInput(outFilePath);
        long result = TimerUtil.getElapsedMilli("sciql query update");
        TimerUtil.clearTimers();
        System.out.println("time: " + result + " ms");
    }

    @Override
    public void dropCollection() {

    }
}
