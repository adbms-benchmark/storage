package framework.rasdaman;

import data.DataGenerator;
import data.DomainGenerator;
import framework.QueryExecutor;
import framework.SystemController;
import framework.context.BenchmarkContext;
import framework.context.RasdamanContext;
import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import util.DomainUtil;
import util.IO;
import util.Pair;

/**
 * @author George Merticariu
 */
public class RasdamanQueryExecutor extends QueryExecutor<RasdamanContext> {

    private DataGenerator dataGenerator;
    private DomainGenerator domainGenerator;
    private RasdamanSystemController rasdamanSystemController;
    private int noOfDimensions;
    private BenchmarkContext benchContext;

    public RasdamanQueryExecutor(RasdamanContext context, RasdamanSystemController rasdamanSystemController, BenchmarkContext benchContext, int noOfDimensions) {
        super(context);
        domainGenerator = new DomainGenerator(noOfDimensions);
        this.rasdamanSystemController = rasdamanSystemController;
        this.noOfDimensions = noOfDimensions;
        this.benchContext = benchContext;
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        List<String> commandList = new ArrayList<>();
        //TODO-GM: read rasql path from config file
        commandList.add(context.getExecuteQueryBin());
        commandList.add("-q");
        commandList.add(query);
        Collections.addAll(commandList, args);

        long startTime = System.currentTimeMillis();
        int status = SystemController.executeShellCommand(commandList.toArray(new String[]{}));
        long result = System.currentTimeMillis() - startTime;

        if (status != 0) {
            System.out.println("failed, restarting system..,");
            rasdamanSystemController.restartSystem();
            startTime = System.currentTimeMillis();
            status = SystemController.executeShellCommand(commandList.toArray(new String[]{}));
            result = System.currentTimeMillis() - startTime;
            if (status != 0) {
                throw new Exception(String.format("Query execution failed with status %d", status));
            }
        }

        return result;
    }

    @Override
    public void createCollection() throws Exception {
        long oneGB = 1024l * 1024l * 1024l;
        int slices = (int) (benchContext.getCollSize() / (oneGB));
        if (benchContext.getCollSize() > oneGB) {
            benchContext.setCollSize(oneGB);
        }

        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchContext.getCollSize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        long chunkSize = DomainUtil.getTileDimensionUpperBound(noOfDimensions, benchContext.getCollTileSize());
        long tileSize = (long) Math.pow(chunkSize + 1l, noOfDimensions);

        dataGenerator = new DataGenerator(fileSize);
        String filePath = dataGenerator.getFilePath();

        List<Pair<Long, Long>> tileStructureDomain = new ArrayList<>();
        for (int i = 0; i < noOfDimensions; i++) {
            tileStructureDomain.add(Pair.of(0l, chunkSize));
        }

        Pair<String, String> aChar = rasdamanSystemController.createRasdamanType(noOfDimensions, "char");

        String createCollectionQuery = String.format("CREATE COLLECTION %s %s", benchContext.getCollName1(), aChar.getSecond());
        executeTimedQuery(createCollectionQuery, new String[]{
                "--user", context.getUser(),
                "--passwd", context.getPassword()});

        String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING ALIGNED %s TILE SIZE %d", benchContext.getCollName1(), RasdamanQueryGenerator.convertToRasdamanDomain(tileStructureDomain), tileSize);
        System.out.println("Executing insert query: " + insertQuery);
        long insertTime = executeTimedQuery(insertQuery, new String[]{
                "--user", context.getUser(),
                "--passwd", context.getPassword(),
                "--mddtype", aChar.getFirst(),
                "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries),
                "--file", filePath});

        File resultsDir = IO.getResultsDir();
        File insertResultFile = new File(resultsDir.getAbsolutePath(), "rasdaman_insert_results.csv");

        if (benchContext.getCollSize() > oneGB) {
            insertTime = updateCollection(slices);
        }

        IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"", benchContext.getCollName1(), fileSize, chunkSize + 1l, noOfDimensions, insertTime));
    }

    /**
     * Hack for inserting datasizes of more than 1Gb
     *
     * @param slices
     * @throws Exception
     */
    public long updateCollection(int slices) throws Exception {
        Pair<String, String> aChar = rasdamanSystemController.createRasdamanType(noOfDimensions, "char");
        long executionTime = 1;
        switch (noOfDimensions) {
            case 1: {
                double approxSlicePerDim = Math.pow(slices, 1 / ((double) noOfDimensions));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchContext.getCollSize()) / Math.pow(slicesPerDim, noOfDimensions));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                DataGenerator dataGenerator = new DataGenerator(fileSize);
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();

                System.out.println("No of updates: " + Math.pow(slicesPerDim, noOfDimensions));
                for (int axis0 = 0; axis0 < slicesPerDim; axis0++) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                    shiftedAxis.add(shiftedAxis0);

                    String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchContext.getCollName1());
                    boolean success = false;

                    while (!success) {
                        try {
                            executionTime = executeTimedQuery(updateQuery, new String[]{
                                    "--user", context.getUser(),
                                    "--passwd", context.getPassword(),
                                    "--mddtype", aChar.getFirst(),
                                    "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                    "--file", filePath});
                            success = true;
                            rasdamanSystemController.restartSystem();
                        } catch (Exception ex) {
                            rasdamanSystemController.restartSystem();
                        }
                    }
                }

                executionTime = (long) (executionTime * Math.pow(slicesPerDim, noOfDimensions));
                break;
            }
            case 2: {
                double approxSlicePerDim = Math.pow(slices, 1 / ((double) noOfDimensions));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchContext.getCollSize()) / Math.pow(slicesPerDim, noOfDimensions));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                DataGenerator dataGenerator = new DataGenerator(fileSize);
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, noOfDimensions));
                for (int axis0 = 0; axis0 < slicesPerDim; ++axis0) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    for (int axis1 = 0; axis1 < slicesPerDim; ++axis1) {
                        Pair<Long, Long> shiftedAxis1 = Pair.of(axis1Domain.getFirst() + axis1 * shift1, axis1Domain.getSecond() + axis1 * shift1);
                        List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                        shiftedAxis.add(shiftedAxis0);
                        shiftedAxis.add(shiftedAxis1);

                        String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchContext.getCollName1());
                        boolean success = false;

                        while (!success) {
                            try {
                                executionTime = executeTimedQuery(updateQuery, new String[]{
                                        "--user", context.getUser(),
                                        "--passwd", context.getPassword(),
                                        "--mddtype", aChar.getFirst(),
                                        "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                        "--file", filePath});
                                success = true;
                                rasdamanSystemController.restartSystem();
                            } catch (Exception ex) {
                                rasdamanSystemController.restartSystem();
                            }
                        }
                    }


                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, noOfDimensions));

                break;
            }
            case 3: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) noOfDimensions));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchContext.getCollSize()) / Math.pow(slicesPerDim, noOfDimensions));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                DataGenerator dataGenerator = new DataGenerator(fileSize);
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);
                Pair<Long, Long> axis2Domain = domainBoundaries.get(2);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                long shift2 = axis2Domain.getSecond() - axis2Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, noOfDimensions));
                for (int axis0 = 0; axis0 < slicesPerDim; ++axis0) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    for (int axis1 = 0; axis1 < slicesPerDim; ++axis1) {
                        Pair<Long, Long> shiftedAxis1 = Pair.of(axis1Domain.getFirst() + axis1 * shift1, axis1Domain.getSecond() + axis1 * shift1);
                        for (int axis2 = 0; axis2 < slicesPerDim; ++axis2) {
                            Pair<Long, Long> shiftedAxis2 = Pair.of(axis2Domain.getFirst() + axis2 * shift2, axis2Domain.getSecond() + axis2 * shift2);

                            List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                            shiftedAxis.add(shiftedAxis0);
                            shiftedAxis.add(shiftedAxis1);
                            shiftedAxis.add(shiftedAxis2);

                            String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchContext.getCollName1());
                            boolean success = false;

                            while (!success) {
                                try {
                                    executionTime = executeTimedQuery(updateQuery, new String[]{
                                            "--user", context.getUser(),
                                            "--passwd", context.getPassword(),
                                            "--mddtype", aChar.getFirst(),
                                            "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                            "--file", filePath});
                                    success = true;
                                } catch (Exception ex) {
                                    rasdamanSystemController.restartSystem();
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, noOfDimensions));
                break;
            }
            case 4: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) noOfDimensions));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchContext.getCollSize()) / Math.pow(slicesPerDim, noOfDimensions));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                DataGenerator dataGenerator = new DataGenerator(fileSize);
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);
                Pair<Long, Long> axis2Domain = domainBoundaries.get(2);
                Pair<Long, Long> axis3Domain = domainBoundaries.get(3);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                long shift2 = axis2Domain.getSecond() - axis2Domain.getFirst();
                long shift3 = axis3Domain.getSecond() - axis3Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, noOfDimensions));
                for (int axis0 = 0; axis0 < slicesPerDim; ++axis0) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    for (int axis1 = 0; axis1 < slicesPerDim; ++axis1) {
                        Pair<Long, Long> shiftedAxis1 = Pair.of(axis1Domain.getFirst() + axis1 * shift1, axis1Domain.getSecond() + axis1 * shift1);
                        for (int axis2 = 0; axis2 < slicesPerDim; ++axis2) {
                            Pair<Long, Long> shiftedAxis2 = Pair.of(axis2Domain.getFirst() + axis2 * shift2, axis2Domain.getSecond() + axis2 * shift2);
                            for (int axis3 = 0; axis3 < slicesPerDim; ++axis3) {
                                Pair<Long, Long> shiftedAxis3 = Pair.of(axis3Domain.getFirst() + axis3 * shift3, axis3Domain.getSecond() + axis3 * shift3);

                                List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                                shiftedAxis.add(shiftedAxis0);
                                shiftedAxis.add(shiftedAxis1);
                                shiftedAxis.add(shiftedAxis2);
                                shiftedAxis.add(shiftedAxis3);

                                String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchContext.getCollName1());
                                boolean success = false;

                                while (!success) {
                                    try {
                                        executionTime = executeTimedQuery(updateQuery, new String[]{
                                                "--user", context.getUser(),
                                                "--passwd", context.getPassword(),
                                                "--mddtype", aChar.getFirst(),
                                                "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                                "--file", filePath});
                                        success = true;
                                    } catch (Exception ex) {
                                        rasdamanSystemController.restartSystem();
                                    }
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, noOfDimensions));
                break;
            }
            case 5: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) noOfDimensions));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchContext.getCollSize()) / Math.pow(slicesPerDim, noOfDimensions));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                DataGenerator dataGenerator = new DataGenerator(fileSize);
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);
                Pair<Long, Long> axis2Domain = domainBoundaries.get(2);
                Pair<Long, Long> axis3Domain = domainBoundaries.get(3);
                Pair<Long, Long> axis4Domain = domainBoundaries.get(4);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                long shift2 = axis2Domain.getSecond() - axis2Domain.getFirst();
                long shift3 = axis3Domain.getSecond() - axis3Domain.getFirst();
                long shift4 = axis4Domain.getSecond() - axis4Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, noOfDimensions));
                int insertNo = 0;
                for (int axis0 = 0; axis0 < slicesPerDim; ++axis0) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    for (int axis1 = 0; axis1 < slicesPerDim; ++axis1) {
                        Pair<Long, Long> shiftedAxis1 = Pair.of(axis1Domain.getFirst() + axis1 * shift1, axis1Domain.getSecond() + axis1 * shift1);
                        for (int axis2 = 0; axis2 < slicesPerDim; ++axis2) {
                            Pair<Long, Long> shiftedAxis2 = Pair.of(axis2Domain.getFirst() + axis2 * shift2, axis2Domain.getSecond() + axis2 * shift2);
                            for (int axis3 = 0; axis3 < slicesPerDim; ++axis3) {
                                Pair<Long, Long> shiftedAxis3 = Pair.of(axis3Domain.getFirst() + axis3 * shift3, axis3Domain.getSecond() + axis3 * shift3);
                                for (int axis4 = 0; axis4 < slicesPerDim; ++axis4) {
                                    Pair<Long, Long> shiftedAxis4 = Pair.of(axis4Domain.getFirst() + axis4 * shift4, axis4Domain.getSecond() + axis4 * shift4);

                                    List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                                    shiftedAxis.add(shiftedAxis0);
                                    shiftedAxis.add(shiftedAxis1);
                                    shiftedAxis.add(shiftedAxis2);
                                    shiftedAxis.add(shiftedAxis3);
                                    shiftedAxis.add(shiftedAxis4);

                                    String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchContext.getCollName1());
                                    boolean success = false;
                                    insertNo++;
                                    System.out.println("Performing insert: " + insertNo);
                                    while (!success) {
                                        try {
                                            executionTime = executeTimedQuery(updateQuery, new String[]{
                                                    "--user", context.getUser(),
                                                    "--passwd", context.getPassword(),
                                                    "--mddtype", aChar.getFirst(),
                                                    "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                                    "--file", filePath});
                                            success = true;

                                            rasdamanSystemController.restartSystem();
                                        } catch (Exception ex) {
                                            rasdamanSystemController.restartSystem();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, noOfDimensions));
                break;
            }
            case 6: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) noOfDimensions));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchContext.getCollSize()) / Math.pow(slicesPerDim, noOfDimensions));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                DataGenerator dataGenerator = new DataGenerator(fileSize);
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);
                Pair<Long, Long> axis2Domain = domainBoundaries.get(2);
                Pair<Long, Long> axis3Domain = domainBoundaries.get(3);
                Pair<Long, Long> axis4Domain = domainBoundaries.get(4);
                Pair<Long, Long> axis5Domain = domainBoundaries.get(5);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                long shift2 = axis2Domain.getSecond() - axis2Domain.getFirst();
                long shift3 = axis3Domain.getSecond() - axis3Domain.getFirst();
                long shift4 = axis4Domain.getSecond() - axis4Domain.getFirst();
                long shift5 = axis5Domain.getSecond() - axis5Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, noOfDimensions));
                int insertNo = 0;
                for (int axis0 = 0; axis0 < slicesPerDim; ++axis0) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    for (int axis1 = 0; axis1 < slicesPerDim; ++axis1) {
                        Pair<Long, Long> shiftedAxis1 = Pair.of(axis1Domain.getFirst() + axis1 * shift1, axis1Domain.getSecond() + axis1 * shift1);
                        for (int axis2 = 0; axis2 < slicesPerDim; ++axis2) {
                            Pair<Long, Long> shiftedAxis2 = Pair.of(axis2Domain.getFirst() + axis2 * shift2, axis2Domain.getSecond() + axis2 * shift2);
                            for (int axis3 = 0; axis3 < slicesPerDim; ++axis3) {
                                Pair<Long, Long> shiftedAxis3 = Pair.of(axis3Domain.getFirst() + axis3 * shift3, axis3Domain.getSecond() + axis3 * shift3);
                                for (int axis4 = 0; axis4 < slicesPerDim; ++axis4) {
                                    Pair<Long, Long> shiftedAxis4 = Pair.of(axis4Domain.getFirst() + axis4 * shift4, axis4Domain.getSecond() + axis4 * shift4);
                                    for (int axis5 = 0; axis5 < slicesPerDim; ++axis5) {
                                        Pair<Long, Long> shiftedAxis5 = Pair.of(axis5Domain.getFirst() + axis5 * shift5, axis5Domain.getSecond() + axis5 * shift5);

                                        List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                                        shiftedAxis.add(shiftedAxis0);
                                        shiftedAxis.add(shiftedAxis1);
                                        shiftedAxis.add(shiftedAxis2);
                                        shiftedAxis.add(shiftedAxis3);
                                        shiftedAxis.add(shiftedAxis4);
                                        shiftedAxis.add(shiftedAxis5);

                                        String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchContext.getCollName1());
                                        boolean success = false;
                                        insertNo++;
                                        System.out.println("Performing insert: " + insertNo);
                                        while (!success) {
                                            try {
                                                executionTime = executeTimedQuery(updateQuery, new String[]{
                                                        "--user", context.getUser(),
                                                        "--passwd", context.getPassword(),
                                                        "--mddtype", aChar.getFirst(),
                                                        "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                                        "--file", filePath});
                                                success = true;
                                                rasdamanSystemController.restartSystem();
                                            } catch (Exception ex) {
                                                rasdamanSystemController.restartSystem();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, noOfDimensions));
                break;
            }

        }
        return executionTime;
    }

    @Override
    public void dropCollection() throws Exception {
        String dropCollectionQuery = MessageFormat.format("DROP COLLECTION {0}", benchContext.getCollName1());
        executeTimedQuery(dropCollectionQuery, new String[]{
                "--user", context.getUser(),
                "--passwd", context.getPassword()
        });
    }
}
