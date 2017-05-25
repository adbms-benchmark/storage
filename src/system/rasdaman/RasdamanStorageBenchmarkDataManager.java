package system.rasdaman;

import data.RandomDataGenerator;
import benchmark.DataManager;
import benchmark.BenchmarkContext;
import benchmark.storage.StorageBenchmarkContext;
import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import util.DomainUtil;
import util.IO;
import util.Pair;
import util.StopWatch;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class RasdamanStorageBenchmarkDataManager extends DataManager<RasdamanSystem> {
    
    private final RasdamanTypeManager typeManager;

    public RasdamanStorageBenchmarkDataManager(RasdamanSystem systemController, 
            RasdamanQueryExecutor queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
        typeManager = new RasdamanTypeManager(queryExecutor);
    }

    @Override
    public long dropData() throws Exception {
        String dropCollectionQuery = MessageFormat.format("DROP COLLECTION {0}", benchmarkContext.getArrayName());
        return queryExecutor.executeTimedQuery(dropCollectionQuery);
    }

    @Override
    public long loadData() throws Exception {
        StopWatch timer = new StopWatch();
        loadStorageBenchmarkData();
        return timer.getElapsedTime();
    }
    
    private void loadStorageBenchmarkData() throws Exception {
        int slices = (int) (benchmarkContext.getArraySize() / (DomainUtil.SIZE_1GB));
        
//        boolean startSequentialUpdate = false;
//        if (benchContext.getCollSize() > DomainUtil.SIZE_1GB) {
//            benchContext.setCollSize(DomainUtil.SIZE_1GB);
//            startSequentialUpdate = true;
//        }

        long insertTime = -1;

        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        long chunkSize = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), ((StorageBenchmarkContext)benchmarkContext).getTileSize());
        long tileSize = (long) Math.pow(chunkSize + 1l, benchmarkContext.getArrayDimensionality());

        dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
        String filePath = dataGenerator.getFilePath();

        List<Pair<Long, Long>> tileStructureDomain = new ArrayList<>();
        for (int i = 0; i < benchmarkContext.getArrayDimensionality(); i++) {
            tileStructureDomain.add(Pair.of(0l, chunkSize));
        }

        Pair<String, String> aChar = typeManager.createType(benchmarkContext.getArrayDimensionality(), "char");

        String createCollectionQuery = String.format("CREATE COLLECTION %s %s", benchmarkContext.getArrayName(), aChar.getSecond());
        queryExecutor.executeTimedQuery(createCollectionQuery);

        String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING ALIGNED %s TILE SIZE %d", 
                benchmarkContext.getArrayName(), RasdamanQueryGenerator.convertToRasdamanDomain(tileStructureDomain), tileSize);
        System.out.println("Executing insert query: " + insertQuery);
        insertTime = queryExecutor.executeTimedQuery(insertQuery,
                "--mddtype", aChar.getFirst(),
                "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries),
                "--file", filePath);

        File resultsDir = IO.getResultsDir();
        File insertResultFile = new File(resultsDir.getAbsolutePath(), "rasdaman_insert_results.csv");

//        if (startSequentialUpdate) {
//            insertTime = updateCollection(slices);
//        }

        IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"", 
                benchmarkContext.getArrayName(), fileSize*slices, chunkSize + 1l, benchmarkContext.getArrayDimensionality(), insertTime));
    }

    /**
     * Insert datasizes of more than 1Gb.
     *
     * @param slices
     * @throws Exception
     */
    public long updateCollection(int slices) throws Exception {
        Pair<String, String> aChar = typeManager.createType(benchmarkContext.getArrayDimensionality(), "char");
        long executionTime = 1;
        switch (benchmarkContext.getArrayDimensionality()) {
            case 1: {
                double approxSlicePerDim = Math.pow(slices, 1 / ((double) benchmarkContext.getArrayDimensionality()));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchmarkContext.getArraySize()) / Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                RandomDataGenerator dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();

                System.out.println("No of updates: " + Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                for (int axis0 = 0; axis0 < slicesPerDim; axis0++) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                    shiftedAxis.add(shiftedAxis0);

                    String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchmarkContext.getArrayName());
                    boolean success = false;

                    while (!success) {
                        try {
                            executionTime = queryExecutor.executeTimedQuery(updateQuery,
                                    "--mddtype", aChar.getFirst(),
                                    "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                    "--file", filePath);
                            success = true;
                            systemController.restartSystem();
                        } catch (Exception ex) {
                            systemController.restartSystem();
                        }
                    }
                }

                executionTime = (long) (executionTime * Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                break;
            }
            case 2: {
                double approxSlicePerDim = Math.pow(slices, 1 / ((double) benchmarkContext.getArrayDimensionality()));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchmarkContext.getArraySize()) / Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                RandomDataGenerator dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                for (int axis0 = 0; axis0 < slicesPerDim; ++axis0) {
                    Pair<Long, Long> shiftedAxis0 = Pair.of(axis0Domain.getFirst() + axis0 * shift0, axis0Domain.getSecond() + axis0 * shift0);
                    for (int axis1 = 0; axis1 < slicesPerDim; ++axis1) {
                        Pair<Long, Long> shiftedAxis1 = Pair.of(axis1Domain.getFirst() + axis1 * shift1, axis1Domain.getSecond() + axis1 * shift1);
                        List<Pair<Long, Long>> shiftedAxis = new ArrayList<>();
                        shiftedAxis.add(shiftedAxis0);
                        shiftedAxis.add(shiftedAxis1);

                        String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchmarkContext.getArrayName());
                        boolean success = false;

                        while (!success) {
                            try {
                                executionTime = queryExecutor.executeTimedQuery(updateQuery,
                                        "--mddtype", aChar.getFirst(),
                                        "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                        "--file", filePath);
                                success = true;
                                systemController.restartSystem();
                            } catch (Exception ex) {
                                systemController.restartSystem();
                            }
                        }
                    }


                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));

                break;
            }
            case 3: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) benchmarkContext.getArrayDimensionality()));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchmarkContext.getArraySize()) / Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                RandomDataGenerator dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);
                Pair<Long, Long> axis2Domain = domainBoundaries.get(2);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                long shift2 = axis2Domain.getSecond() - axis2Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
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

                            String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchmarkContext.getArrayName());
                            boolean success = false;

                            while (!success) {
                                try {
                                    executionTime = queryExecutor.executeTimedQuery(updateQuery,
                                            "--mddtype", aChar.getFirst(),
                                            "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                            "--file", filePath);
                                    success = true;
                                } catch (Exception ex) {
                                    systemController.restartSystem();
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                break;
            }
            case 4: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) benchmarkContext.getArrayDimensionality()));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchmarkContext.getArraySize()) / Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                RandomDataGenerator dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
                String filePath = dataGenerator.getFilePath();

                Pair<Long, Long> axis0Domain = domainBoundaries.get(0);
                Pair<Long, Long> axis1Domain = domainBoundaries.get(1);
                Pair<Long, Long> axis2Domain = domainBoundaries.get(2);
                Pair<Long, Long> axis3Domain = domainBoundaries.get(3);

                long shift0 = axis0Domain.getSecond() - axis0Domain.getFirst();
                long shift1 = axis1Domain.getSecond() - axis1Domain.getFirst();
                long shift2 = axis2Domain.getSecond() - axis2Domain.getFirst();
                long shift3 = axis3Domain.getSecond() - axis3Domain.getFirst();
                System.out.println("No of updates: " + Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
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

                                String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchmarkContext.getArrayName());
                                boolean success = false;

                                while (!success) {
                                    try {
                                        executionTime = queryExecutor.executeTimedQuery(updateQuery,
                                                "--mddtype", aChar.getFirst(),
                                                "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                                "--file", filePath);
                                        success = true;
                                    } catch (Exception ex) {
                                        systemController.restartSystem();
                                    }
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                break;
            }
            case 5: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) benchmarkContext.getArrayDimensionality()));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchmarkContext.getArraySize()) / Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                RandomDataGenerator dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
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
                System.out.println("No of updates: " + Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
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

                                    String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchmarkContext.getArrayName());
                                    boolean success = false;
                                    insertNo++;
                                    System.out.println("Performing insert: " + insertNo);
                                    while (!success) {
                                        try {
                                            executionTime = queryExecutor.executeTimedQuery(updateQuery,
                                                    "--mddtype", aChar.getFirst(),
                                                    "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                                    "--file", filePath);
                                            success = true;

                                            systemController.restartSystem();
                                        } catch (Exception ex) {
                                            systemController.restartSystem();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                break;
            }
            case 6: {

                double approxSlicePerDim = Math.pow(slices, 1 / ((double) benchmarkContext.getArrayDimensionality()));
                long slicesPerDim = ((long) Math.ceil(approxSlicePerDim));
                long newFileSize = (long) (((double) slices) * ((double) benchmarkContext.getArraySize()) / Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(newFileSize);
                long fileSize = domainGenerator.getFileSize(domainBoundaries);

                RandomDataGenerator dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
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
                System.out.println("No of updates: " + Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
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

                                        String updateQuery = String.format("UPDATE %s AS t set t assign $1", benchmarkContext.getArrayName());
                                        boolean success = false;
                                        insertNo++;
                                        System.out.println("Performing insert: " + insertNo);
                                        while (!success) {
                                            try {
                                                executionTime = queryExecutor.executeTimedQuery(updateQuery,
                                                        "--mddtype", aChar.getFirst(),
                                                        "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(shiftedAxis),
                                                        "--file", filePath);
                                                success = true;
                                                systemController.restartSystem();
                                            } catch (Exception ex) {
                                                systemController.restartSystem();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                executionTime = (long) (executionTime * Math.pow(slicesPerDim, benchmarkContext.getArrayDimensionality()));
                break;
            }

        }
        return executionTime;
    }
    
}
