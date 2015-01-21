package framework.rasdaman;

import data.DataGenerator;
import data.DomainGenerator;
import framework.context.BenchmarkContext;
import framework.QueryExecutor;
import framework.SystemController;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import framework.context.RasdamanContext;
import util.Pair;

/**
 *
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
    public long executeTimedQuery(String query, String... args) {
        List<String> commandList = new ArrayList<>();
        //TODO-GM: read rasql path from config file
        commandList.add("/home/rasdaman/install/bin/rasql");
        commandList.add("-q");
        commandList.add(query);
        commandList.add("--out");
        commandList.add("string");
        Collections.addAll(commandList, args);

        long startTime = System.currentTimeMillis();
        SystemController.executeShellCommand(commandList.toArray(new String[]{}));
        long result = System.currentTimeMillis() - startTime;

        return result;
    }

    @Override
    public void createCollection() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchContext.getCollSize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        double approxChunkSize = Math.pow(benchContext.getCollTileSize(), 1 / ((double) noOfDimensions));
        int chunkSize = ((int) Math.ceil(approxChunkSize)) - 1;
        long tileSize = (long) Math.pow(chunkSize, noOfDimensions);

        dataGenerator = new DataGenerator(fileSize);
        String filePath = dataGenerator.getFilePath();

        Pair<String, String> aChar = rasdamanSystemController.createRasdamanType(noOfDimensions, "char");

        String createCollectionQuery = String.format("CREATE COLLECTION %s %s", benchContext.getCollName1(), aChar.getSecond());
        executeTimedQuery(createCollectionQuery, new String[]{
                "--user", context.getUser(),
                "--passwd", context.getPassword()});

        String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR [0:%d,0:%d] TILE SIZE %d", benchContext.getCollName1(), chunkSize, chunkSize, tileSize);
        executeTimedQuery(insertQuery, new String[]{
                "--user", context.getUser(),
                "--passwd", context.getPassword(),
                "--mddtype", aChar.getFirst(),
                "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries),
                "--file", filePath});
    }

    @Override
    public void dropCollection() {
        String dropCollectionQuery = MessageFormat.format("DROP COLLECTION {0}", benchContext.getCollName1());
        executeTimedQuery(dropCollectionQuery, new String[]{
                "--user", context.getUser(),
                "--passwd", context.getPassword()
        });
    }
}
