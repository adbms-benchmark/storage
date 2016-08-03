package system.rasdaman;

import benchmark.CachingBenchmarkDataManager;
import benchmark.BenchmarkContext;
import java.text.MessageFormat;
import java.util.List;
import util.Pair;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class RasdamanCachingBenchmarkDataManager extends CachingBenchmarkDataManager<RasdamanSystem> {
    
    private final RasdamanTypeManager typeManager;

    public RasdamanCachingBenchmarkDataManager(RasdamanSystem systemController,
            RasdamanQueryExecutor queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
        typeManager = new RasdamanTypeManager(queryExecutor);
    }

    @Override
    public long loadData() throws Exception {
        long totalTime = 0;
        
        String mddTypeName = createColl();
        String xy = "0:" + (BAND_WIDTH - 1) + ",0:" + (BAND_HEIGHT - 1);

        List<String> sliceFilePaths = getSliceFilePaths(benchmarkContext);
        for (int i = 0; i < sliceFilePaths.size(); i++) {
            String updateQuery;
            if (i == 0) {
                updateQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR [0:0,0:499,0:499] TILE SIZE 5500000", benchmarkContext.getArrayName());
            } else {
                updateQuery = String.format("UPDATE %s AS m SET m[*:*,*:*,*:*] ASSIGN $1", benchmarkContext.getArrayName(), i);
            }
            String mddDomain = "[" + i + ":" + i + "," + xy + "]";
            totalTime += queryExecutor.executeTimedQuery(updateQuery,
                    "-f", sliceFilePaths.get(i),
                    "--mdddomain", mddDomain,
                    "--mddtype", mddTypeName
            );
        }

        return totalTime;
    }

    /**
     * Create collection.
     *
     * @return MDD type name
     * @throws Exception
     */
    private String createColl() throws Exception {
        Pair<String, String> type = typeManager.createType(3, "ushort", "ushort",
                "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort");
        String createCollectionQuery = String.format("CREATE COLLECTION %s %s", benchmarkContext.getArrayName(), type.getSecond());
        queryExecutor.executeTimedQuery(createCollectionQuery);
        return type.getFirst();
    }

    @Override
    public long dropData() throws Exception {
        String dropQuery = MessageFormat.format("DROP COLLECTION {0}", benchmarkContext.getArrayName());
        long ret = queryExecutor.executeTimedQuery(dropQuery);
        
        String baseTypeName = typeManager.getBaseTypeName("ushort", "ushort",
                "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort");
        typeManager.deleteTypes(
                typeManager.getSetTypeName(3, baseTypeName),
                typeManager.getMddTypeName(3, baseTypeName), baseTypeName);
        return ret;
    }

}
