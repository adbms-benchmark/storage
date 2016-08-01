package framework.rasdaman;

import framework.DataManager;
import framework.QueryExecutor;
import framework.context.BenchmarkContext;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import util.IO;
import util.Pair;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class RasdamanCachingBenchmarkDataManager extends DataManager<RasdamanSystem> {

    public static final int MAX_SLICE_NO = 100;
    public static final int BAND_NO = 11;
    public static final int BAND_WIDTH = 8000;
    public static final int BAND_HEIGHT = 8000;
    public static final String SLICE_EXT = ".bin";

    public RasdamanCachingBenchmarkDataManager(RasdamanSystem systemController,
            QueryExecutor<RasdamanSystem> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }

    @Override
    public long loadData() throws Exception {
        long totalTime = 0;
        
        String mddTypeName = createColl();
        String mddTypeName2D = create2DType();
        String xy = "0:" + (BAND_WIDTH - 1) + ",0:" + (BAND_HEIGHT - 1);

        List<String> sliceFilePaths = getSliceFilePaths(benchmarkContext);
        for (int i = 0; i < sliceFilePaths.size(); i++) {
            String updateQuery = null;
            String mddType = null;
            String mddDomain = null;
            if (i == 0) {
                updateQuery = String.format("INSERT INTO %s VALUES $1 TILING ALIGNED [0:0,0:499,0:499] TILE SIZE 5500000", benchmarkContext.getArrayName());
                mddDomain = "[0:0," + xy + "]";
                mddType = mddTypeName;
            } else {
                updateQuery = String.format("UPDATE %s AS m SET m[%d,*:*,*:*] ASSIGN $1", benchmarkContext.getArrayName(), i);
                mddDomain = "[" + xy + "]";
                mddType = mddTypeName2D;
            }
            totalTime += queryExecutor.executeTimedQuery(updateQuery,
                    "--user", systemController.getUser(),
                    "--passwd", systemController.getPassword(),
                    "-f", sliceFilePaths.get(i),
                    "--mdddomain", mddDomain,
                    "--mddtype", mddType
            );
        }

        return totalTime;
    }
    
    public static List<String> getSliceFilePaths(BenchmarkContext benchmarkContext) {
        List<String> ret = new ArrayList<>();
        for (int i = 0; i <= MAX_SLICE_NO; i++) {
            String sliceFileName = i + SLICE_EXT;
            String sliceFilePath = IO.concatPaths(benchmarkContext.getDataDir(), sliceFileName);
            if (!IO.fileExists(sliceFilePath)) {
                break;
            }
            ret.add(sliceFilePath);
        }
        return ret;
    }

    /**
     * Create collection.
     *
     * @return MDD type name
     * @throws Exception
     */
    private String createColl() throws Exception {
        Pair<String, String> type = systemController.createRasdamanType(3, "ushort", "ushort",
                "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort");
        String createCollectionQuery = String.format("CREATE COLLECTION %s %s", benchmarkContext.getArrayName(), type.getSecond());
        queryExecutor.executeTimedQuery(createCollectionQuery,
                "--user", systemController.getUser(),
                "--passwd", systemController.getPassword());
        return type.getFirst();
    }
    
    private String create2DType() throws Exception {
        Pair<String, String> type = systemController.createRasdamanType(2, "ushort", "ushort",
                "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort", "ushort");
        return type.getFirst();
    }

    @Override
    public long dropData() throws Exception {
        String dropQuery = MessageFormat.format("DROP COLLECTION {0}", benchmarkContext.getArrayName());
        return queryExecutor.executeTimedQuery(dropQuery, new String[]{
                "--user", systemController.getUser(),
                "--passwd", systemController.getPassword()
        });
    }

}
