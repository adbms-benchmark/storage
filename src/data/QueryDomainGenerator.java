package data;

import benchmark.BenchmarkContext;
import benchmark.storage.StorageBenchmarkContext;

import java.util.ArrayList;
import java.util.List;

import util.DomainUtil;
import util.Pair;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class QueryDomainGenerator {

    private final BenchmarkContext benchContext;
    private final int noOfDimensions;
    private final int noOfQueries;

    public QueryDomainGenerator(BenchmarkContext benchmarkContext) {
        this.noOfDimensions = benchmarkContext.getArrayDimensionality();
        this.benchContext = benchmarkContext;
        if (benchmarkContext instanceof StorageBenchmarkContext) {
            this.noOfQueries = ((StorageBenchmarkContext)benchContext).getQueryNumber();
        } else {
            this.noOfQueries = 1;
        }
    }

    public List<Pair<Long, Long>> getMiddlePointQueryDomain() {
        long dimensionUpperBound = DomainUtil.getDimensionUpperBound(noOfDimensions, benchContext.getArraySize());
        long middleBound = dimensionUpperBound / 2;
        List<Pair<Long, Long>> domain = new ArrayList<>();

        for (int i = 0; i < noOfDimensions; ++i) {
            domain.add(Pair.of(middleBound, middleBound));
        }

        return domain;
    }

    public List<List<Pair<Long, Long>>> getSizeQueryDomain() {

        List<List<Pair<Long, Long>>> result = new ArrayList<>();

        double selectStep = (Math.pow(((StorageBenchmarkContext)benchContext).getMaxSelectSize(), 1 / ((double) noOfDimensions)) / ((double) noOfQueries));
        double axisSize = selectStep;

        for (int queryIndex = 0; queryIndex < noOfQueries; ++queryIndex) {
            List<Pair<Long, Long>> domain = new ArrayList<>();
            for (int domainIndex = 0; domainIndex < noOfDimensions; ++domainIndex) {
                domain.add(Pair.of(0l, (long) (Math.ceil(axisSize) - 1l)));
            }
            axisSize += selectStep;
            result.add(domain);
        }

        return result;
    }

    public List<List<Pair<Long, Long>>> getPositionQueryDomain() {
        List<List<Pair<Long, Long>>> result = new ArrayList<>();

        long tileDimensionUpperBound = DomainUtil.getDimensionUpperBound(noOfDimensions, 
                ((StorageBenchmarkContext)benchContext).getTileSize());
        long tileDimensionInsideTilePosition = tileDimensionUpperBound / 2;

        for (int i = 0; i < noOfDimensions + 1; ++i) {
            List<Pair<Long, Long>> domain = new ArrayList<>();
            for (int j = 0; j < noOfDimensions; ++j) {
                if (j < i) {
                    domain.add(Pair.of(tileDimensionInsideTilePosition, tileDimensionInsideTilePosition));
                } else {
                    domain.add(Pair.of(tileDimensionUpperBound, tileDimensionUpperBound + 1l));
                }

            }
            result.add(domain);
        }

        return result;
    }

    public List<List<Pair<Long, Long>>> getShapeQueryDomain() {
        List<List<Pair<Long, Long>>> result = new ArrayList<>();
        if (noOfDimensions < 2) {
            return result;
        }
        double selectAxisSize = Math.pow(((StorageBenchmarkContext)benchContext).getMaxSelectSize(), 1 / (double) noOfDimensions);

        double step = selectAxisSize / ((double) (noOfQueries - 1));
        double firstAxisSize = 1;

        for (int queryIndex = 0; queryIndex < noOfQueries; ++queryIndex) {
            double restAxisSize = firstAxisSize;
            for (int i = 2; i < noOfDimensions; ++i) {
                restAxisSize *= selectAxisSize;
            }
            double secondAxisSize = ((StorageBenchmarkContext)benchContext).getMaxSelectSize() / restAxisSize;

            List<Pair<Long, Long>> domain = new ArrayList<>();
            domain.add(Pair.of(0l, (long) (Math.ceil(firstAxisSize) - 1l)));
            domain.add(Pair.of(0l, (long) (Math.ceil(secondAxisSize) - 1l)));
            for (int i = 2; i < noOfDimensions; ++i) {
                domain.add(Pair.of(0l, (long) (Math.ceil(selectAxisSize) - 1l)));
            }

            result.add(domain);

            firstAxisSize += step;
        }

        return result;
    }

    public List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> getMultiAccessQueryDomain() {
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> result = new ArrayList<>();

        long maxChunkSelectSize = ((StorageBenchmarkContext)benchContext).getMaxSelectSize() / 2;

        double collectionAxisSize = Math.pow(benchContext.getArraySize(), 1 / (double) noOfDimensions);
        double selectAxisSize = Math.pow(maxChunkSelectSize, 1 / (double) noOfDimensions);

        double selectStep = (collectionAxisSize - selectAxisSize) / ((double) noOfQueries);
        double lowerAxisSize = 0;
        double higherAxisSize = selectAxisSize;

        List<Pair<Long, Long>> firstChunkDomain = new ArrayList<>();
        for (int domainIndex = 0; domainIndex < noOfDimensions; ++domainIndex) {
            firstChunkDomain.add(Pair.of((long) Math.ceil(lowerAxisSize), (long) (Math.ceil(higherAxisSize) - 1l)));
        }
        lowerAxisSize += selectStep;
        higherAxisSize += selectStep;

        for (int queryIndex = 0; queryIndex < noOfQueries; ++queryIndex) {
            List<Pair<Long, Long>> secondChunkDomain = new ArrayList<>();
            for (int domainIndex = 0; domainIndex < noOfDimensions; ++domainIndex) {
                secondChunkDomain.add(Pair.of((long) Math.ceil(lowerAxisSize), (long) (Math.ceil(higherAxisSize) - 1l)));
            }
            lowerAxisSize += selectStep;
            higherAxisSize += selectStep;
            result.add(Pair.of(firstChunkDomain, secondChunkDomain));

        }

        return result;
    }
}
