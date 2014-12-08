package data;

import java.util.ArrayList;
import java.util.List;
import util.Pair;

/**
 *
 * @author George Merticariu
 */
public class QueryDomainGenerator {

    private int noOfDimensions;
    private long maxSelectSize;
    private long collectionSize;
    private int noOfQueries;

    public QueryDomainGenerator(long collectionSize, int noOfDimensions, long maxSelectSize, int noOfQueries) {
        this.collectionSize = collectionSize;
        this.noOfDimensions = noOfDimensions;
        this.maxSelectSize = maxSelectSize;
        this.noOfQueries = noOfQueries;
    }

    public List<List<Pair<Long, Long>>> getSizeQueryDomain() {

        List<List<Pair<Long, Long>>> result = new ArrayList<>();

        double selectStep = (Math.pow(maxSelectSize, 1 / ((double) noOfDimensions)) / ((double) noOfQueries));
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

        double collectionAxisSize = Math.pow(collectionSize, 1 / (double) noOfDimensions);
        double selectAxisSize = Math.pow(maxSelectSize, 1 / (double) noOfDimensions);

        double selectStep = (collectionAxisSize - selectAxisSize) / ((double) noOfQueries);
        double lowerAxisSize = 0;
        double higherAxisSize = selectAxisSize;

        for (int queryIndex = 0; queryIndex < noOfQueries; ++queryIndex) {
            List<Pair<Long, Long>> domain = new ArrayList<>();
            for (int domainIndex = 0; domainIndex < noOfDimensions; ++domainIndex) {
                domain.add(Pair.of((long) Math.ceil(lowerAxisSize), (long) (Math.ceil(higherAxisSize) - 1l)));
            }
            lowerAxisSize += selectStep;
            higherAxisSize += selectStep;
            result.add(domain);

        }

        return result;
    }

    public List<List<Pair<Long, Long>>> getShapeQueryDomain() {
        List<List<Pair<Long, Long>>> result = new ArrayList<>();
        if (noOfDimensions < 2) {
            return result;
        }
        double selectAxisSize = Math.pow(maxSelectSize, 1 / (double) noOfDimensions);

        double step = selectAxisSize / ((double) noOfQueries);
        double firstAxisSize = step;

        for (int queryIndex = 0; queryIndex < noOfQueries; ++queryIndex) {
            double restAxisSize = firstAxisSize;
            for (int i = 2; i < noOfDimensions; ++i) {
                restAxisSize *= selectAxisSize;
            }
            double secondAxisSize = maxSelectSize / restAxisSize;

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

        long maxChunkSelectSize = maxSelectSize / 2;

        double collectionAxisSize = Math.pow(collectionSize, 1 / (double) noOfDimensions);
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
