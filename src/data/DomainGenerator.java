package data;

import java.util.ArrayList;
import java.util.List;
import util.Pair;

/**
 *
 * @author George Merticariu
 */
public class DomainGenerator {

    public static final long DEFAULT_DOMAIN_LOWER_BOUND = 0l;
    private int noOfDimensions;

    public DomainGenerator(int noOfDimensions) {
        this.noOfDimensions = noOfDimensions;
    }

    public List<Pair<Long, Long>> getDomainBoundaries(long approxFileSize) {

        double approxAxisSize = Math.pow(approxFileSize, 1 / ((double) noOfDimensions));

        long axisSize = ((long) Math.ceil(approxAxisSize)) - 1l;

        List<Pair<Long, Long>> result = new ArrayList<>();
        for (int i = 0; i < noOfDimensions; ++i) {
            result.add(Pair.of(DEFAULT_DOMAIN_LOWER_BOUND, axisSize));
        }

        return result;
    }

    public long getFileSize(List<Pair<Long, Long>> domain) {
        long fileSize = 1l;
        for (Pair<Long, Long> axisBoundaries : domain) {
            fileSize *= (axisBoundaries.getSecond() - axisBoundaries.getFirst() + 1l);
        }

        return fileSize;
    }

}
