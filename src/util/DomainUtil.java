package util;

public class DomainUtil {

    public static long getDimensionUpperBound(int noOfDimensions, long totalSize) {
        double approxChunkSizePerDim = Math.pow(totalSize, 1 / ((double) noOfDimensions));
        long chunkSizePerDim = ((long) Math.ceil(approxChunkSizePerDim)) - 1;

        return chunkSizePerDim;
    }
}
