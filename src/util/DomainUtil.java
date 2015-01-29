package util;

public class DomainUtil {

    public static long getTileDimensionUpperBound(int noOfDimensions, long tileSize) {
        double approxChunkSizePerDim = Math.pow(tileSize, 1 / ((double) noOfDimensions));
        long chunkSizePerDim = ((long) Math.ceil(approxChunkSizePerDim)) - 1l;

        return chunkSizePerDim;
    }
}
