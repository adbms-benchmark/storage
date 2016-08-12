package util;

/**
 *
 * @author Dimitar Misev
 * @author George Merticariu
 */
public class DomainUtil {

    public static final long SIZE_1B = 1024l;
    public static final long SIZE_1KB = 1024l;
    public static final long SIZE_1MB = 1048576l;
    public static final long SIZE_1GB = 1073741824l;
    public static final long SIZE_1TB = 1099511627776l;
    public static final long SIZE_1PB = 1125899906842624l;
    public static final long SIZE_1EB = 1152921504606846976l;
    
    public static final long SIZE_100MB = 100 * SIZE_1MB;

    public static long getDimensionUpperBound(int noOfDimensions, long totalSize) {
        double approxChunkSizePerDim = Math.pow(totalSize, 1 / ((double) noOfDimensions));
        long chunkSizePerDim = ((long) Math.ceil(approxChunkSizePerDim)) - 1;

        return chunkSizePerDim;
    }
    
    public static Pair<Long, String>[] parseSizes(String[] sizes) {
        Pair<Long, String>[] ret = new Pair[sizes.length];
        for (int i = 0; i < sizes.length; i++) {
            ret[i] = DomainUtil.parseSize(sizes[i]);
        }
        return ret;
    }

    /**
     * @return the given size in (bytes, symbol)
     */
    public static Pair<Long, String> parseSize(String size) {
        int ind = getNonDigitIndex(size);
        if (ind == -1) {
            ind = size.length();
            size += "b";
        }
        String suffix = size.substring(ind).toLowerCase();
        long value = Long.parseLong(size.substring(0, ind));
        long multiplier = getMultiplier(suffix);
        return Pair.of(value * multiplier, suffix);
    }

    public static long getMultiplier(String symbol) {
        switch (symbol) {
            case "b":
                return SIZE_1B;
            case "kb":
            case "kib":
                return SIZE_1KB;
            case "mb":
            case "mib":
                return SIZE_1MB;
            case "gb":
            case "gib":
                return SIZE_1GB;
            case "tb":
            case "tib":
                return SIZE_1TB;
            case "pb":
            case "pib":
                return SIZE_1PB;
            case "eb":
            case "eib":
                return SIZE_1EB;
            default:
                throw new RuntimeException("Unsupported size symbol: " + symbol);
        }
    }

    private static int getNonDigitIndex(String s) {
        int ret = -1;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                continue;
            }
            ret = i;
            break;
        }
        return ret;
    }

    public static void main(String[] args) {
        System.out.println(parseSize("12"));
        System.out.println(parseSize("12B"));
        System.out.println(parseSize("12MB"));
        System.out.println(getDimensionUpperBound(2, 16000000));
    }
}
