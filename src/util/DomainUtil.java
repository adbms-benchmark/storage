package util;

/**
 *
 * @author Dimitar Misev
 * @author George Merticariu
 */
public class DomainUtil {

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
                return 1l;
            case "kb":
            case "kib":
                return 1024l;
            case "mb":
            case "mib":
                return 1048576l;
            case "gb":
            case "gib":
                return 1073741824l;
            case "tb":
            case "tib":
                return 1099511627776l;
            case "pb":
            case "pib":
                return 1125899906842624l;
            case "eb":
            case "eib":
                return 1152921504606846976l;
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
    }
}
