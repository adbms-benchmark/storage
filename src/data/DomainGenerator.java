package data;

import util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class DomainGenerator {

    public static final long DEFAULT_DOMAIN_LOWER_BOUND = 0l;
    private final int noOfDimensions;

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

    public List<Pair<Long, Long>> getDomainBoundaries(long approxFileSize, String dataType) {

        int bytes = getBytes(dataType);

        approxFileSize /= bytes;
        double approxAxisSize = Math.pow(approxFileSize, 1 / ((double) noOfDimensions));

        long axisSize = ((long) Math.ceil(approxAxisSize)) - 1l;

        List<Pair<Long, Long>> result = new ArrayList<>();
        for (int i = 0; i < noOfDimensions; ++i) {
            result.add(Pair.of(DEFAULT_DOMAIN_LOWER_BOUND, axisSize));
        }

        return result;
    }

    public String getTileDomainBoundariesOperations() {

        String boundary = "0:*";
//        List<Pair<Long, Long>> result = new ArrayList<>();
        String result = "";
        for (int i = 0; i < noOfDimensions; ++i) {
            if (i == 0) {
                result += boundary;
            } else {
                result += "," + boundary;
            }
        }

        result = "[" + result + "]";
        return result;
    }

    public List<Pair<Long, Long>> getDomainBoundaries2D(long approxFileSize) {
        int dims = 2;
        double approxAxisSize = Math.pow(approxFileSize, 1 / ((double) dims));

        long axisSize = ((long) Math.ceil(approxAxisSize)) - 1l;

        List<Pair<Long, Long>> result = new ArrayList<>();
        for (int i = 0; i < dims; ++i) {
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

    public int getBytes(String dataType) {
        int bytes = 1;
        switch(dataType) {
            case "char":
            case "bool":
                bytes = 1;
                break;
            case "short":
                bytes = 2;
                break;
            case "float":
            case "long":
            case "int32":
            case "unsigned long":
            case "unsigned":
            case "uint32":
                bytes = 4;
                break;
            case "double":
            case "int64":
                bytes = 8;
                break;
        }

        return bytes;
    }

    public long getFileSize(List<Pair<Long, Long>> domain, String dataType) {

        int bytes = getBytes(dataType);

        long fileSize = 1l;
        for (Pair<Long, Long> axisBoundaries : domain) {
            fileSize *= (axisBoundaries.getSecond() - axisBoundaries.getFirst() + 1l);
        }

        return fileSize * bytes;
    }

    public String getMDArrayDomain() {
        StringBuilder ret = new StringBuilder();
        ret.append('[');
        for (int i = 0; i < noOfDimensions; i++) {
            if (i > 0) {
                ret.append(',');
            }
            ret.append('d').append(i);
        }
        ret.append(']');
        return ret.toString();
    }

    public String getSciQLDomain(List<Pair<Long, Long>> domainBoundaries) {
        StringBuilder ret = new StringBuilder();
        ret.append('(');
        int i = 0;
        for (Pair<Long, Long> domainBoundary : domainBoundaries) {
            ret.append('d').append(i).append(" INT DIMENSION [")
                    .append(domainBoundary.getFirst()).append(':')
                    .append(domainBoundary.getSecond()).append("],");
            ++i;
        }
        ret.append(" v TINYINT DEFAULT 5)");
        return ret.toString();
    }

}
