package framework;

/**
 *
 * @author Dimitar Misev
 */
public class TableContext {

    public final String asqldbTable1;
    public final String asqldbTable2;
    public final String sciqlTable1;
    public final String sciqlTable2;
    public final String fileName1;
    public final String fileName2;
    public final int dataSize;
    public final String dataSizeString;
    public final int index1;
    public final int index2;

    public TableContext(int index, int size) {
        dataSize = size;
        index1 = 2 * index + 1;
        index2 = 2 * index + 2;
        dataSizeString = getDataSizeString();
        asqldbTable1 = getAsqldbTable(index1);
        asqldbTable2 = getAsqldbTable(index2);
        sciqlTable1 = getSciqlTable(index1);
        sciqlTable2 = getSciqlTable(index2);
        fileName1 = getFileName(1);
        fileName2 = getFileName(2);
    }

    private String getDataSizeString() {
        String ret = "";
        if (dataSize < 10) {
            ret += "0";
        }
        if (dataSize < 100) {
            ret += "0";
        }
        ret += dataSize;
        return ret;
    }

    private String getFileName(int index) {
        return "grey" + index + "_" + dataSizeString + ".tif";
    }

    private String getAsqldbTable(int index) {
        return "t" + index;
    }

    private String getSciqlTable(int index) {
        return "rs.image" + index;
    }

    @Override
    public String toString() {
        return "TableContext"
                + "\n asqldbTable1=" + asqldbTable1 + "\n asqldbTable2=" + asqldbTable2
                + "\n sciqlTable1=" + sciqlTable1 + "\n sciqlTable2=" + sciqlTable2
                + "\n fileName1=" + fileName1 + "\n fileName2=" + fileName2
                + "\n dataSize=" + dataSize + "\n dataSizeString=" + dataSizeString;
    }
}
