package util;

public class StopWatch {

    private long startTime;

    public StopWatch() {
        reset();
    }

    public void reset() {
        this.startTime = System.currentTimeMillis();
    }

    public long getElapsedTime() {
        long endTime = System.currentTimeMillis();
        return endTime - this.startTime;
    }

    /**
     * milliseconds period
     *
     * @param period
     * @return
     */
    public boolean periodExpired(long period) {
        return period < getElapsedTime();
    }
}
