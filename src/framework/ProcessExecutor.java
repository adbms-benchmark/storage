package framework;

import util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class ProcessExecutor {
    private static final String DEV_NULL = "/dev/null";

    private long maxExecutionTime;
    private Process process;
    private int exitStatus;
    private long executionTime;
    private String[] command;
    private boolean interrupted;
    private boolean executeWithTimeLimit;

    public ProcessExecutor(String... command) {
        this.executeWithTimeLimit = false;
        this.command = command;
    }

    public ProcessExecutor(int executionTimeLimit, String... command) {
        this(command);

        this.maxExecutionTime = ((long) executionTimeLimit) * 1000l;
        this.executeWithTimeLimit = true;

        this.interrupted = false;
    }

    public void execute() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command).redirectOutput(new File(DEV_NULL));

        Timer timer = new Timer();
        if (executeWithTimeLimit) {
            timer.schedule(new TerminateProcessJob(), maxExecutionTime);
        }

        StopWatch executionTimer = new StopWatch();
        executionTimer.reset();
        process = processBuilder.start();
        process.waitFor();
        executionTime = executionTimer.getElapsedTime();
        exitStatus = process.exitValue();

        if (executeWithTimeLimit && !interrupted) {
            timer.cancel();
        }

    }

    public int getExitStatus() {
        return exitStatus;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    private class TerminateProcessJob extends TimerTask {
        @Override
        public void run() {
            process.destroy();
            interrupted = true;
        }
    }
}
