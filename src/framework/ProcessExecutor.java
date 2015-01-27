package framework;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import util.StopWatch;

public class ProcessExecutor {
    private static final String DEV_NULL = "/dev/null";
    private static final int EXIT_SUCCESS = 0;

    private long maxExecutionTime = -1;
    private Process process;
    private int exitStatus = -1;
    private long executionTime = -1;
    private String[] command;
    private boolean interrupted = false;
    private boolean executeWithTimeLimit = false;
    private String error = "";

    public ProcessExecutor(String... command) {
        this.command = command;
    }

    public ProcessExecutor(int executionTimeLimit, String... command) {
        this(command);

        this.maxExecutionTime = ((long) executionTimeLimit) * 1000l;
        this.executeWithTimeLimit = true;
    }

    public void executeRedirectOutput(String filePath) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command).redirectOutput(new File(filePath));
        execute(processBuilder);
    }

    public void executeRedirectInput(String filePath) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command).redirectInput(new File(filePath));
        execute(processBuilder);
    }

    private void execute(ProcessBuilder processBuilder) throws IOException, InterruptedException {
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

        StringBuilder error = new StringBuilder();

        if (executeWithTimeLimit && !interrupted) {
            timer.cancel();
        }

        if (!interrupted && exitStatus != EXIT_SUCCESS) {
            try (Scanner scan = new Scanner(process.getErrorStream())) {
                while (scan.hasNextLine()) {
                    error.append(scan.nextLine());
                    error.append("\n");
                }
                this.error = error.toString();
            }
        }
    }

    public void execute() throws IOException, InterruptedException {
        executeRedirectOutput(DEV_NULL);
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

    public String getError() {
        return error;
    }

    private class TerminateProcessJob extends TimerTask {
        @Override
        public void run() {
            process.destroy();
            interrupted = true;
        }
    }
}
