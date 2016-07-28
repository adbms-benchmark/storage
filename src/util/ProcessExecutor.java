package util;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessExecutor {

    private static final Logger log = LoggerFactory.getLogger(ProcessExecutor.class);
    
    public static final String DEV_NULL = "/dev/null";
    private static final int EXIT_SUCCESS = 0;

    private long maxExecutionTime = -1;
    private Process process;
    private int exitStatus = -1;
    private long executionTime = -1;
    private String[] command;
    private boolean interrupted = false;
    private boolean executeWithTimeLimit = false;
    private String error = "";
    private String output = "";

    public ProcessExecutor(String... command) {
        this.command = command;
    }

    public ProcessExecutor(int executionTimeLimit, String... command) {
        this(command);
        this.maxExecutionTime = ((long) executionTimeLimit) * 1000l;
        this.executeWithTimeLimit = true;
    }

    public void execute() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        execute(processBuilder);
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
        if (!interrupted && exitStatus != EXIT_SUCCESS) {
            try (Scanner scan = new Scanner(process.getErrorStream())) {
                while (scan.hasNextLine()) {
                    error.append(scan.nextLine());
                    error.append("\n");
                }
                this.error = error.toString();
            }
        }
        StringBuilder output = new StringBuilder();
        try (Scanner scan = new Scanner(process.getInputStream())) {
            while (scan.hasNextLine()) {
                output.append(scan.nextLine());
                output.append("\n");
            }
            this.output = output.toString();
        }

        timer.cancel();

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

    public String getOutput() {
        return output;
    }

    private class TerminateProcessJob extends TimerTask {
        @Override
        public void run() {
            process.destroy();
            interrupted = true;
        }
    }

    public static int executeShellCommand(String... command) {
        return executeShellCommandRedirect(ProcessExecutor.DEV_NULL, command);
    }

    public static int executeShellCommandRedirect(String output, String... command) {
        String cmd = StringUtil.arrayToString(command) + " > " + output;
        log.debug("executing shell command: " + cmd);

        ProcessExecutor processExecutor = new ProcessExecutor(command);
        try {
            StopWatch timer = new StopWatch();
            processExecutor.executeRedirectOutput(output);
            log.trace(" -> finished in " + timer.getElapsedTime() + " ms.");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        if (processExecutor.getExitStatus() != 0) {
            log.error("shell command failed: " + cmd);
            log.error(" -> error: " + processExecutor.getError());
        }

        return processExecutor.getExitStatus();
    }

    public static String executeShellCommandOutput(boolean ignoreError, String... command) {
        String cmd = StringUtil.arrayToString(command);
        log.debug("executing shell command: " + cmd);

        ProcessExecutor processExecutor = new ProcessExecutor(command);
        try {
            StopWatch timer = new StopWatch();
            processExecutor.execute();
            log.trace(" -> finished in " + timer.getElapsedTime() + " ms.");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        if (processExecutor.getExitStatus() != 0 && !ignoreError) {
            log.error("shell command failed (exit code " + processExecutor.getExitStatus() + "): " + cmd);
            if (!"".equals(processExecutor.getError())) {
                log.error(" -> error: " + processExecutor.getError());
            }
        }

        return processExecutor.getOutput();
    }
}
