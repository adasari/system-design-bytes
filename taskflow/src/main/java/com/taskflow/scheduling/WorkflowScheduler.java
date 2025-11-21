package com.taskflow.scheduling;

import com.taskflow.core.WorkflowDefinition;
import com.taskflow.core.WorkflowResult;
import com.taskflow.executor.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class WorkflowScheduler {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowScheduler.class);

    private final ScheduledExecutorService scheduler;
    private final WorkflowExecutor executor;
    private final Map<String, ScheduledFuture<?>> scheduledTasks;
    private final Map<String, ScheduledWorkflow> scheduledWorkflows;

    public WorkflowScheduler(WorkflowExecutor executor) {
        this.scheduler = Executors.newScheduledThreadPool(5);
        this.executor = executor;
        this.scheduledTasks = new ConcurrentHashMap<>();
        this.scheduledWorkflows = new ConcurrentHashMap<>();
    }

    public String scheduleAt(WorkflowDefinition workflow, Instant executionTime, Map<String, Object> inputs) {
        String scheduleId = UUID.randomUUID().toString();
        long delay = Duration.between(Instant.now(), executionTime).toMillis();

        if (delay < 0) {
            throw new IllegalArgumentException("Execution time must be in the future");
        }

        ScheduledWorkflow scheduledWorkflow = new ScheduledWorkflow(
            scheduleId, workflow, inputs, executionTime, ScheduleType.ONE_TIME
        );

        ScheduledFuture<?> future = scheduler.schedule(
            () -> executeScheduledWorkflow(scheduledWorkflow),
            delay,
            TimeUnit.MILLISECONDS
        );

        scheduledTasks.put(scheduleId, future);
        scheduledWorkflows.put(scheduleId, scheduledWorkflow);

        logger.info("Scheduled workflow {} for execution at {}", workflow.getName(), executionTime);
        return scheduleId;
    }

    public String scheduleWithDelay(WorkflowDefinition workflow, Duration delay, Map<String, Object> inputs) {
        return scheduleAt(workflow, Instant.now().plus(delay), inputs);
    }

    public String scheduleRecurring(WorkflowDefinition workflow, Duration interval,
                                    Map<String, Object> inputs, boolean fixedRate) {
        String scheduleId = UUID.randomUUID().toString();

        ScheduledWorkflow scheduledWorkflow = new ScheduledWorkflow(
            scheduleId, workflow, inputs, Instant.now(),
            fixedRate ? ScheduleType.FIXED_RATE : ScheduleType.FIXED_DELAY
        );
        scheduledWorkflow.setInterval(interval);

        Runnable task = () -> executeScheduledWorkflow(scheduledWorkflow);

        ScheduledFuture<?> future;
        if (fixedRate) {
            future = scheduler.scheduleAtFixedRate(
                task, 0, interval.toMillis(), TimeUnit.MILLISECONDS
            );
        } else {
            future = scheduler.scheduleWithFixedDelay(
                task, 0, interval.toMillis(), TimeUnit.MILLISECONDS
            );
        }

        scheduledTasks.put(scheduleId, future);
        scheduledWorkflows.put(scheduleId, scheduledWorkflow);

        logger.info("Scheduled recurring workflow {} with interval {}",
            workflow.getName(), interval);
        return scheduleId;
    }

    public String scheduleCron(WorkflowDefinition workflow, String cronExpression,
                              Map<String, Object> inputs) {
        String scheduleId = UUID.randomUUID().toString();
        CronSchedule cronSchedule = new CronSchedule(cronExpression);

        ScheduledWorkflow scheduledWorkflow = new ScheduledWorkflow(
            scheduleId, workflow, inputs, Instant.now(), ScheduleType.CRON
        );
        scheduledWorkflow.setCronExpression(cronExpression);

        scheduleNextCronExecution(scheduleId, scheduledWorkflow, cronSchedule);

        scheduledWorkflows.put(scheduleId, scheduledWorkflow);

        logger.info("Scheduled cron workflow {} with expression {}",
            workflow.getName(), cronExpression);
        return scheduleId;
    }

    private void scheduleNextCronExecution(String scheduleId, ScheduledWorkflow workflow,
                                          CronSchedule cronSchedule) {
        Optional<Instant> nextExecution = cronSchedule.getNextExecutionTime();
        if (nextExecution.isPresent()) {
            long delay = Duration.between(Instant.now(), nextExecution.get()).toMillis();

            ScheduledFuture<?> future = scheduler.schedule(() -> {
                executeScheduledWorkflow(workflow);
                scheduleNextCronExecution(scheduleId, workflow, cronSchedule);
            }, delay, TimeUnit.MILLISECONDS);

            scheduledTasks.put(scheduleId, future);
        }
    }

    public boolean cancel(String scheduleId) {
        ScheduledFuture<?> future = scheduledTasks.remove(scheduleId);
        scheduledWorkflows.remove(scheduleId);

        if (future != null) {
            boolean cancelled = future.cancel(false);
            logger.info("Cancelled scheduled workflow {}: {}", scheduleId, cancelled);
            return cancelled;
        }
        return false;
    }

    public void pause(String scheduleId) {
        ScheduledWorkflow workflow = scheduledWorkflows.get(scheduleId);
        if (workflow != null) {
            workflow.setPaused(true);
            logger.info("Paused scheduled workflow {}", scheduleId);
        }
    }

    public void resume(String scheduleId) {
        ScheduledWorkflow workflow = scheduledWorkflows.get(scheduleId);
        if (workflow != null) {
            workflow.setPaused(false);
            logger.info("Resumed scheduled workflow {}", scheduleId);
        }
    }

    private void executeScheduledWorkflow(ScheduledWorkflow scheduledWorkflow) {
        if (scheduledWorkflow.isPaused()) {
            logger.info("Skipping paused workflow {}", scheduledWorkflow.getScheduleId());
            return;
        }

        try {
            scheduledWorkflow.incrementExecutionCount();
            scheduledWorkflow.setLastExecution(Instant.now());

            Map<String, Object> inputs = new HashMap<>(scheduledWorkflow.getInputs());
            inputs.put("scheduleId", scheduledWorkflow.getScheduleId());
            inputs.put("executionCount", scheduledWorkflow.getExecutionCount());
            inputs.put("scheduledTime", scheduledWorkflow.getScheduledTime());

            CompletableFuture<WorkflowResult> future = executor.execute(
                scheduledWorkflow.getWorkflow(), inputs
            );

            future.whenComplete((result, error) -> {
                if (error != null) {
                    logger.error("Scheduled workflow execution failed", error);
                    publishScheduleErrorEvent(scheduledWorkflow, error);
                } else {
                    logger.info("Scheduled workflow completed with state: {}", result.getState());
                    publishScheduleCompleteEvent(scheduledWorkflow, result);
                }
            });

        } catch (Exception e) {
            logger.error("Error executing scheduled workflow", e);
            publishScheduleErrorEvent(scheduledWorkflow, e);
        }
    }

    private void publishScheduleCompleteEvent(ScheduledWorkflow workflow, WorkflowResult result) {

    }

    private void publishScheduleErrorEvent(ScheduledWorkflow workflow, Throwable error) {

    }

    public List<ScheduledWorkflow> getScheduledWorkflows() {
        return new ArrayList<>(scheduledWorkflows.values());
    }

    public Optional<ScheduledWorkflow> getScheduledWorkflow(String scheduleId) {
        return Optional.ofNullable(scheduledWorkflows.get(scheduleId));
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public enum ScheduleType {
        ONE_TIME,
        FIXED_RATE,
        FIXED_DELAY,
        CRON
    }

    public static class ScheduledWorkflow {
        private final String scheduleId;
        private final WorkflowDefinition workflow;
        private final Map<String, Object> inputs;
        private final Instant scheduledTime;
        private final ScheduleType type;
        private Duration interval;
        private String cronExpression;
        private int executionCount = 0;
        private Instant lastExecution;
        private boolean paused = false;

        public ScheduledWorkflow(String scheduleId, WorkflowDefinition workflow,
                                Map<String, Object> inputs, Instant scheduledTime,
                                ScheduleType type) {
            this.scheduleId = scheduleId;
            this.workflow = workflow;
            this.inputs = inputs != null ? new HashMap<>(inputs) : new HashMap<>();
            this.scheduledTime = scheduledTime;
            this.type = type;
        }

        public String getScheduleId() { return scheduleId; }
        public WorkflowDefinition getWorkflow() { return workflow; }
        public Map<String, Object> getInputs() { return new HashMap<>(inputs); }
        public Instant getScheduledTime() { return scheduledTime; }
        public ScheduleType getType() { return type; }
        public Duration getInterval() { return interval; }
        public void setInterval(Duration interval) { this.interval = interval; }
        public String getCronExpression() { return cronExpression; }
        public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }
        public int getExecutionCount() { return executionCount; }
        public void incrementExecutionCount() { this.executionCount++; }
        public Instant getLastExecution() { return lastExecution; }
        public void setLastExecution(Instant lastExecution) { this.lastExecution = lastExecution; }
        public boolean isPaused() { return paused; }
        public void setPaused(boolean paused) { this.paused = paused; }
    }

    private static class CronSchedule {
        private final String expression;

        public CronSchedule(String expression) {
            this.expression = expression;
        }

        public Optional<Instant> getNextExecutionTime() {
            return Optional.of(Instant.now().plusSeconds(60));
        }
    }
}
