package com.taskflow.scheduling;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.taskflow.core.WorkflowDefinition;
import com.taskflow.core.WorkflowResult;
import com.taskflow.core.WorkflowState;
import com.taskflow.definition.WorkflowDefinitionService;
import com.taskflow.persistence.entity.WorkflowInstanceEntity;
import com.taskflow.persistence.entity.WorkflowInstanceEntity.TriggerType;
import com.taskflow.persistence.entity.ScheduledWorkflowEntity;
import com.taskflow.persistence.service.WorkflowPersistenceService;
import com.taskflow.persistence.entity.ScheduledWorkflowEntity.ScheduleStatus;
import com.taskflow.persistence.entity.ScheduledWorkflowEntity.ScheduleType;
import com.taskflow.persistence.repository.ScheduledWorkflowRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

/**
 * Enhanced workflow scheduler service with database persistence.
 * 
 * <p>Features:</p>
 * <ul>
 *   <li>Database persistence for schedule configurations</li>
 *   <li>Automatic recovery on startup</li>
 *   <li>Distributed scheduling support</li>
 *   <li>Cron expression support</li>
 *   <li>Email alerts on failures</li>
 *   <li>Schedule management REST API</li>
 * </ul>
 */
@Service
public class WorkflowSchedulerService {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkflowSchedulerService.class);
    
    @Autowired
    private ScheduledWorkflowRepository scheduledWorkflowRepository;
    
    @Autowired
    private WorkflowPersistenceService workflowPersistenceService;
    
    @Autowired
    private WorkflowDefinitionService definitionService;
    
    @Value("${workflow.scheduler.enabled:true}")
    private boolean schedulerEnabled;
    
    @Value("${workflow.scheduler.poll-interval-ms:60000}")
    private long pollIntervalMs;
    
    @Value("${workflow.scheduler.thread-pool-size:10}")
    private int threadPoolSize;
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // Only for scheduling, not execution
    private final Map<String, ScheduledFuture<?>> activeSchedules = new ConcurrentHashMap<>();
    private final CronParser cronParser;
    
    public WorkflowSchedulerService() {
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
    }
    
    @PostConstruct
    public void init() {
        if (schedulerEnabled) {
            logger.info("Starting workflow scheduler service");
            recoverActiveSchedules();
            startSchedulePoller();
        }
    }
    
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down workflow scheduler service");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Create a one-time scheduled workflow
     */
    @Transactional
    public String scheduleWorkflow(
            String workflowName,
            Integer workflowVersion,
            Instant executionTime,
            Map<String, Object> inputData,
            String description,
            String createdBy) {
        
        String scheduleId = UUID.randomUUID().toString();
        
        ScheduledWorkflowEntity schedule = ScheduledWorkflowEntity.builder()
            .scheduleId(scheduleId)
            .workflowName(workflowName)
            .workflowVersion(workflowVersion)
            .description(description)
            .scheduleType(ScheduleType.ONE_TIME)
            .startTime(executionTime)
            .nextExecutionTime(executionTime)
            .inputData(inputData != null ? inputData : new HashMap<>())
            .status(ScheduleStatus.ACTIVE)
            .createdBy(createdBy)
            .createdAt(Instant.now())
            .build();
        
        scheduledWorkflowRepository.save(schedule);
        
        logger.info("Created one-time schedule {} for workflow {} at {}", 
            scheduleId, workflowName, executionTime);
        
        scheduleNextExecution(schedule);
        
        return scheduleId;
    }
    
    /**
     * Create a recurring scheduled workflow with fixed rate
     */
    @Transactional
    public String scheduleRecurringWorkflow(
            String workflowName,
            Integer workflowVersion,
            Duration interval,
            boolean fixedRate,
            Map<String, Object> inputData,
            String description,
            String createdBy,
            Instant startTime,
            Instant endTime) {
        
        String scheduleId = UUID.randomUUID().toString();
        
        ScheduledWorkflowEntity schedule = ScheduledWorkflowEntity.builder()
            .scheduleId(scheduleId)
            .workflowName(workflowName)
            .workflowVersion(workflowVersion)
            .description(description)
            .scheduleType(fixedRate ? ScheduleType.FIXED_RATE : ScheduleType.FIXED_DELAY)
            .fixedRateSeconds(fixedRate ? interval.getSeconds() : null)
            .fixedDelaySeconds(!fixedRate ? interval.getSeconds() : null)
            .startTime(startTime)
            .endTime(endTime)
            .nextExecutionTime(startTime != null ? startTime : Instant.now())
            .inputData(inputData != null ? inputData : new HashMap<>())
            .status(ScheduleStatus.ACTIVE)
            .createdBy(createdBy)
            .createdAt(Instant.now())
            .build();
        
        scheduledWorkflowRepository.save(schedule);
        
        logger.info("Created recurring schedule {} for workflow {} with interval {}", 
            scheduleId, workflowName, interval);
        
        scheduleNextExecution(schedule);
        
        return scheduleId;
    }
    
    /**
     * Create a cron-based scheduled workflow
     */
    @Transactional
    public String scheduleCronWorkflow(
            String workflowName,
            Integer workflowVersion,
            String cronExpression,
            Map<String, Object> inputData,
            String description,
            String createdBy,
            String timezone,
            Instant endTime) {
        
        // Validate cron expression
        try {
            cronParser.parse(cronExpression);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid cron expression: " + cronExpression, e);
        }
        
        String scheduleId = UUID.randomUUID().toString();
        
        ScheduledWorkflowEntity schedule = ScheduledWorkflowEntity.builder()
            .scheduleId(scheduleId)
            .workflowName(workflowName)
            .workflowVersion(workflowVersion)
            .description(description)
            .scheduleType(ScheduleType.CRON)
            .cronExpression(cronExpression)
            .timezone(timezone != null ? timezone : "UTC")
            .endTime(endTime)
            .nextExecutionTime(calculateNextCronExecution(cronExpression, timezone))
            .inputData(inputData != null ? inputData : new HashMap<>())
            .status(ScheduleStatus.ACTIVE)
            .createdBy(createdBy)
            .createdAt(Instant.now())
            .build();
        
        scheduledWorkflowRepository.save(schedule);
        
        logger.info("Created cron schedule {} for workflow {} with expression {}", 
            scheduleId, workflowName, cronExpression);
        
        scheduleNextExecution(schedule);
        
        return scheduleId;
    }
    
    /**
     * Pause a scheduled workflow
     */
    @Transactional
    public boolean pauseSchedule(String scheduleId) {
        Optional<ScheduledWorkflowEntity> scheduleOpt = scheduledWorkflowRepository.findById(scheduleId);
        
        if (scheduleOpt.isPresent()) {
            ScheduledWorkflowEntity schedule = scheduleOpt.get();
            schedule.setStatus(ScheduleStatus.PAUSED);
            scheduledWorkflowRepository.save(schedule);
            
            // Cancel active schedule
            ScheduledFuture<?> future = activeSchedules.remove(scheduleId);
            if (future != null) {
                future.cancel(false);
            }
            
            logger.info("Paused schedule {}", scheduleId);
            return true;
        }
        
        return false;
    }
    
    /**
     * Resume a paused scheduled workflow
     */
    @Transactional
    public boolean resumeSchedule(String scheduleId) {
        Optional<ScheduledWorkflowEntity> scheduleOpt = scheduledWorkflowRepository.findById(scheduleId);
        
        if (scheduleOpt.isPresent()) {
            ScheduledWorkflowEntity schedule = scheduleOpt.get();
            
            if (schedule.getStatus() == ScheduleStatus.PAUSED) {
                schedule.setStatus(ScheduleStatus.ACTIVE);
                
                // Recalculate next execution time
                Instant nextExecution = calculateNextExecutionTime(schedule);
                schedule.setNextExecutionTime(nextExecution);
                
                scheduledWorkflowRepository.save(schedule);
                
                scheduleNextExecution(schedule);
                
                logger.info("Resumed schedule {}", scheduleId);
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Delete a scheduled workflow
     */
    @Transactional
    public boolean deleteSchedule(String scheduleId) {
        Optional<ScheduledWorkflowEntity> scheduleOpt = scheduledWorkflowRepository.findById(scheduleId);
        
        if (scheduleOpt.isPresent()) {
            // Cancel active schedule
            ScheduledFuture<?> future = activeSchedules.remove(scheduleId);
            if (future != null) {
                future.cancel(false);
            }
            
            scheduledWorkflowRepository.deleteById(scheduleId);
            
            logger.info("Deleted schedule {}", scheduleId);
            return true;
        }
        
        return false;
    }
    
    /**
     * Get schedule details
     */
    public Optional<ScheduledWorkflowEntity> getSchedule(String scheduleId) {
        return scheduledWorkflowRepository.findById(scheduleId);
    }
    
    /**
     * Get all schedules for a workflow
     */
    public List<ScheduledWorkflowEntity> getSchedulesByWorkflow(String workflowName) {
        return scheduledWorkflowRepository.findByWorkflowName(workflowName);
    }
    
    /**
     * Get all active schedules
     */
    public List<ScheduledWorkflowEntity> getActiveSchedules() {
        return scheduledWorkflowRepository.findByStatus(ScheduleStatus.ACTIVE);
    }
    
    /**
     * Execute scheduled workflow
     * 
     * This method creates a WorkflowInstanceEntity entry with TriggerType.SCHEDULED.
     * The WorkflowProcessor will pick it up through its normal polling mechanism,
     * ensuring all workflows share the same execution pool.
     * 
     * Uses pessimistic locking to ensure only one instance processes each schedule
     * in a distributed environment.
     */
    @Transactional
    private void executeScheduledWorkflow(ScheduledWorkflowEntity schedule) {
        try {
            // Try to lock and claim this schedule execution
            Optional<ScheduledWorkflowEntity> lockedSchedule = 
                scheduledWorkflowRepository.findAndLockSchedule(schedule.getScheduleId());
            
            if (lockedSchedule.isEmpty()) {
                logger.debug("Schedule {} already being processed by another instance", 
                    schedule.getScheduleId());
                return;
            }
            
            schedule = lockedSchedule.get();
            
            // Double-check the schedule should still execute
            if (!schedule.shouldExecute()) {
                logger.debug("Schedule {} should not execute now", schedule.getScheduleId());
                return;
            }
            
            logger.info("Creating scheduled workflow instance {} - {}", 
                schedule.getScheduleId(), schedule.getWorkflowName());
            
            // Get workflow definition
            Optional<WorkflowDefinition> workflowDef;
            if (schedule.getWorkflowVersion() != null) {
                workflowDef = definitionService.getWorkflowDefinition(
                    schedule.getWorkflowName(), schedule.getWorkflowVersion());
            } else {
                workflowDef = definitionService.getLatestWorkflowDefinition(
                    schedule.getWorkflowName());
            }
            
            if (workflowDef.isEmpty()) {
                throw new IllegalStateException("Workflow definition not found: " + 
                    schedule.getWorkflowName());
            }
            
            // Prepare input data with schedule metadata
            Map<String, Object> inputData = new HashMap<>(schedule.getInputData());
            inputData.put("scheduleId", schedule.getScheduleId());
            inputData.put("scheduledTime", schedule.getNextExecutionTime());
            inputData.put("executionCount", schedule.getExecutionCount() + 1);
            
            // Create workflow instance in database with SCHEDULED trigger type
            String workflowInstanceId = UUID.randomUUID().toString();
            String correlationId = "scheduled-" + schedule.getScheduleId();
            
            WorkflowInstanceEntity instance = workflowPersistenceService.createWorkflowInstance(
                workflowInstanceId,
                workflowDef.get().getName(),
                workflowDef.get().getVersion(),
                inputData,
                correlationId,
                    null
            );
            
            // Set the trigger type and schedule reference
            instance.setTriggerType(TriggerType.SCHEDULED);
            instance.setScheduleId(schedule.getScheduleId());
            workflowPersistenceService.updateWorkflowInstance(instance);
            
            // Create initial tasks for the workflow
            workflowPersistenceService.createInitialTasks(instance, workflowDef.get());
            
            // Record the execution attempt
            scheduledWorkflowRepository.recordSuccessfulExecution(
                schedule.getScheduleId(),
                Instant.now(),
                workflowInstanceId,
                "PENDING"
            );
            
            logger.info("Created workflow instance {} for schedule {}", 
                workflowInstanceId, schedule.getScheduleId());
            
            // Monitor the workflow completion in a separate thread
            monitorScheduledWorkflowCompletion(schedule, workflowInstanceId);
            
        } catch (Exception e) {
            logger.error("Error creating scheduled workflow instance {}", schedule.getScheduleId(), e);
            handleExecutionFailure(schedule, e);
            scheduleNextExecutionAfterCompletion(schedule);
        }
    }
    
    /**
     * Monitor scheduled workflow completion
     * 
     * Polls the workflow instance status to check when it completes,
     * then handles scheduling the next execution.
     */
    private void monitorScheduledWorkflowCompletion(ScheduledWorkflowEntity schedule, String workflowInstanceId) {
        // Use the scheduler to periodically check workflow completion
        ScheduledFuture<?> monitorFuture = scheduler.scheduleWithFixedDelay(() -> {
            try {
                Optional<WorkflowInstanceEntity> instanceOpt = 
                    workflowPersistenceService.findWorkflowInstance(workflowInstanceId);
                
                if (instanceOpt.isPresent()) {
                    WorkflowInstanceEntity instance = instanceOpt.get();
                    WorkflowState state = instance.getState();
                    
                    if (state == WorkflowState.COMPLETED || 
                        state == WorkflowState.FAILED || 
                        state == WorkflowState.TERMINATED) {
                        
                        // Workflow has finished
                        if (state == WorkflowState.COMPLETED) {
                            handleExecutionSuccess(schedule, workflowInstanceId, state);
                        } else {
                            handleExecutionFailure(schedule, 
                                new RuntimeException("Workflow " + state.name().toLowerCase()));
                        }
                        
                        // Schedule next execution if applicable
                        scheduleNextExecutionAfterCompletion(schedule);
                        
                        // Cancel this monitoring task
                        throw new RuntimeException("Workflow completed");
                    }
                }
            } catch (RuntimeException e) {
                if (e.getMessage().equals("Workflow completed")) {
                    // Expected - cancel the monitoring
                    throw e;
                }
                logger.error("Error monitoring workflow {}", workflowInstanceId, e);
            }
        }, 5, 5, TimeUnit.SECONDS);
        
        // Cancel monitoring after 1 hour (safety timeout)
        scheduler.schedule(() -> {
            if (!monitorFuture.isDone()) {
                monitorFuture.cancel(true);
                logger.warn("Monitoring timeout for workflow {}", workflowInstanceId);
                handleExecutionFailure(schedule, new RuntimeException("Monitoring timeout"));
                scheduleNextExecutionAfterCompletion(schedule);
            }
        }, 1, TimeUnit.HOURS);
    }
    
    /**
     * Handle successful execution
     */
    @Transactional
    public void handleExecutionSuccess(ScheduledWorkflowEntity schedule, String workflowInstanceId, WorkflowState state) {
        scheduledWorkflowRepository.recordSuccessfulExecution(
            schedule.getScheduleId(),
            Instant.now(),
            workflowInstanceId,
            state.name()
        );
        
        // Check if workflow actually succeeded
        if (state != WorkflowState.COMPLETED) {
            logger.warn("Scheduled workflow {} completed with state: {}", 
                schedule.getScheduleId(), state);
            
            if (state == WorkflowState.FAILED && schedule.getAlertOnFailure()) {
                sendFailureAlert(schedule, "Workflow failed with state: " + state);
            }
        }
        
        logger.info("Successfully executed scheduled workflow {} - Instance: {}", 
            schedule.getScheduleId(), workflowInstanceId);
    }
    
    /**
     * Handle execution failure
     */
    @Transactional
    public void handleExecutionFailure(ScheduledWorkflowEntity schedule, Throwable error) {
        scheduledWorkflowRepository.recordFailedExecution(
            schedule.getScheduleId(),
            Instant.now(),
            null,
            "ERROR"
        );
        
        logger.error("Failed to execute scheduled workflow {}", schedule.getScheduleId(), error);
        
        // Send alert if configured
        if (schedule.getAlertOnFailure()) {
            sendFailureAlert(schedule, error.getMessage());
        }
        
        // Check if should disable schedule
        schedule = scheduledWorkflowRepository.findById(schedule.getScheduleId()).orElse(schedule);
        if (schedule.getConsecutiveFailures() >= schedule.getMaxConsecutiveFailures()) {
            logger.error("Schedule {} disabled due to {} consecutive failures", 
                schedule.getScheduleId(), schedule.getConsecutiveFailures());
            scheduledWorkflowRepository.updateStatus(
                schedule.getScheduleId(), ScheduleStatus.ERROR, Instant.now());
        }
    }
    
    /**
     * Send failure alert email
     */
    private void sendFailureAlert(ScheduledWorkflowEntity schedule, String errorMessage) {
        // to-do
    }
    
    /**
     * Schedule next execution after completion
     */
    private void scheduleNextExecutionAfterCompletion(ScheduledWorkflowEntity schedule) {
        schedule = scheduledWorkflowRepository.findById(schedule.getScheduleId()).orElse(schedule);
        
        if (schedule.getStatus() != ScheduleStatus.ACTIVE) {
            return;
        }
        
        if (schedule.getScheduleType() == ScheduleType.ONE_TIME) {
            // One-time schedule completed
            scheduledWorkflowRepository.updateStatus(
                schedule.getScheduleId(), ScheduleStatus.COMPLETED, Instant.now());
        } else {
            // Calculate and schedule next execution
            Instant nextExecution = calculateNextExecutionTime(schedule);
            
            if (nextExecution != null) {
                scheduledWorkflowRepository.updateNextExecutionTime(
                    schedule.getScheduleId(), nextExecution, Instant.now());
                schedule.setNextExecutionTime(nextExecution);
                scheduleNextExecution(schedule);
            } else {
                // No next execution (end time reached)
                scheduledWorkflowRepository.updateStatus(
                    schedule.getScheduleId(), ScheduleStatus.EXPIRED, Instant.now());
            }
        }
    }
    
    /**
     * Schedule next execution
     */
    private void scheduleNextExecution(ScheduledWorkflowEntity schedule) {
        if (schedule.getStatus() != ScheduleStatus.ACTIVE || 
            schedule.getNextExecutionTime() == null) {
            return;
        }
        
        long delay = Duration.between(Instant.now(), schedule.getNextExecutionTime()).toMillis();
        
        if (delay < 0) {
            // Already past due, execute immediately
            delay = 0;
        }
        
        ScheduledFuture<?> future = scheduler.schedule(
            () -> executeScheduledWorkflow(schedule),
            delay,
            TimeUnit.MILLISECONDS
        );
        
        activeSchedules.put(schedule.getScheduleId(), future);
        
        logger.debug("Scheduled next execution for {} at {}", 
            schedule.getScheduleId(), schedule.getNextExecutionTime());
    }
    
    /**
     * Calculate next execution time based on schedule type
     */
    private Instant calculateNextExecutionTime(ScheduledWorkflowEntity schedule) {
        Instant now = Instant.now();
        
        // Check end time
        if (schedule.getEndTime() != null && now.isAfter(schedule.getEndTime())) {
            return null;
        }
        
        switch (schedule.getScheduleType()) {
            case ONE_TIME:
                return null; // No next execution for one-time
                
            case FIXED_RATE:
                if (schedule.getLastExecutionTime() != null) {
                    return schedule.getLastExecutionTime()
                        .plusSeconds(schedule.getFixedRateSeconds());
                }
                return now.plusSeconds(schedule.getFixedRateSeconds());
                
            case FIXED_DELAY:
                return now.plusSeconds(schedule.getFixedDelaySeconds());
                
            case CRON:
                return calculateNextCronExecution(
                    schedule.getCronExpression(), schedule.getTimezone());
                
            default:
                return null;
        }
    }
    
    /**
     * Calculate next cron execution time
     */
    private Instant calculateNextCronExecution(String cronExpression, String timezone) {
        try {
            Cron cron = cronParser.parse(cronExpression);
            ZoneId zoneId = timezone != null ? ZoneId.of(timezone) : ZoneId.of("UTC");
            ZonedDateTime now = ZonedDateTime.now(zoneId);
            
            ExecutionTime executionTime = ExecutionTime.forCron(cron);
            Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(now);
            
            return nextExecution.map(ZonedDateTime::toInstant).orElse(null);
            
        } catch (Exception e) {
            logger.error("Error calculating cron execution for expression: {}", cronExpression, e);
            return null;
        }
    }
    
    /**
     * Recover active schedules on startup
     */
    private void recoverActiveSchedules() {
        logger.info("Recovering active schedules");
        
        List<ScheduledWorkflowEntity> activeSchedules = 
            scheduledWorkflowRepository.findActiveRecurringSchedules();
        
        for (ScheduledWorkflowEntity schedule : activeSchedules) {
            try {
                // Recalculate next execution if needed
                if (schedule.getNextExecutionTime() == null || 
                    schedule.getNextExecutionTime().isBefore(Instant.now())) {
                    
                    Instant nextExecution = calculateNextExecutionTime(schedule);
                    
                    if (nextExecution != null) {
                        schedule.setNextExecutionTime(nextExecution);
                        scheduledWorkflowRepository.save(schedule);
                    }
                }
                
                scheduleNextExecution(schedule);
                logger.info("Recovered schedule: {} - {}", 
                    schedule.getScheduleId(), schedule.getWorkflowName());
                    
            } catch (Exception e) {
                logger.error("Failed to recover schedule: {}", schedule.getScheduleId(), e);
            }
        }
        
        logger.info("Recovered {} active schedules", activeSchedules.size());
    }
    
    /**
     * Poll for due schedules (backup mechanism)
     */
    private void startSchedulePoller() {
        scheduler.scheduleWithFixedDelay(this::pollDueSchedules, 
            pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Poll and execute due schedules
     */
    private void pollDueSchedules() {
        try {
            List<ScheduledWorkflowEntity> dueSchedules = 
                scheduledWorkflowRepository.findDueSchedules(Instant.now());
            
            for (ScheduledWorkflowEntity schedule : dueSchedules) {
                if (!activeSchedules.containsKey(schedule.getScheduleId())) {
                    logger.warn("Found unscheduled due workflow: {}", schedule.getScheduleId());
                    executeScheduledWorkflow(schedule);
                }
            }
            
        } catch (Exception e) {
            logger.error("Error polling due schedules", e);
        }
    }
    
    /**
     * Clean up old completed schedules
     */
    @Scheduled(cron = "0 0 2 * * *") // Daily at 2 AM
    public void cleanupOldSchedules() {
        try {
            Instant cutoff = Instant.now().minus(30, ChronoUnit.DAYS);
            int deleted = scheduledWorkflowRepository.deleteOldCompletedSchedules(cutoff);
            
            if (deleted > 0) {
                logger.info("Cleaned up {} old completed schedules", deleted);
            }
            
        } catch (Exception e) {
            logger.error("Error cleaning up old schedules", e);
        }
    }
}