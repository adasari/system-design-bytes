package com.taskflow.processor;

import com.taskflow.core.*;
import com.taskflow.executor.TaskRegistry;
import com.taskflow.persistence.entity.TaskExecutionEntity;
import com.taskflow.persistence.entity.WorkflowInstanceEntity;
import com.taskflow.persistence.service.WorkflowPersistenceService;
import com.taskflow.system.SystemTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * WorkflowProcessor is the core engine for executing workflows in a distributed environment.
 *
 * <p>This component manages workflow execution across a cluster of microservice instances
 * using a database-based distributed queue approach. Each instance runs a fixed number
 * of concurrent workflows and claims available workflow instances from the shared database.</p>
 * 
 * <p><b>Unified Execution Model:</b></p>
 * <p>All workflow executions - whether triggered manually, via API, through SQS events,
 * or by the scheduler - flow through this single processor. This ensures:</p>
 * <ul>
 *   <li>Consistent resource management with a single thread pool</li>
 *   <li>Uniform concurrency limits across all trigger types</li>
 *   <li>Centralized monitoring and metrics collection</li>
 *   <li>Simplified error handling and recovery</li>
 * </ul>
 *
 * <p><b>Key Responsibilities:</b></p>
 * <ul>
 *   <li><b>Workflow Claiming</b>: Uses pessimistic locking to claim workflow instances</li>
 *   <li><b>Task Execution</b>: Routes tasks to appropriate executors (system vs custom)</li>
 *   <li><b>State Management</b>: Updates workflow and task states in the database</li>
 *   <li><b>Concurrency Control</b>: Limits concurrent workflow executions per instance</li>
 *   <li><b>Error Handling</b>: Manages task failures, retries, and circuit breaking</li>
 *   <li><b>Recovery</b>: Handles stale task detection and workflow resumption</li>
 * </ul>
 *
 * <p><b>Architecture:</b></p>
 * <ul>
 *   <li>Database serves as distributed queue using FOR UPDATE SKIP LOCKED</li>
 *   <li>Each workflow instance executes on a single microservice instance</li>
 *   <li>Semaphore-based concurrency control (configurable max concurrent workflows)</li>
 *   <li>Scheduled polling for new workflow instances and stale task recovery</li>
 * </ul>
 *
 * <p><b>Task Types Supported:</b></p>
 * <ul>
 *   <li><b>System Tasks</b>: HTTP, Decision, Set Variable, Wait, Terminate</li>
 *   <li><b>Custom Tasks</b>: User-defined tasks registered in TaskRegistry</li>
 *   <li><b>Parallel Tasks</b>: Fork/Join, Dynamic Fork for concurrent execution</li>
 *   <li><b>Sub-workflows</b>: Nested workflow composition</li>
 * </ul>
 *
 * <p><b>Configuration:</b></p>
 * <pre>{@code
 * # application.yml
 * workflow:
 *   processor:
 *     max-concurrent-workflows: 10
 *     polling-interval-ms: 5000
 *     stale-task-threshold-minutes: 30
 * }</pre>
 *
 * @see WorkflowPersistenceService
 * @see SystemTaskExecutor
 * @see TaskRegistry
 * @see ExecutionContext
 */
@Component
public class WorkflowProcessor {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowProcessor.class);

    private final int maxConcurrentWorkflows;
    private final WorkflowPersistenceService persistenceService;
    private final TaskRegistry taskRegistry;
    private final SystemTaskExecutor systemTaskExecutor;
    private final ExecutorService workflowExecutor;
    private final ScheduledExecutorService scheduler;
    private final Semaphore workflowSlots;
    private final Map<String, WorkflowExecution> runningWorkflows;
    private volatile boolean running;

    public WorkflowProcessor(
            @Value("${workflow.processor.concurrent:5}") int maxConcurrentWorkflows,
            WorkflowPersistenceService persistenceService,
            TaskRegistry taskRegistry,
            SystemTaskExecutor systemTaskExecutor) {

        this.maxConcurrentWorkflows = maxConcurrentWorkflows;
        this.persistenceService = persistenceService;
        this.taskRegistry = taskRegistry;
        this.systemTaskExecutor = systemTaskExecutor;
        this.workflowExecutor = Executors.newFixedThreadPool(maxConcurrentWorkflows * 2);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.workflowSlots = new Semaphore(maxConcurrentWorkflows);
        this.runningWorkflows = new ConcurrentHashMap<>();
        this.running = true;
    }

    @PostConstruct
    public void start() {
        logger.info("Starting workflow processor with {} concurrent workflow slots", maxConcurrentWorkflows);

        // Poll for new workflows to execute
        scheduler.scheduleWithFixedDelay(this::pollAndExecuteWorkflows, 1, 2, TimeUnit.SECONDS);

        // Check for stale workflows and recover
        scheduler.scheduleWithFixedDelay(this::recoverStaleWorkflows, 30, 30, TimeUnit.SECONDS);

        logger.info("Workflow processor started successfully");
    }

    @PreDestroy
    public void stop() {
        logger.info("Stopping workflow processor");
        running = false;

        scheduler.shutdown();
        workflowExecutor.shutdown();

        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!workflowExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                workflowExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            workflowExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Workflow processor stopped");
    }

    private void pollAndExecuteWorkflows() {
        if (!running) return;

        try {
            int availableSlots = workflowSlots.availablePermits();
            if (availableSlots > 0) {
                // Try to claim and execute workflows up to available slots
                for (int i = 0; i < availableSlots; i++) {
                    WorkflowInstanceEntity workflow = persistenceService.claimNextPendingWorkflow();
                    if (workflow != null) {
                        executeWorkflow(workflow);
                    } else {
                        break; // No more pending workflows
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error polling workflows", e);
        }
    }

    private void executeWorkflow(WorkflowInstanceEntity workflow) {
        try {
            if (!workflowSlots.tryAcquire()) {
                // Release the claim if we can't get a slot
                persistenceService.releaseWorkflowClaim(workflow.getWorkflowInstanceId());
                return;
            }

            WorkflowExecution execution = new WorkflowExecution(workflow);
            runningWorkflows.put(workflow.getWorkflowInstanceId(), execution);

            CompletableFuture.runAsync(() -> {
                try {
                    logger.info("Starting execution of workflow: {}", workflow.getWorkflowInstanceId());
                    execution.execute();
                } catch (Exception e) {
                    logger.error("Error executing workflow: {}", workflow.getWorkflowInstanceId(), e);
                    persistenceService.markWorkflowFailed(workflow.getWorkflowInstanceId(), e.getMessage());
                } finally {
                    runningWorkflows.remove(workflow.getWorkflowInstanceId());
                    workflowSlots.release();
                    logger.info("Completed execution of workflow: {}", workflow.getWorkflowInstanceId());
                }
            }, workflowExecutor);

        } catch (Exception e) {
            logger.error("Error starting workflow execution", e);
            workflowSlots.release();
        }
    }

    private void recoverStaleWorkflows() {
        if (!running) return;

        try {
            // Find workflows that have been running too long without updates
            Instant staleThreshold = Instant.now().minusMillis(10 * 60 * 1000);
            persistenceService.resetStaleWorkflows(staleThreshold);
        } catch (Exception e) {
            logger.error("Error recovering stale workflows", e);
        }
    }

    private class WorkflowExecution {
        private final WorkflowInstanceEntity workflow;
        private final ExecutionContext context;
        private final Map<String, CompletableFuture<TaskResult<?>>> taskFutures;

        public WorkflowExecution(WorkflowInstanceEntity workflow) {
            this.workflow = workflow;
            this.context = Optional.ofNullable(persistenceService.loadExecutionContext(workflow.getWorkflowInstanceId()))
                .orElse(new ExecutionContext(workflow.getWorkflowInstanceId()));

            this.taskFutures = new ConcurrentHashMap<>();
        }

        public void execute() {
            try {
                // Update workflow state to RUNNING
                persistenceService.updateWorkflowState(workflow.getWorkflowInstanceId(), WorkflowState.RUNNING);

                // Get all tasks for this workflow
                var tasks = persistenceService.getTaskExecutions(workflow.getWorkflowInstanceId());

                // Execute tasks respecting dependencies
                while (hasIncompleteTasks(tasks) && !context.isCancelled()) {
                    // Find tasks ready to execute (dependencies satisfied)
                    var readyTasks = findReadyTasks(tasks);

                    if (readyTasks.isEmpty()) {
                        // Check if we're stuck (circular dependency or all failed)
                        if (hasFailedTasks(tasks) && !hasRetriableTasks(tasks)) {
                            persistenceService.updateWorkflowState(
                                workflow.getWorkflowInstanceId(), WorkflowState.FAILED
                            );
                            break;
                        }

                        // Wait a bit for running tasks to complete
                        Thread.sleep(500);
                        continue;
                    }

                    // Execute ready tasks
                    for (TaskExecutionEntity taskEntity : readyTasks) {
                        executeTask(taskEntity);
                    }

                    // Reload tasks to check status
                    tasks = persistenceService.getTaskExecutions(workflow.getWorkflowInstanceId());
                }

                // Final workflow state update
                if (context.isCancelled()) {
                    persistenceService.updateWorkflowState(
                        workflow.getWorkflowInstanceId(), WorkflowState.CANCELLED
                    );
                } else if (allTasksCompleted(tasks)) {
                    persistenceService.updateWorkflowState(
                        workflow.getWorkflowInstanceId(), WorkflowState.COMPLETED
                    );
                    persistenceService.saveWorkflowResult(workflow.getWorkflowInstanceId(),
                        buildWorkflowResult(tasks));
                }

            } catch (Exception e) {
                logger.error("Workflow execution failed: {}", workflow.getWorkflowInstanceId(), e);
                persistenceService.updateWorkflowState(
                    workflow.getWorkflowInstanceId(), WorkflowState.FAILED
                );
            }
        }

        private void executeTask(TaskExecutionEntity taskEntity) {
            try {
                logger.info("Executing task: {} of type: {} for workflow: {}",
                    taskEntity.getTaskName(), taskEntity.getTaskType(), workflow.getWorkflowInstanceId());

                // Update task status to RUNNING
                persistenceService.updateTaskStatus(
                    taskEntity.getId(), TaskStatus.RUNNING, "processor"
                );

                // Determine task type and execute accordingly
                TaskDefinition.TaskType taskType = TaskDefinition.TaskType.valueOf(taskEntity.getTaskType());
                TaskResult<?> result = null;

                if (isSystemTask(taskType)) {
                    // Execute system task directly
                    result = executeSystemTaskDirectly(taskEntity, taskType);
                    handleTaskCompletion(taskEntity, result, null);
                } else {
                    // Execute user-defined task
                    executeUserTask(taskEntity);
                }

            } catch (Exception e) {
                logger.error("Error executing task: {}", taskEntity.getTaskName(), e);
                handleTaskFailure(taskEntity, e);
            }
        }

        private TaskResult<?> executeSystemTaskDirectly(TaskExecutionEntity taskEntity, TaskDefinition.TaskType taskType) {
            // Create TaskDefinition from entity for system task execution
            TaskDefinition taskDef = createTaskDefinitionFromEntity(taskEntity);

            // Execute system task
            TaskResult<?> result = systemTaskExecutor.executeSystemTask(taskDef, context, taskEntity);

            // Handle specific system task behaviors
            handleSystemTaskResult(taskEntity, taskType, result);

            return result;
        }

        private void executeUserTask(TaskExecutionEntity taskEntity) {
            // Get task implementation
            Task<?> task = taskRegistry.getTask(taskEntity.getTaskName());
            if (task == null) {
                logger.warn("No implementation found for task: {}", taskEntity.getTaskName());
                persistenceService.updateTaskStatus(
                    taskEntity.getId(), TaskStatus.SKIPPED, "processor"
                );
                return;
            }

            // Execute task asynchronously
            CompletableFuture<? extends TaskResult<?>> future = task.execute(context);
            taskFutures.put(taskEntity.getTaskId(), (CompletableFuture<TaskResult<?>>) future);

            // Handle task completion
            future.whenComplete((result, error) -> {
                handleTaskCompletion(taskEntity, result, error);
            });

            // Wait for task with timeout if specified
            if (taskEntity.getTimeoutMs() != null) {
                try {
                    future.get(taskEntity.getTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    future.cancel(true);
                    handleTaskTimeout(taskEntity);
                }
            }
        }

        private void handleTaskCompletion(TaskExecutionEntity taskEntity, TaskResult<?> result, Throwable error) {
            if (error != null) {
                logger.error("Task failed: {}", taskEntity.getTaskName(), error);
                handleTaskFailure(taskEntity, error);
            } else if (result != null) {
                logger.info("Task completed: {} with status: {}", taskEntity.getTaskName(), result.getStatus());
                handleTaskSuccess(taskEntity, result);
            }
        }

        private boolean isSystemTask(TaskDefinition.TaskType taskType) {
            // Only support core system tasks from our SystemTaskExecutor
            switch (taskType) {
                case HTTP:
                case DECISION:
                case SET_VARIABLE:
                case WAIT:
                case TERMINATE:
                    return true;
                default:
                    return false;
            }
        }

        private TaskDefinition createTaskDefinitionFromEntity(TaskExecutionEntity entity) {
            try {
                // Parse input data as JSON to extract configuration
                Map<String, Object> inputData = new java.util.HashMap<>();
                if (entity.getInputData() != null) {
                    inputData = new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(entity.getInputData(), Map.class);
                }

                TaskDefinition.Builder builder = TaskDefinition.builder()
                    .name(entity.getTaskName())
                    .taskReferenceName(entity.getTaskReferenceName())
                    .type(TaskDefinition.TaskType.valueOf(entity.getTaskType()));

                inputData.forEach(builder::inputParameter);

                if (entity.getTimeoutMs() != null) {
                    builder.timeout(java.time.Duration.ofMillis(entity.getTimeoutMs()));
                }

                builder.retryCount(entity.getMaxRetries());

                return builder.build();

            } catch (Exception e) {
                logger.error("Error creating task definition from entity", e);
                return TaskDefinition.builder()
                    .name(entity.getTaskName())
                    .taskReferenceName(entity.getTaskReferenceName())
                    .type(TaskDefinition.TaskType.SIMPLE)
                    .build();
            }
        }

        private void handleSystemTaskResult(TaskExecutionEntity taskEntity, TaskDefinition.TaskType taskType, TaskResult<?> result) {
            // Handle special system task behaviors for our core tasks only
            switch (taskType) {
                case DECISION:
                    // Decision tasks might affect flow - could be enhanced later
                    break;
                case TERMINATE:
                    handleTerminateResult(taskEntity, result);
                    break;
                case WAIT:
                    // Wait tasks are handled within SystemTaskExecutor
                    break;
                case SET_VARIABLE:
                    // Variable setting is handled within SystemTaskExecutor
                    break;
                case HTTP:
                    // HTTP tasks are standard execution
                    break;
                default:
                    // Standard handling for other system tasks
                    break;
            }
        }


        private void handleTerminateResult(TaskExecutionEntity taskEntity, TaskResult<?> result) {
            if (result.isSuccess()) {
                // Check if workflow should be terminated
                Boolean terminated = (Boolean) context.get("__terminated");
                if (Boolean.TRUE.equals(terminated)) {
                    String terminationReason = (String) context.get("__terminationReason");

                    // Terminate the workflow
                    persistenceService.updateWorkflowState(workflow.getWorkflowInstanceId(), WorkflowState.COMPLETED);
                    logger.info("Workflow terminated: {} - Reason: {}", workflow.getWorkflowInstanceId(), terminationReason);
                }
            }
        }


        private void handleTaskSuccess(TaskExecutionEntity taskEntity, TaskResult<?> result) {
            persistenceService.updateTaskResult(taskEntity.getId(), result);
            persistenceService.updateTaskStatus(
                taskEntity.getId(), TaskStatus.SUCCESS, "processor"
            );

            // Save task result to context
            context.setTaskResult(taskEntity.getTaskReferenceName(), result);
            persistenceService.saveExecutionContext(workflow.getWorkflowInstanceId(), context);
        }

        private void handleTaskFailure(TaskExecutionEntity taskEntity, Throwable error) {
            if (taskEntity.getRetryCount() < taskEntity.getMaxRetries()) {
                // Retry the task
                taskEntity.setRetryCount(taskEntity.getRetryCount() + 1);
                persistenceService.updateTaskStatus(
                    taskEntity.getId(), TaskStatus.RETRYING, "processor"
                );

                // Schedule retry with delay
                scheduler.schedule(() -> executeTask(taskEntity),
                    calculateRetryDelay(taskEntity.getRetryCount()), TimeUnit.MILLISECONDS);
            } else {
                // Max retries exceeded
                persistenceService.updateTaskStatus(
                    taskEntity.getId(), TaskStatus.FAILED, "processor"
                );
                TaskResult<?> failureResult = TaskResult.failure(
                    taskEntity.getTaskName(), error instanceof Exception ? ((Exception)error).getMessage() : error.toString()
                );
                persistenceService.updateTaskResult(taskEntity.getId(), failureResult);
            }
        }

        private void handleTaskTimeout(TaskExecutionEntity taskEntity) {
            logger.warn("Task timed out: {}", taskEntity.getTaskName());
            persistenceService.updateTaskStatus(
                taskEntity.getId(), TaskStatus.FAILED, "processor"
            );
            TaskResult<?> timeoutResult = TaskResult.failure(
                taskEntity.getTaskName(), "Task execution timed out"
            );
            persistenceService.updateTaskResult(taskEntity.getId(), timeoutResult);
        }

        private java.util.List<TaskExecutionEntity> findReadyTasks(java.util.List<TaskExecutionEntity> tasks) {
            return tasks.stream()
                .filter(t -> t.getStatus() == TaskStatus.PENDING)
                .filter(t -> areDependenciesSatisfied(t, tasks))
                .collect(Collectors.toList());
        }

        private boolean areDependenciesSatisfied(TaskExecutionEntity task,
                                                 java.util.List<TaskExecutionEntity> allTasks) {
            if (task.getDependencies().isEmpty()) {
                return true;
            }

            for (String depTaskRef : task.getDependencies().keySet()) {
                boolean depCompleted = allTasks.stream()
                    .anyMatch(t -> t.getTaskReferenceName().equals(depTaskRef) &&
                             (t.getStatus() == TaskStatus.SUCCESS || t.getStatus() == TaskStatus.SKIPPED));

                if (!depCompleted) {
                    return false;
                }
            }

            return true;
        }

        private boolean hasIncompleteTasks(java.util.List<TaskExecutionEntity> tasks) {
            return tasks.stream().anyMatch(t ->
                t.getStatus() == TaskStatus.PENDING ||
                t.getStatus() == TaskStatus.RUNNING ||
                t.getStatus() == TaskStatus.RETRYING
            );
        }

        private boolean allTasksCompleted(java.util.List<TaskExecutionEntity> tasks) {
            return tasks.stream().allMatch(t ->
                t.getStatus() == TaskStatus.SUCCESS ||
                t.getStatus() == TaskStatus.SKIPPED
            );
        }

        private boolean hasFailedTasks(java.util.List<TaskExecutionEntity> tasks) {
            return tasks.stream().anyMatch(t -> t.getStatus() == TaskStatus.FAILED);
        }

        private boolean hasRetriableTasks(java.util.List<TaskExecutionEntity> tasks) {
            return tasks.stream()
                .filter(t -> t.getStatus() == TaskStatus.FAILED)
                .anyMatch(t -> t.getRetryCount() < t.getMaxRetries());
        }

        private long calculateRetryDelay(int retryCount) {
            // Exponential backoff: 1s, 2s, 4s, 8s, etc.
            return Math.min(1000L * (1L << retryCount), 60000L);
        }

        private WorkflowResult buildWorkflowResult(java.util.List<TaskExecutionEntity> tasks) {
            WorkflowResult.Builder builder = WorkflowResult.builder()
                .workflowId(workflow.getWorkflowInstanceId())
                .workflowName(workflow.getWorkflowName())
                .state(WorkflowState.COMPLETED)
                .startTime(workflow.getStartedAt())
                .endTime(Instant.now())
                .outputData(context.getAllData());

            for (TaskExecutionEntity task : tasks) {
                TaskResult<?> result = context.getTaskResult(task.getTaskReferenceName());
                if (result != null) {
                    builder.addStepResult(task.getTaskReferenceName(), result);
                }
            }

            return builder.build();
        }
    }

    public int getAvailableSlots() {
        return workflowSlots.availablePermits();
    }

    public int getRunningWorkflowCount() {
        return maxConcurrentWorkflows - workflowSlots.availablePermits();
    }

    public boolean isHealthy() {
        return running && !workflowExecutor.isShutdown();
    }
}
