package com.taskflow.persistence.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskflow.core.*;
import com.taskflow.definition.WorkflowDefinitionEntity;
import com.taskflow.persistence.entity.TaskExecutionEntity;
import com.taskflow.persistence.entity.WorkflowEventEntity;
import com.taskflow.persistence.entity.WorkflowInstanceEntity;
import com.taskflow.definition.WorkflowDefinitionService;
import com.taskflow.persistence.repository.TaskExecutionRepository;
import com.taskflow.persistence.repository.WorkflowInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Transactional
public class WorkflowPersistenceService {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowPersistenceService.class);

    @Autowired
    private WorkflowInstanceRepository workflowRepository;

    @Autowired
    private TaskExecutionRepository taskRepository;

    @Autowired
    private WorkflowDefinitionService definitionService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public WorkflowInstanceEntity createWorkflowInstance(String workflowId, String workflowInstanceId, Integer version,
                                                        Map<String, Object> inputs, String correlationId,
                                                         WorkflowDefinition workflowDefinition) {
        WorkflowInstanceEntity instance = new WorkflowInstanceEntity();
        instance.setWorkflowInstanceId(workflowInstanceId);
        instance.setWorkflowId(workflowId);
        instance.setWorkflowName(workflowDefinition.getName());
        instance.setWorkflowVersion(version);
        instance.setState(WorkflowState.NOT_STARTED);
        instance.setCorrelationId(correlationId);
        instance.setStartedAt(Instant.now());

        try {
            instance.setInputData(objectMapper.writeValueAsString(inputs));
        } catch (Exception e) {
            logger.error("Error serializing input data", e);
            instance.setInputData("{}");
        }

        return workflowRepository.save(instance);
    }

    public void createInitialTasks(WorkflowInstanceEntity workflow, WorkflowDefinition definition) {
        if (definition == null || definition.getTasks() == null) {
            return;
        }

        int sequenceNumber = 0;
        for (TaskDefinition taskDef : definition.getTasks()) {
            TaskExecutionEntity task = new TaskExecutionEntity();
            task.setTaskId(UUID.randomUUID().toString());
            task.setTaskName(taskDef.getName());
            task.setTaskReferenceName(taskDef.getTaskReferenceName());
            task.setTaskType(taskDef.getType().toString());
            task.setWorkflow(workflow);
            task.setSequenceNumber(sequenceNumber++);
            task.setStatus(TaskStatus.PENDING);
            task.setMaxRetries(taskDef.getRetryCount());
            task.setTimeoutMs(taskDef.getTimeout() != null ? taskDef.getTimeout().toMillis() : null);

            try {
                task.setInputData(objectMapper.writeValueAsString(taskDef.getInputParameters()));
            } catch (Exception e) {
                logger.error("Error serializing task input data", e);
                task.setInputData("{}");
            }

            for (String dep : taskDef.getDependencies()) {
                task.getDependencies().put(dep, "PENDING");
            }

            taskRepository.save(task);
            workflow.addTaskExecution(task);
        }

        workflowRepository.save(workflow);
    }

    @Transactional
    public WorkflowInstanceEntity claimNextPendingWorkflow() {
        return workflowRepository.claimNextPendingWorkflow();
    }

    @Transactional
    public void releaseWorkflowClaim(String workflowId) {
        workflowRepository.releaseWorkflowClaim(workflowId);
    }

    @Transactional
    public void resetStaleWorkflows(Instant threshold) {
        int resetCount = workflowRepository.resetStaleWorkflows(threshold);
        if (resetCount > 0) {
            logger.info("Reset {} stale workflows", resetCount);
        }
    }

    @Transactional
    public void markWorkflowFailed(String workflowId, String error) {
        workflowRepository.markWorkflowFailed(workflowId, Instant.now());

        WorkflowInstanceEntity workflow = getWorkflowInstance(workflowId);
        if (workflow != null) {
            workflow.addError(error);
            workflowRepository.save(workflow);
        }
    }

    public WorkflowInstanceEntity getWorkflowInstance(String workflowInstanceId) {
        return workflowRepository.findById(workflowInstanceId).orElse(null);
    }

    public Optional<WorkflowInstanceEntity> findWorkflowInstance(String workflowInstanceId) {
        return workflowRepository.findById(workflowInstanceId);
    }

    @Transactional
    public void updateWorkflowInstance(WorkflowInstanceEntity workflow) {
        workflowRepository.save(workflow);
    }

    public List<TaskExecutionEntity> getTaskExecutions(String workflowInstanceId) {
        return taskRepository.findByWorkflowId(workflowInstanceId);
    }

    public TaskExecutionEntity getTaskExecution(Long taskId) {
        return taskRepository.findById(taskId).orElse(null);
    }

    @Transactional
    public void updateWorkflowState(String workflowInstanceId, WorkflowState newState) {
        workflowRepository.updateState(workflowInstanceId, newState, Instant.now());

        if (newState == WorkflowState.COMPLETED || newState == WorkflowState.FAILED ||
            newState == WorkflowState.CANCELLED) {
            WorkflowInstanceEntity workflow = getWorkflowInstance(workflowInstanceId);
            if (workflow != null) {
                workflow.setCompletedAt(Instant.now());
                workflowRepository.save(workflow);
            }
        }

        logWorkflowEvent(workflowInstanceId, "STATE_CHANGE",
            "Workflow state changed to: " + newState, null);
    }

    @Transactional
    public void updateTaskStatus(Long taskId, TaskStatus status, String workerId) {
        TaskExecutionEntity task = getTaskExecution(taskId);
        if (task == null) return;

        TaskStatus previousStatus = task.getStatus();
        task.setStatus(status);
        task.setWorkerId(workerId);

        if (status == TaskStatus.RUNNING) {
            task.setStartedAt(Instant.now());
        } else if (status == TaskStatus.SUCCESS || status == TaskStatus.FAILED) {
            task.setCompletedAt(Instant.now());
            if (task.getStartedAt() != null) {
                task.setExecutionTimeMs(
                    task.getCompletedAt().toEpochMilli() - task.getStartedAt().toEpochMilli()
                );
            }
        }

        taskRepository.save(task);

        logWorkflowEvent(task.getWorkflow().getWorkflowInstanceId(), "TASK_STATUS_CHANGE",
            String.format("Task %s changed from %s to %s", task.getTaskName(), previousStatus, status),
            task.getTaskId());
    }

    @Transactional
    public void updateTaskResult(Long taskId, TaskResult<?> result) {
        TaskExecutionEntity task = getTaskExecution(taskId);
        if (task == null) return;

        task.setStatus(result.getStatus());

        try {
            if (result.getData() != null) {
                task.setOutputData(objectMapper.writeValueAsString(result.getData()));
            }
        } catch (Exception e) {
            logger.error("Error serializing task output data", e);
        }

        if (result.isFailed()) {
            task.setErrorMessage(result.getErrorMessage());
            if (result.getException().isPresent()) {
                Exception ex = result.getException().get();
                task.setErrorType(ex.getClass().getName());
                task.setErrorDetails(ex.toString());
            }
        }

        taskRepository.save(task);
    }

    @Transactional
    public void saveExecutionContext(String workflowId, ExecutionContext context) {
        WorkflowInstanceEntity workflow = getWorkflowInstance(workflowId);
        if (workflow == null) return;

        try {
            workflow.setContextData(objectMapper.writeValueAsString(context.getAllData()));
            workflowRepository.save(workflow);
        } catch (Exception e) {
            logger.error("Error saving execution context", e);
        }
    }

    @Transactional
    public ExecutionContext loadExecutionContext(String workflowId) {
        WorkflowInstanceEntity workflow = getWorkflowInstance(workflowId);
        if (workflow == null) return null;

        ExecutionContext context = new ExecutionContext(workflowId);

        try {
            if (workflow.getContextData() != null) {
                Map<String, Object> data = objectMapper.readValue(
                    workflow.getContextData(), Map.class
                );
                context.merge(data);
            }
        } catch (Exception e) {
            logger.error("Error loading execution context", e);
        }

        return context;
    }

    @Transactional
    public void saveWorkflowResult(String workflowId, WorkflowResult result) {
        WorkflowInstanceEntity workflow = getWorkflowInstance(workflowId);
        if (workflow == null) return;

        workflow.setState(result.getState());
        workflow.setCompletedAt(result.getEndTime());

        try {
            workflow.setOutputData(objectMapper.writeValueAsString(result.getOutputData()));
        } catch (Exception e) {
            logger.error("Error saving workflow output", e);
        }

        workflow.getErrors().addAll(result.getErrors());

        workflowRepository.save(workflow);
    }

    @Transactional
    public void requeuePendingTasks(String workflowId) {
        List<TaskExecutionEntity> tasks = taskRepository.findByWorkflowId(workflowId);
        for (TaskExecutionEntity task : tasks) {
            if (task.getStatus() == TaskStatus.PENDING || task.getStatus() == TaskStatus.RUNNING) {
                task.setWorkerId(null);
                task.setStatus(TaskStatus.PENDING);
                taskRepository.save(task);
            }
        }
    }

    @Transactional
    public void cancelPendingTasks(String workflowId) {
        taskRepository.cancelWorkflowTasks(workflowId);
    }

    @Transactional
    public void resetFailedTasks(String workflowId) {
        List<TaskExecutionEntity> tasks = taskRepository.findByWorkflowId(workflowId);
        for (TaskExecutionEntity task : tasks) {
            if (task.getStatus() == TaskStatus.FAILED) {
                task.setStatus(TaskStatus.PENDING);
                task.setWorkerId(null);
                task.setRetryCount(task.getRetryCount() + 1);
                taskRepository.save(task);
            }
        }
    }

    @Transactional
    public void retryTask(Long taskId) {
        TaskExecutionEntity task = getTaskExecution(taskId);
        if (task != null && task.getStatus() == TaskStatus.FAILED) {
            task.setStatus(TaskStatus.PENDING);
            task.setWorkerId(null);
            task.setRetryCount(task.getRetryCount() + 1);
            taskRepository.save(task);
        }
    }

    @Transactional
    public List<WorkflowInstanceEntity> getSubWorkflows(String parentWorkflowId) {
        return workflowRepository.getSubWorkflows(parentWorkflowId);
    }

    private void logWorkflowEvent(String workflowId, String eventType, String message, String taskId) {
        WorkflowInstanceEntity workflow = getWorkflowInstance(workflowId);
        if (workflow == null) return;

        WorkflowEventEntity event = new WorkflowEventEntity();
        event.setWorkflow(workflow);
        event.setEventType(eventType);
        event.setMessage(message);
        event.setTaskId(taskId);
        event.setTimestamp(Instant.now());

        workflow.addEvent(event);
        workflowRepository.save(workflow);
    }

    public Page<WorkflowInstanceEntity> findWorkflowsByState(WorkflowState state, Pageable pageable) {
        return workflowRepository.findByState(state, pageable);
    }

    public Page<WorkflowInstanceEntity> findWorkflowsByCorrelationId(String correlationId, Pageable pageable) {
        return workflowRepository.findByCorrelationId(correlationId, pageable);
    }

    public Page<WorkflowInstanceEntity> findAllWorkflows(Pageable pageable) {
        return workflowRepository.findAll(pageable);
    }

    public boolean isDatabaseHealthy() {
        try {
            workflowRepository.count();
            return true;
        } catch (Exception e) {
            logger.error("Database health check failed", e);
            return false;
        }
    }

    public WorkflowMetrics getMetrics(String timeWindow) {
        Instant since = calculateSinceTime(timeWindow);

        return new WorkflowMetrics(
            workflowRepository.countByState("RUNNING"),
            workflowRepository.countByState("COMPLETED"),
            workflowRepository.countByState("FAILED"),
            workflowRepository.countPendingWorkflows(),
            workflowRepository.countStartedSince(since),
            workflowRepository.countCompletedSince(since),
            workflowRepository.countFailedSince(since),
            workflowRepository.averageExecutionTimeSince(since)
        );
    }

    private Instant calculateSinceTime(String timeWindow) {
        Instant now = Instant.now();
        if (timeWindow.endsWith("h")) {
            int hours = Integer.parseInt(timeWindow.substring(0, timeWindow.length() - 1));
            return now.minusSeconds(hours * 3600);
        } else if (timeWindow.endsWith("d")) {
            int days = Integer.parseInt(timeWindow.substring(0, timeWindow.length() - 1));
            return now.minusSeconds(days * 86400);
        }
        return now.minusSeconds(3600); // Default 1 hour
    }

    public static class WorkflowMetrics {
        private final long runningCount;
        private final long completedCount;
        private final long failedCount;
        private final long pendingCount;
        private final long startedInWindow;
        private final long completedInWindow;
        private final long failedInWindow;
        private final Double avgExecutionTime;

        public WorkflowMetrics(long runningCount, long completedCount, long failedCount,
                              long pendingCount, long startedInWindow, long completedInWindow,
                              long failedInWindow, Double avgExecutionTime) {
            this.runningCount = runningCount;
            this.completedCount = completedCount;
            this.failedCount = failedCount;
            this.pendingCount = pendingCount;
            this.startedInWindow = startedInWindow;
            this.completedInWindow = completedInWindow;
            this.failedInWindow = failedInWindow;
            this.avgExecutionTime = avgExecutionTime;
        }

        public long getRunningCount() { return runningCount; }
        public long getCompletedCount() { return completedCount; }
        public long getFailedCount() { return failedCount; }
        public long getPendingCount() { return pendingCount; }
        public long getStartedInWindow() { return startedInWindow; }
        public long getCompletedInWindow() { return completedInWindow; }
        public long getFailedInWindow() { return failedInWindow; }
        public Double getAvgExecutionTime() { return avgExecutionTime; }
    }
}
