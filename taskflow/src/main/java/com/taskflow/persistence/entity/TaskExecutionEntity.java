package com.taskflow.persistence.entity;

import com.taskflow.core.TaskStatus;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Entity
@Table(name = "task_executions", 
       indexes = {
           @Index(name = "idx_task_workflow", columnList = "workflow_id"),
           @Index(name = "idx_task_status", columnList = "status"),
           @Index(name = "idx_task_started", columnList = "started_at")
       })
@NamedQueries({
    @NamedQuery(name = "TaskExecution.findByWorkflow", 
                query = "SELECT t FROM TaskExecutionEntity t WHERE t.workflow.workflowId = :workflowId ORDER BY t.startedAt"),
    @NamedQuery(name = "TaskExecution.findByStatus", 
                query = "SELECT t FROM TaskExecutionEntity t WHERE t.status = :status"),
    @NamedQuery(name = "TaskExecution.findPending", 
                query = "SELECT t FROM TaskExecutionEntity t WHERE t.status = 'PENDING' AND t.scheduledAt <= :now")
})
public class TaskExecutionEntity {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;
    
    @Column(name = "task_id", nullable = false, length = 100)
    private String taskId;
    
    @Column(name = "task_name", nullable = false)
    private String taskName;
    
    @Column(name = "task_reference_name")
    private String taskReferenceName;
    
    @Column(name = "task_type")
    private String taskType;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private TaskStatus status;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workflow_id", nullable = false)
    private WorkflowInstanceEntity workflow;
    
    @Column(name = "sequence_number")
    private Integer sequenceNumber;
    
    @Column(name = "retry_count")
    private Integer retryCount = 0;
    
    @Column(name = "max_retries")
    private Integer maxRetries = 3;
    
    @Column(name = "scheduled_at")
    private Instant scheduledAt;
    
    @Column(name = "started_at")
    private Instant startedAt;
    
    @Column(name = "completed_at")
    private Instant completedAt;
    
    @Column(name = "execution_time_ms")
    private Long executionTimeMs;
    
    @Column(name = "timeout_ms")
    private Long timeoutMs;
    
    @Lob
    @Column(name = "input_data")
    private String inputData;
    
    @Lob
    @Column(name = "output_data")
    private String outputData;
    
    @Column(name = "error_message", length = 2000)
    private String errorMessage;
    
    @Column(name = "error_type")
    private String errorType;
    
    @Lob
    @Column(name = "error_details")
    private String errorDetails;
    
    @Column(name = "worker_id")
    private String workerId;
    
    @Column(name = "poll_count")
    private Integer pollCount = 0;
    
    @Column(name = "callback_after")
    private Instant callbackAfter;
    
    @Column(name = "parent_task_id")
    private String parentTaskId;
    
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "task_dependencies", 
                     joinColumns = @JoinColumn(name = "task_execution_id"))
    @Column(name = "dependency_task_id")
    private Map<String, String> dependencies = new HashMap<>();
    
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "task_metadata", 
                     joinColumns = @JoinColumn(name = "task_execution_id"))
    @MapKeyColumn(name = "meta_key")
    @Column(name = "meta_value")
    private Map<String, String> metadata = new HashMap<>();
    
    @PrePersist
    protected void onCreate() {
        if (status == null) {
            status = TaskStatus.PENDING;
        }
        if (scheduledAt == null) {
            scheduledAt = Instant.now();
        }
    }
    
    @PreUpdate
    protected void onUpdate() {
        if (status == TaskStatus.SUCCESS || status == TaskStatus.FAILED || 
            status == TaskStatus.CANCELLED || status == TaskStatus.SKIPPED) {
            if (completedAt == null) {
                completedAt = Instant.now();
            }
            if (startedAt != null) {
                executionTimeMs = completedAt.toEpochMilli() - startedAt.toEpochMilli();
            }
        }
    }
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public String getTaskName() {
        return taskName;
    }
    
    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
    
    public String getTaskReferenceName() {
        return taskReferenceName;
    }
    
    public void setTaskReferenceName(String taskReferenceName) {
        this.taskReferenceName = taskReferenceName;
    }
    
    public String getTaskType() {
        return taskType;
    }
    
    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }
    
    public TaskStatus getStatus() {
        return status;
    }
    
    public void setStatus(TaskStatus status) {
        this.status = status;
    }
    
    public WorkflowInstanceEntity getWorkflow() {
        return workflow;
    }
    
    public void setWorkflow(WorkflowInstanceEntity workflow) {
        this.workflow = workflow;
    }
    
    public Integer getSequenceNumber() {
        return sequenceNumber;
    }
    
    public void setSequenceNumber(Integer sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    
    public Integer getRetryCount() {
        return retryCount;
    }
    
    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }
    
    public Integer getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public Instant getScheduledAt() {
        return scheduledAt;
    }
    
    public void setScheduledAt(Instant scheduledAt) {
        this.scheduledAt = scheduledAt;
    }
    
    public Instant getStartedAt() {
        return startedAt;
    }
    
    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }
    
    public Instant getCompletedAt() {
        return completedAt;
    }
    
    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }
    
    public Long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    public void setExecutionTimeMs(Long executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
    }
    
    public Long getTimeoutMs() {
        return timeoutMs;
    }
    
    public void setTimeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
    
    public String getInputData() {
        return inputData;
    }
    
    public void setInputData(String inputData) {
        this.inputData = inputData;
    }
    
    public String getOutputData() {
        return outputData;
    }
    
    public void setOutputData(String outputData) {
        this.outputData = outputData;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public String getErrorType() {
        return errorType;
    }
    
    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }
    
    public String getErrorDetails() {
        return errorDetails;
    }
    
    public void setErrorDetails(String errorDetails) {
        this.errorDetails = errorDetails;
    }
    
    public String getWorkerId() {
        return workerId;
    }
    
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }
    
    public Integer getPollCount() {
        return pollCount;
    }
    
    public void setPollCount(Integer pollCount) {
        this.pollCount = pollCount;
    }
    
    public Instant getCallbackAfter() {
        return callbackAfter;
    }
    
    public void setCallbackAfter(Instant callbackAfter) {
        this.callbackAfter = callbackAfter;
    }
    
    public String getParentTaskId() {
        return parentTaskId;
    }
    
    public void setParentTaskId(String parentTaskId) {
        this.parentTaskId = parentTaskId;
    }
    
    public Map<String, String> getDependencies() {
        return dependencies;
    }
    
    public void setDependencies(Map<String, String> dependencies) {
        this.dependencies = dependencies;
    }
    
    public Map<String, String> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}