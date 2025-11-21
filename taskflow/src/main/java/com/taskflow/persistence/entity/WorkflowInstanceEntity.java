package com.taskflow.persistence.entity;

import com.taskflow.core.WorkflowState;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Entity
@Table(name = "workflow_instances")
public class WorkflowInstanceEntity {

    @Id
    @Column(name = "workflow_instance_id", length = 100)
    private String workflowInstanceId;

    @Column(name = "workflow_name", nullable = false)
    private String workflowName; // Denormalized for easier queries

    @Column(name = "workflow_id", nullable = false)
    private String workflowId;

    @Column(name = "workflow_version", nullable = false)
    private Integer workflowVersion; // References WorkflowDefinitionEntity.version

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false)
    private WorkflowState state;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "parent_workflow_id")
    private String parentWorkflowId;

    @Column(name = "processor_id")
    private String processorId;  // ID of the processor currently executing this workflow

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "last_updated")
    private Instant lastUpdated;

    @Column(name = "created_by")
    private String createdBy;

    @Column(name = "trigger_type", length = 50)
    @Enumerated(EnumType.STRING)
    private TriggerType triggerType = TriggerType.MANUAL;

    @Column(name = "schedule_id", length = 100)
    private String scheduleId;  // Reference to ScheduledWorkflowEntity if triggered by scheduler

    @Column(name = "priority")
    private Integer priority = 5;  // Higher number = higher priority

    @Column(name = "retry_count")
    private Integer retryCount = 0;

    @Column(name = "max_retries")
    private Integer maxRetries = 3;

    @Lob
    @Column(name = "input_data")
    private String inputData;

    @Lob
    @Column(name = "output_data")
    private String outputData;

    @Lob
    @Column(name = "context_data")
    private String contextData;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "workflow_metadata",
                     joinColumns = @JoinColumn(name = "workflow_instance_id"))
    @MapKeyColumn(name = "meta_key")
    @Column(name = "meta_value")
    private Map<String, String> metadata = new HashMap<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "workflow_errors",
                     joinColumns = @JoinColumn(name = "workflow_instance_id"))
    @Column(name = "error_message")
    @OrderColumn(name = "error_index")
    private List<String> errors = new ArrayList<>();

    @OneToMany(mappedBy = "workflow", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @OrderBy("sequenceNumber ASC")
    private List<TaskExecutionEntity> taskExecutions = new ArrayList<>();

    @OneToMany(mappedBy = "workflow", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @OrderBy("timestamp ASC")
    private List<WorkflowEventEntity> events = new ArrayList<>();

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumns({
        @JoinColumn(name = "workflow_id", referencedColumnName = "workflow_id", insertable = false, updatable = false),
        @JoinColumn(name = "workflow_version", referencedColumnName = "version", insertable = false, updatable = false)
    })
    private WorkflowDefinitionEntity workflowDefinition;

    @Version
    @Column(name = "version")
    private Long version;  // For optimistic locking

    @PrePersist
    protected void onCreate() {
        lastUpdated = Instant.now();
        if (state == null) {
            state = WorkflowState.NOT_STARTED;
        }
        if (priority == null) {
            priority = 5;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        lastUpdated = Instant.now();
    }

    public void addTaskExecution(TaskExecutionEntity taskExecution) {
        taskExecutions.add(taskExecution);
        taskExecution.setWorkflow(this);
    }

    public void addEvent(WorkflowEventEntity event) {
        events.add(event);
        event.setWorkflow(this);
    }

    public void addError(String error) {
        errors.add(error);
    }


    /**
     * Enum to track how a workflow was triggered
     */
    public enum TriggerType {
        MANUAL,      // Triggered directly via API or code
        SCHEDULED,   // Triggered by scheduler
        SQS_EVENT,   // Triggered by SQS event
        SUB_WORKFLOW // Triggered as a sub-workflow
    }
}
