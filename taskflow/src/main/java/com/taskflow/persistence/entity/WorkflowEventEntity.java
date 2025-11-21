package com.taskflow.persistence.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "workflow_events",
       indexes = {
           @Index(name = "idx_event_workflow", columnList = "workflow_id"),
           @Index(name = "idx_event_timestamp", columnList = "timestamp"),
           @Index(name = "idx_event_type", columnList = "event_type")
       })
public class WorkflowEventEntity {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workflow_id", nullable = false)
    private WorkflowInstanceEntity workflow;
    
    @Column(name = "event_type", nullable = false)
    private String eventType;
    
    @Column(name = "event_name")
    private String eventName;
    
    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;
    
    @Column(name = "task_id")
    private String taskId;
    
    @Column(name = "task_name")
    private String taskName;
    
    @Lob
    @Column(name = "event_data")
    private String eventData;
    
    @Column(name = "message", length = 2000)
    private String message;
    
    @Column(name = "severity")
    private String severity;
    
    @PrePersist
    protected void onCreate() {
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public WorkflowInstanceEntity getWorkflow() {
        return workflow;
    }
    
    public void setWorkflow(WorkflowInstanceEntity workflow) {
        this.workflow = workflow;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    
    public String getEventName() {
        return eventName;
    }
    
    public void setEventName(String eventName) {
        this.eventName = eventName;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
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
    
    public String getEventData() {
        return eventData;
    }
    
    public void setEventData(String eventData) {
        this.eventData = eventData;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public String getSeverity() {
        return severity;
    }
    
    public void setSeverity(String severity) {
        this.severity = severity;
    }
}