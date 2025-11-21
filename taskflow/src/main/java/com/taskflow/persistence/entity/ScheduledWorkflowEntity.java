package com.taskflow.persistence.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Entity for persisting scheduled workflow configurations.
 * 
 * Supports various scheduling strategies:
 * - One-time execution at specific time
 * - Fixed rate/delay recurring execution
 * - Cron-based scheduling
 */
@Entity
@Table(name = "scheduled_workflows",
    indexes = {
        @Index(name = "idx_scheduled_workflow_status", columnList = "status"),
        @Index(name = "idx_scheduled_workflow_next_execution", columnList = "next_execution_time"),
        @Index(name = "idx_scheduled_workflow_name", columnList = "workflow_name")
    })
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScheduledWorkflowEntity {
    
    @Id
    @Column(name = "schedule_id", length = 100)
    private String scheduleId;
    
    @Column(name = "workflow_name", nullable = false, length = 255)
    private String workflowName;
    
    @Column(name = "workflow_version")
    private Integer workflowVersion;
    
    @Column(name = "description", length = 1000)
    private String description;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false, length = 20)
    private ScheduleType scheduleType;
    
    @Column(name = "cron_expression", length = 100)
    private String cronExpression;
    
    @Column(name = "fixed_delay_seconds")
    private Long fixedDelaySeconds;
    
    @Column(name = "fixed_rate_seconds")
    private Long fixedRateSeconds;
    
    @Column(name = "start_time")
    private Instant startTime;
    
    @Column(name = "end_time")
    private Instant endTime;
    
    @Column(name = "next_execution_time")
    private Instant nextExecutionTime;
    
    @Column(name = "last_execution_time")
    private Instant lastExecutionTime;
    
    @Column(name = "execution_count")
    private Integer executionCount = 0;
    
    @Column(name = "max_executions")
    private Integer maxExecutions;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private ScheduleStatus status = ScheduleStatus.ACTIVE;
    
    @Column(name = "input_data", columnDefinition = "TEXT")
    @Convert(converter = JsonMapConverter.class)
    private Map<String, Object> inputData = new HashMap<>();
    
    @Column(name = "timezone", length = 50)
    private String timezone = "UTC";
    
    @Column(name = "retry_on_failure")
    private Boolean retryOnFailure = true;
    
    @Column(name = "alert_on_failure")
    private Boolean alertOnFailure = true;
    
    @Column(name = "alert_email", length = 255)
    private String alertEmail;
    
    @Column(name = "created_by", length = 100)
    private String createdBy;
    
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    
    @Column(name = "updated_at")
    private Instant updatedAt;
    
    @Column(name = "last_workflow_instance_id", length = 100)
    private String lastWorkflowInstanceId;
    
    @Column(name = "last_workflow_status", length = 20)
    private String lastWorkflowStatus;
    
    @Column(name = "consecutive_failures")
    private Integer consecutiveFailures = 0;
    
    @Column(name = "max_consecutive_failures")
    private Integer maxConsecutiveFailures = 5;
    
    @Column(name = "metadata", columnDefinition = "TEXT")
    @Convert(converter = JsonMapConverter.class)
    private Map<String, Object> metadata = new HashMap<>();
    
    @Version
    private Long version;
    
    public enum ScheduleType {
        ONE_TIME,      // Execute once at specific time
        FIXED_RATE,    // Execute at fixed intervals (start to start)
        FIXED_DELAY,   // Execute with fixed delay (end to start)
        CRON           // Cron expression based
    }
    
    public enum ScheduleStatus {
        ACTIVE,        // Schedule is active and will execute
        PAUSED,        // Temporarily paused
        COMPLETED,     // One-time schedule completed or max executions reached
        DISABLED,      // Manually disabled
        ERROR,         // Too many consecutive failures
        EXPIRED        // End time reached
    }
    
    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (executionCount == null) {
            executionCount = 0;
        }
        if (consecutiveFailures == null) {
            consecutiveFailures = 0;
        }
    }
    
    @PreUpdate
    public void preUpdate() {
        updatedAt = Instant.now();
    }
    
    /**
     * Check if schedule should be executed
     */
    public boolean shouldExecute() {
        if (status != ScheduleStatus.ACTIVE) {
            return false;
        }
        
        Instant now = Instant.now();
        
        // Check start time
        if (startTime != null && now.isBefore(startTime)) {
            return false;
        }
        
        // Check end time
        if (endTime != null && now.isAfter(endTime)) {
            status = ScheduleStatus.EXPIRED;
            return false;
        }
        
        // Check max executions
        if (maxExecutions != null && executionCount >= maxExecutions) {
            status = ScheduleStatus.COMPLETED;
            return false;
        }
        
        // Check consecutive failures
        if (maxConsecutiveFailures != null && consecutiveFailures >= maxConsecutiveFailures) {
            status = ScheduleStatus.ERROR;
            return false;
        }
        
        // Check next execution time
        return nextExecutionTime != null && !now.isBefore(nextExecutionTime);
    }
    
    /**
     * Update after successful execution
     */
    public void recordSuccessfulExecution() {
        lastExecutionTime = Instant.now();
        executionCount++;
        consecutiveFailures = 0;
        
        // Update status if one-time or max executions reached
        if (scheduleType == ScheduleType.ONE_TIME || 
            (maxExecutions != null && executionCount >= maxExecutions)) {
            status = ScheduleStatus.COMPLETED;
        }
    }
    
    /**
     * Update after failed execution
     */
    public void recordFailedExecution() {
        lastExecutionTime = Instant.now();
        executionCount++;
        consecutiveFailures++;
        
        // Check if should disable due to failures
        if (maxConsecutiveFailures != null && consecutiveFailures >= maxConsecutiveFailures) {
            status = ScheduleStatus.ERROR;
        }
    }
}

/**
 * JPA converter for JSON map storage
 */
@Converter
class JsonMapConverter implements AttributeConverter<Map<String, Object>, String> {
    private static final com.fasterxml.jackson.databind.ObjectMapper objectMapper = 
        new com.fasterxml.jackson.databind.ObjectMapper();
    
    @Override
    public String convertToDatabaseColumn(Map<String, Object> attribute) {
        try {
            return attribute == null ? null : objectMapper.writeValueAsString(attribute);
        } catch (Exception e) {
            throw new RuntimeException("Error converting map to JSON", e);
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> convertToEntityAttribute(String dbData) {
        try {
            return dbData == null ? new HashMap<>() : 
                objectMapper.readValue(dbData, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("Error converting JSON to map", e);
        }
    }
}