package com.taskflow.persistence.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity for persisting workflow definitions with versioning support.
 * 
 * <p>This table maintains the complete history of workflow definitions:</p>
 * <ul>
 *   <li>Each workflow has a unique workflow_id</li>
 *   <li>Multiple versions can exist for the same workflow_id</li>
 *   <li>Supports definition evolution and rollback</li>
 *   <li>Stores complete workflow definition as JSON</li>
 * </ul>
 * 
 * <p><b>Primary Key:</b> workflow_id + version</p>
 * <p><b>Versioning:</b> Auto-incrementing version numbers per workflow_id</p>
 * <p><b>Status:</b> ACTIVE, DEPRECATED, ARCHIVED</p>
 */
@Entity
@Table(name = "workflow_definitions",
    indexes = {
        @Index(name = "idx_workflow_def_name", columnList = "workflow_name"),
        @Index(name = "idx_workflow_def_id_version", columnList = "workflow_id, version"),
        @Index(name = "idx_workflow_def_status", columnList = "status"),
        @Index(name = "idx_workflow_def_latest", columnList = "workflow_id, is_latest")
    },
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_workflow_id_version", columnNames = {"workflow_id", "version"}),
        @UniqueConstraint(name = "uk_workflow_latest", columnNames = {"workflow_id", "is_latest"})
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@IdClass(WorkflowDefinitionId.class)
public class WorkflowDefinitionEntity {
    
    @Id
    @Column(name = "workflow_id", length = 100)
    private String workflowId;
    
    @Id
    @Column(name = "version")
    private Integer version;
    
    @Column(name = "workflow_name", nullable = false, length = 255)
    private String workflowName;
    
    @Column(name = "description", length = 1000)
    private String description;
    
    @Lob
    @Column(name = "definition_json", nullable = false)
    private String definitionJson;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private DefinitionStatus status = DefinitionStatus.ACTIVE;
    
    @Column(name = "is_latest", nullable = false)
    private Boolean isLatest = false;
    
    @Column(name = "created_by", length = 100)
    private String createdBy;
    
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    
    @Column(name = "updated_by", length = 100)  
    private String updatedBy;
    
    @Column(name = "updated_at")
    private Instant updatedAt;
    
    @Column(name = "deprecated_at")
    private Instant deprecatedAt;
    
    @Column(name = "deprecated_by", length = 100)
    private String deprecatedBy;
    
    @Column(name = "deprecation_reason", length = 500)
    private String deprecationReason;
    
    @Column(name = "tags", length = 1000)
    private String tags;  // Comma-separated tags for categorization
    
    @Column(name = "timeout_seconds")
    private Long timeoutSeconds;
    
    @Column(name = "retry_policy", length = 500)
    private String retryPolicy;  // JSON for retry configuration
    
    @Version
    private Long entityVersion;
    
    /**
     * References to workflow instances using this definition version
     */
    @OneToMany(mappedBy = "workflowDefinition", cascade = CascadeType.DETACH)
    private List<WorkflowInstanceEntity> workflowInstances = new ArrayList<>();
    
    public enum DefinitionStatus {
        ACTIVE,      // Available for new workflow instances
        DEPRECATED,  // Still running existing instances but not for new ones
        ARCHIVED     // Historical record only
    }
    
    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (isLatest == null) {
            isLatest = false;
        }
    }
    
    @PreUpdate
    public void preUpdate() {
        updatedAt = Instant.now();
    }
    
    /**
     * Mark this version as deprecated
     */
    public void deprecate(String deprecatedBy, String reason) {
        this.status = DefinitionStatus.DEPRECATED;
        this.deprecatedAt = Instant.now();
        this.deprecatedBy = deprecatedBy;
        this.deprecationReason = reason;
        this.isLatest = false;
    }
    
    /**
     * Check if this version is available for new workflows
     */
    public boolean isAvailableForNewWorkflows() {
        return status == DefinitionStatus.ACTIVE;
    }
    
    /**
     * Get full workflow identifier
     */
    public String getFullIdentifier() {
        return workflowId + ":" + version;
    }
}