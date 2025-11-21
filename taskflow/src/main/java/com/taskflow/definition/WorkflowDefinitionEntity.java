package com.taskflow.definition;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * JPA entity for persisting workflow definitions with versioning support.
 * Inspired by Netflix Conductor's workflow definition management.
 */
@Entity
@Table(name = "workflow_definitions",
       uniqueConstraints = @UniqueConstraint(columnNames = {"name", "version"}),
       indexes = {
           @Index(name = "idx_workflow_name", columnList = "name"),
           @Index(name = "idx_workflow_status", columnList = "validationStatus"),
           @Index(name = "idx_workflow_updated", columnList = "updatedAt")
       })
public class WorkflowDefinitionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Column(name = "version", nullable = false)
    private Integer version;

    @Column(name = "description", length = 1000)
    private String description;

    @Lob
    @Column(name = "definition_json", nullable = false)
    private String definitionJson;

    @Column(name = "validation_status", length = 50)
    private String validationStatus = "VALID";

    @Column(name = "validation_message", length = 2000)
    private String validationMessage;

    @Column(name = "owner_email", length = 255)
    private String ownerEmail;

    @Column(name = "created_by", length = 100)
    private String createdBy;

    @Column(name = "updated_by", length = 100)
    private String updatedBy;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "is_active")
    private Boolean isActive = true;

    @Column(name = "timeout_seconds")
    private Long timeoutSeconds;

    @Column(name = "timeout_policy", length = 50)
    private String timeoutPolicy = "ALERT_ONLY";

    @Column(name = "restartable")
    private Boolean restartable = true;

    @Column(name = "failure_workflow", length = 255)
    private String failureWorkflow;

    @Column(name = "schema_version")
    private Integer schemaVersion = 2;

    @Lob
    @Column(name = "input_parameters")
    private String inputParameters;

    @Lob
    @Column(name = "output_parameters")
    private String outputParameters;

    @Lob
    @Column(name = "variables")
    private String variables;

    @Version
    @Column(name = "entity_version")
    private Long entityVersion = 0L;

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDefinitionJson() {
        return definitionJson;
    }

    public void setDefinitionJson(String definitionJson) {
        this.definitionJson = definitionJson;
    }

    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }

    public String getValidationMessage() {
        return validationMessage;
    }

    public void setValidationMessage(String validationMessage) {
        this.validationMessage = validationMessage;
    }

    public String getOwnerEmail() {
        return ownerEmail;
    }

    public void setOwnerEmail(String ownerEmail) {
        this.ownerEmail = ownerEmail;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    public Long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(Long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getTimeoutPolicy() {
        return timeoutPolicy;
    }

    public void setTimeoutPolicy(String timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
    }

    public Boolean getRestartable() {
        return restartable;
    }

    public void setRestartable(Boolean restartable) {
        this.restartable = restartable;
    }

    public String getFailureWorkflow() {
        return failureWorkflow;
    }

    public void setFailureWorkflow(String failureWorkflow) {
        this.failureWorkflow = failureWorkflow;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(Integer schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getInputParameters() {
        return inputParameters;
    }

    public void setInputParameters(String inputParameters) {
        this.inputParameters = inputParameters;
    }

    public String getOutputParameters() {
        return outputParameters;
    }

    public void setOutputParameters(String outputParameters) {
        this.outputParameters = outputParameters;
    }

    public String getVariables() {
        return variables;
    }

    public void setVariables(String variables) {
        this.variables = variables;
    }

    public Long getEntityVersion() {
        return entityVersion;
    }

    public void setEntityVersion(Long entityVersion) {
        this.entityVersion = entityVersion;
    }

    @Override
    public String toString() {
        return String.format("WorkflowDefinition{name='%s', version=%d, status='%s'}",
            name, version, validationStatus);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowDefinitionEntity)) return false;

        WorkflowDefinitionEntity that = (WorkflowDefinitionEntity) o;

        if (!name.equals(that.name)) return false;
        return version.equals(that.version);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + version.hashCode();
        return result;
    }
}
