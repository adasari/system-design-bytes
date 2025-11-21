package com.taskflow.persistence.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Objects;

/**
 * Composite primary key for WorkflowDefinitionEntity.
 * 
 * Combines workflow_id and version to uniquely identify each workflow definition version.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowDefinitionId implements Serializable {
    
    private String workflowId;
    private Integer version;
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkflowDefinitionId that = (WorkflowDefinitionId) o;
        return Objects.equals(workflowId, that.workflowId) && 
               Objects.equals(version, that.version);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(workflowId, version);
    }
    
    @Override
    public String toString() {
        return workflowId + ":" + version;
    }
}