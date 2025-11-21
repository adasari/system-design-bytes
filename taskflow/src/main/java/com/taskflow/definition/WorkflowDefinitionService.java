package com.taskflow.definition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskflow.core.WorkflowDefinition;
import com.taskflow.persistence.entity.WorkflowDefinitionEntity;
import com.taskflow.persistence.entity.WorkflowDefinitionEntity.DefinitionStatus;
import com.taskflow.persistence.entity.WorkflowDefinitionId;
import com.taskflow.persistence.repository.WorkflowDefinitionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing workflow definitions with versioning support.
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Database persistence with versioning</li>
 *   <li>Automatic version management</li>
 *   <li>Definition caching for performance</li>
 *   <li>Status management (Active/Deprecated/Archived)</li>
 *   <li>Latest version tracking</li>
 * </ul>
 *
 * <p><b>Workflow ID Management:</b></p>
 * <ul>
 *   <li>Each workflow name gets a unique workflow_id (UUID)</li>
 *   <li>All versions share the same workflow_id</li>
 *   <li>Version numbers auto-increment per workflow_id</li>
 * </ul>
 */
@Service
@Transactional
public class WorkflowDefinitionService {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowDefinitionService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, WorkflowDefinition> definitionCache = new ConcurrentHashMap<>();

    @Autowired
    private WorkflowDefinitionRepository definitionRepository;

    /**
     * Register a new workflow or create a new version of existing workflow
     *
     * @param definition The workflow definition
     * @param createdBy User who created the definition
     * @return The persisted entity
     */
    public WorkflowDefinitionEntity registerWorkflow(WorkflowDefinition definition, String createdBy) {
        logger.info("Registering workflow: {} version: {}", definition.getName(), definition.getVersion());

        try {
            // Validate workflow definition
            validateWorkflowDefinition(definition);

            // Check if workflow name already exists
            Optional<WorkflowDefinitionEntity> latestExisting =
                definitionRepository.findLatestByWorkflowName(definition.getName());

            String workflowId;
            Integer version;

            if (latestExisting.isPresent()) {
                // Existing workflow - create new version
                workflowId = latestExisting.get().getWorkflowId();

                if (definition.getVersion() > 0) {
                    // Use specified version if provided
                    version = definition.getVersion();

                    // Check if this version already exists
                    if (definitionRepository.findByWorkflowIdAndVersion(workflowId, version).isPresent()) {
                        throw new IllegalArgumentException(
                            String.format("Workflow %s version %d already exists", definition.getName(), version));
                    }
                } else {
                    // Auto-increment version
                    version = definitionRepository.getNextVersionNumber(workflowId);
                }

            } else {
                // New workflow - create new workflow_id
                workflowId = UUID.randomUUID().toString();
                version = definition.getVersion() > 0 ? definition.getVersion() : 1;
            }

            // Serialize definition to JSON
            String definitionJson = objectMapper.writeValueAsString(definition);

            // Create entity
            WorkflowDefinitionEntity entity = WorkflowDefinitionEntity.builder()
                .workflowId(workflowId)
                .version(version)
                .workflowName(definition.getName())
                .description(definition.getDescription())
                .definitionJson(definitionJson)
                .status(DefinitionStatus.ACTIVE)
                .createdBy(createdBy)
                .timeoutSeconds(definition.getTimeout().toMillis())
                .build();

            // Clear latest flags for this workflow and set new one as latest
            definitionRepository.clearLatestFlags(workflowId, Instant.now());
            entity.setIsLatest(true);

            // Save entity
            entity = definitionRepository.save(entity);

            // Update cache
            String cacheKey = getCacheKey(workflowId, version);
            definitionCache.put(cacheKey, definition);

            logger.info("Successfully registered workflow: {} [{}:{}]",
                definition.getName(), workflowId, version);

            return entity;

        } catch (Exception e) {
            logger.error("Error registering workflow: {}", definition.getName(), e);
            throw new RuntimeException("Failed to register workflow: " + e.getMessage(), e);
        }
    }

    /**
     * Register workflow without specifying creator (defaults to "system")
     */
    public WorkflowDefinitionEntity registerWorkflow(WorkflowDefinition definition) {
        return registerWorkflow(definition, "system");
    }

    /**
     * Get workflow definition by name and version
     */
    public Optional<WorkflowDefinition> getWorkflowDefinition(String workflowName, Integer version) {
        Optional<WorkflowDefinitionEntity> entityOpt =
            definitionRepository.findByWorkflowNameAndVersion(workflowName, version);

        return entityOpt.map(this::convertToWorkflowDefinition);
    }

    /**
     * Get workflow definition by workflow ID and version
     */
    public Optional<WorkflowDefinition> getWorkflowDefinitionById(String workflowId, Integer version) {
        String cacheKey = getCacheKey(workflowId, version);
        WorkflowDefinition cached = definitionCache.get(cacheKey);
        if (cached != null) {
            return Optional.of(cached);
        }

        Optional<WorkflowDefinitionEntity> entityOpt =
            definitionRepository.findByWorkflowIdAndVersion(workflowId, version);

        return entityOpt.map(entity -> {
            WorkflowDefinition definition = convertToWorkflowDefinition(entity);
            definitionCache.put(cacheKey, definition);
            return definition;
        });
    }

    /**
     * Get latest version of workflow by name
     */
    public Optional<WorkflowDefinition> getLatestWorkflowDefinition(String workflowName) {
        Optional<WorkflowDefinitionEntity> entityOpt =
            definitionRepository.findLatestByWorkflowName(workflowName);

        return entityOpt.map(this::convertToWorkflowDefinition);
    }

    /**
     * Get latest version of workflow by workflow ID
     */
    public Optional<WorkflowDefinition> getLatestWorkflowDefinitionById(String workflowId) {
        String cacheKey = getCacheKey(workflowId, "latest");
        WorkflowDefinition cached = definitionCache.get(cacheKey);
        if (cached != null) {
            return Optional.of(cached);
        }

        Optional<WorkflowDefinitionEntity> entityOpt =
            definitionRepository.findLatestByWorkflowId(workflowId);

        return entityOpt.map(entity -> {
            WorkflowDefinition definition = convertToWorkflowDefinition(entity);
            definitionCache.put(getCacheKey(workflowId, entity.getVersion()), definition);
            definitionCache.put(cacheKey, definition);
            return definition;
        });
    }

    /**
     * Get all versions of a workflow
     */
    public List<WorkflowDefinitionEntity> getAllVersions(String workflowName) {
        return definitionRepository.findAllVersionsByWorkflowName(workflowName);
    }

    /**
     * Get all active workflows (latest versions only)
     */
    public List<WorkflowDefinitionEntity> getAllActiveWorkflows() {
        return definitionRepository.findAllActiveLatestVersions();
    }

    /**
     * Deprecate a specific version
     */
    public void deprecateVersion(String workflowName, Integer version, String deprecatedBy, String reason) {
        Optional<WorkflowDefinitionEntity> entityOpt =
            definitionRepository.findByWorkflowNameAndVersion(workflowName, version);

        if (entityOpt.isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Workflow %s version %d not found", workflowName, version));
        }

        WorkflowDefinitionEntity entity = entityOpt.get();
        definitionRepository.deprecateVersion(entity.getWorkflowId(), version, deprecatedBy, reason, Instant.now());

        // Remove from cache
        definitionCache.remove(getCacheKey(entity.getWorkflowId(), version));
        if (entity.getIsLatest()) {
            definitionCache.remove(getCacheKey(entity.getWorkflowId(), "latest"));
        }

        logger.info("Deprecated workflow {} version {} by {}: {}",
            workflowName, version, deprecatedBy, reason);
    }

    /**
     * Set a specific version as latest
     */
    public void setLatestVersion(String workflowName, Integer version, String updatedBy) {
        Optional<WorkflowDefinitionEntity> entityOpt =
            definitionRepository.findByWorkflowNameAndVersion(workflowName, version);

        if (entityOpt.isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Workflow %s version %d not found", workflowName, version));
        }

        WorkflowDefinitionEntity entity = entityOpt.get();

        if (!entity.isAvailableForNewWorkflows()) {
            throw new IllegalArgumentException(
                String.format("Cannot set deprecated/archived version %d as latest for %s", version, workflowName));
        }

        // Clear all latest flags for this workflow
        definitionRepository.clearLatestFlags(entity.getWorkflowId(), Instant.now());

        // Set this version as latest
        definitionRepository.setAsLatest(entity.getWorkflowId(), version, Instant.now());

        // Update cache
        definitionCache.remove(getCacheKey(entity.getWorkflowId(), "latest"));

        logger.info("Set workflow {} version {} as latest by {}", workflowName, version, updatedBy);
    }

    /**
     * Check if workflow exists
     */
    public boolean workflowExists(String workflowName) {
        return definitionRepository.existsByWorkflowName(workflowName);
    }

    /**
     * Get workflow ID by name (from latest version)
     */
    public Optional<String> getWorkflowId(String workflowName) {
        return definitionRepository.findLatestByWorkflowName(workflowName)
            .map(WorkflowDefinitionEntity::getWorkflowId);
    }

    /**
     * Convert entity to WorkflowDefinition
     */
    public WorkflowDefinition convertToWorkflowDefinition(WorkflowDefinitionEntity entity) {
        try {
            WorkflowDefinition definition = objectMapper.readValue(
                entity.getDefinitionJson(), WorkflowDefinition.class);

            return definition;

        } catch (Exception e) {
            logger.error("Error deserializing workflow definition for {}:{}",
                entity.getWorkflowId(), entity.getVersion(), e);
            throw new RuntimeException("Failed to deserialize workflow definition", e);
        }
    }

    /**
     * Validate workflow definition
     */
    private void validateWorkflowDefinition(WorkflowDefinition definition) {
        if (definition.getName() == null || definition.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Workflow name is required");
        }

        if (definition.getTasks() == null || definition.getTasks().isEmpty()) {
            throw new IllegalArgumentException("Workflow must have at least one task");
        }

        // Additional validation can be added here
        // - Task dependencies validation
        // - Circular dependency detection
        // - Required fields validation
    }

    /**
     * Generate cache key
     */
    private String getCacheKey(String workflowId, Integer version) {
        return workflowId + ":" + version;
    }

    private String getCacheKey(String workflowId, String version) {
        return workflowId + ":" + version;
    }
}
