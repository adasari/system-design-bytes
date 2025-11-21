package com.taskflow.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

/**
 * Configuration for SQS-based workflow triggers.
 * Maps SQS queues to workflow definitions for event-driven execution.
 */
@Configuration
@ConfigurationProperties(prefix = "workflow.sqs")
public class SqsWorkflowConfig {
    
    /**
     * General workflow trigger queue
     */
    private String triggerQueue = "workflow-trigger-queue";
    
    /**
     * Mapping of SQS queues to workflow names
     */
    private Map<String, String> queueToWorkflow = Map.of(
        "data-processing-workflow", "data-processing",
        "order-processing-workflow", "order-processing", 
        "notification-workflow", "notification",
        "file-processing-workflow", "file-processing"
    );
    
    /**
     * SQS listener configuration
     */
    private int maxConcurrentMessages = 10;
    private int maxMessageBatchSize = 10;
    private long messageVisibilityTimeout = 30; // seconds
    private long messageWaitTime = 20; // seconds
    private boolean autoDeleteMessages = true;
    
    /**
     * Dead letter queue configuration
     */
    private String deadLetterQueue = "workflow-dlq";
    private int maxRetries = 3;
    
    // Getters and setters
    
    public String getTriggerQueue() {
        return triggerQueue;
    }
    
    public void setTriggerQueue(String triggerQueue) {
        this.triggerQueue = triggerQueue;
    }
    
    public Map<String, String> getQueueToWorkflow() {
        return queueToWorkflow;
    }
    
    public void setQueueToWorkflow(Map<String, String> queueToWorkflow) {
        this.queueToWorkflow = queueToWorkflow;
    }
    
    public int getMaxConcurrentMessages() {
        return maxConcurrentMessages;
    }
    
    public void setMaxConcurrentMessages(int maxConcurrentMessages) {
        this.maxConcurrentMessages = maxConcurrentMessages;
    }
    
    public int getMaxMessageBatchSize() {
        return maxMessageBatchSize;
    }
    
    public void setMaxMessageBatchSize(int maxMessageBatchSize) {
        this.maxMessageBatchSize = maxMessageBatchSize;
    }
    
    public long getMessageVisibilityTimeout() {
        return messageVisibilityTimeout;
    }
    
    public void setMessageVisibilityTimeout(long messageVisibilityTimeout) {
        this.messageVisibilityTimeout = messageVisibilityTimeout;
    }
    
    public long getMessageWaitTime() {
        return messageWaitTime;
    }
    
    public void setMessageWaitTime(long messageWaitTime) {
        this.messageWaitTime = messageWaitTime;
    }
    
    public boolean isAutoDeleteMessages() {
        return autoDeleteMessages;
    }
    
    public void setAutoDeleteMessages(boolean autoDeleteMessages) {
        this.autoDeleteMessages = autoDeleteMessages;
    }
    
    public String getDeadLetterQueue() {
        return deadLetterQueue;
    }
    
    public void setDeadLetterQueue(String deadLetterQueue) {
        this.deadLetterQueue = deadLetterQueue;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
}