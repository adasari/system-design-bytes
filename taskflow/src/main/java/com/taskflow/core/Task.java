package com.taskflow.core;

import java.util.concurrent.CompletableFuture;

/**
 * Task defines the fundamental unit of work in the workflow engine.
 *
 * <p>A Task represents the actual business logic or operation that needs to be executed.
 * It is a functional interface that encapsulates the execution logic and returns a
 * result asynchronously via CompletableFuture.</p>
 *
 * <p><b>Key Concepts:</b></p>
 * <ul>
 *   <li><b>Execution Logic</b>: The actual work performed (HTTP call, computation, etc.)</li>
 *   <li><b>Reusability</b>: Tasks can be reused across different workflows and steps</li>
 *   <li><b>Type Safety</b>: Generic type T defines the output data type</li>
 *   <li><b>Asynchronous</b>: Returns CompletableFuture for non-blocking execution</li>
 *   <li><b>Context Aware</b>: Receives ExecutionContext for data access and sharing</li>
 * </ul>
 *
 * <p><b>Implementation Example:</b></p>
 * <pre>{@code
 * public class EmailTask implements Task<String> {
 *     private final String recipient;
 *
 *     public EmailTask(String recipient) {
 *         this.recipient = recipient;
 *     }
 *
 *     @Override
 *     public CompletableFuture<TaskResult<String>> execute(ExecutionContext context) {
 *         return CompletableFuture.supplyAsync(() -> {
 *             String subject = (String) context.get("emailSubject");
 *             String body = (String) context.get("emailBody");
 *
 *             // Send email logic here
 *             String messageId = sendEmail(recipient, subject, body);
 *
 *             return TaskResult.success("emailTask", messageId);
 *         });
 *     }
 * }
 * }</pre>
 *
 * <p><b>Lambda Implementation:</b></p>
 * <pre>{@code
 * Task<Integer> computeTask = context ->
 *     CompletableFuture.completedFuture(
 *         TaskResult.success("compute",
 *             ((Integer) context.get("x")) * 2)
 *     );
 * }</pre>
 *
 * @param <T> The type of data returned by the task upon successful execution
 * @see TaskResult
 * @see ExecutionContext
 */
@FunctionalInterface
public interface Task<T> {

    CompletableFuture<TaskResult<T>> execute(ExecutionContext context);

    default String getName() {
        return this.getClass().getSimpleName();
    }

    default int getMaxRetries() {
        return 0;
    }

    default long getRetryDelayMs() {
        return 1000;
    }

    default boolean isAsync() {
        return true;
    }
}
