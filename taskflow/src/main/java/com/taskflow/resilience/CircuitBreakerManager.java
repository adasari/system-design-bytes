package com.taskflow.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Circuit Breaker implementation inspired by Netflix Conductor's resilience patterns.
 * Manages circuit breakers for tasks and workflows to prevent cascading failures.
 */
@Component
public class CircuitBreakerManager {

    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerManager.class);

    private final ConcurrentHashMap<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();

    /**
     * Get or create a circuit breaker for the given identifier
     */
    public CircuitBreaker getCircuitBreaker(String identifier) {
        return getCircuitBreaker(identifier, CircuitBreakerConfig.defaultConfig());
    }

    /**
     * Get or create a circuit breaker with custom configuration
     */
    public CircuitBreaker getCircuitBreaker(String identifier, CircuitBreakerConfig config) {
        return circuitBreakers.computeIfAbsent(identifier, k -> new CircuitBreaker(identifier, config));
    }

    /**
     * Execute operation with circuit breaker protection
     */
    public <T> T execute(String identifier, CircuitBreakerOperation<T> operation) throws Exception {
        CircuitBreaker cb = getCircuitBreaker(identifier);
        return cb.execute(operation);
    }

    /**
     * Get circuit breaker state
     */
    public CircuitBreakerState getState(String identifier) {
        CircuitBreaker cb = circuitBreakers.get(identifier);
        return cb != null ? cb.getState() : CircuitBreakerState.CLOSED;
    }

    /**
     * Get circuit breaker statistics
     */
    public CircuitBreakerStats getStats(String identifier) {
        CircuitBreaker cb = circuitBreakers.get(identifier);
        return cb != null ? cb.getStats() : new CircuitBreakerStats();
    }

    /**
     * Reset circuit breaker to closed state
     */
    public void reset(String identifier) {
        CircuitBreaker cb = circuitBreakers.get(identifier);
        if (cb != null) {
            cb.reset();
            logger.info("Circuit breaker reset: {}", identifier);
        }
    }

    /**
     * Remove circuit breaker
     */
    public void remove(String identifier) {
        CircuitBreaker removed = circuitBreakers.remove(identifier);
        if (removed != null) {
            logger.info("Circuit breaker removed: {}", identifier);
        }
    }

    /**
     * Get all circuit breaker identifiers
     */
    public java.util.Set<String> getAllIdentifiers() {
        return circuitBreakers.keySet();
    }

    /**
     * Circuit Breaker States
     */
    public enum CircuitBreakerState {
        CLOSED,    // Normal operation
        OPEN,      // Failing fast
        HALF_OPEN  // Testing if service recovered
    }

    /**
     * Circuit Breaker Configuration
     */
    public static class CircuitBreakerConfig {
        private final int failureThreshold;
        private final int requestVolumeThreshold;
        private final long timeoutMs;
        private final int halfOpenMaxCalls;
        private final double errorPercentageThreshold;

        public CircuitBreakerConfig(int failureThreshold, int requestVolumeThreshold,
                                   long timeoutMs, int halfOpenMaxCalls,
                                   double errorPercentageThreshold) {
            this.failureThreshold = failureThreshold;
            this.requestVolumeThreshold = requestVolumeThreshold;
            this.timeoutMs = timeoutMs;
            this.halfOpenMaxCalls = halfOpenMaxCalls;
            this.errorPercentageThreshold = errorPercentageThreshold;
        }

        public static CircuitBreakerConfig defaultConfig() {
            return new CircuitBreakerConfig(5, 10, 60000, 3, 50.0);
        }

        public static CircuitBreakerConfig create(int failureThreshold, int requestVolumeThreshold,
                                                 long timeoutMs) {
            return new CircuitBreakerConfig(failureThreshold, requestVolumeThreshold,
                                          timeoutMs, 3, 50.0);
        }

        // Getters
        public int getFailureThreshold() { return failureThreshold; }
        public int getRequestVolumeThreshold() { return requestVolumeThreshold; }
        public long getTimeoutMs() { return timeoutMs; }
        public int getHalfOpenMaxCalls() { return halfOpenMaxCalls; }
        public double getErrorPercentageThreshold() { return errorPercentageThreshold; }
    }

    /**
     * Circuit Breaker Statistics
     */
    public static class CircuitBreakerStats {
        private final AtomicLong totalRequests = new AtomicLong(0);
        private final AtomicLong successfulRequests = new AtomicLong(0);
        private final AtomicLong failedRequests = new AtomicLong(0);
        private final AtomicLong rejectedRequests = new AtomicLong(0);
        private volatile Instant lastFailureTime;
        private volatile String lastFailureReason;

        public void recordSuccess() {
            totalRequests.incrementAndGet();
            successfulRequests.incrementAndGet();
        }

        public void recordFailure(String reason) {
            totalRequests.incrementAndGet();
            failedRequests.incrementAndGet();
            lastFailureTime = Instant.now();
            lastFailureReason = reason;
        }

        public void recordRejection() {
            rejectedRequests.incrementAndGet();
        }

        public double getErrorPercentage() {
            long total = totalRequests.get();
            if (total == 0) return 0.0;
            return (failedRequests.get() * 100.0) / total;
        }

        // Getters
        public long getTotalRequests() { return totalRequests.get(); }
        public long getSuccessfulRequests() { return successfulRequests.get(); }
        public long getFailedRequests() { return failedRequests.get(); }
        public long getRejectedRequests() { return rejectedRequests.get(); }
        public Instant getLastFailureTime() { return lastFailureTime; }
        public String getLastFailureReason() { return lastFailureReason; }
    }

    /**
     * Circuit Breaker Implementation
     */
    public static class CircuitBreaker {
        private final String identifier;
        private final CircuitBreakerConfig config;
        private final CircuitBreakerStats stats;
        private final AtomicInteger consecutiveFailures;
        private final AtomicLong lastOpenTime;
        private final AtomicInteger halfOpenCalls;
        private volatile CircuitBreakerState state;

        public CircuitBreaker(String identifier, CircuitBreakerConfig config) {
            this.identifier = identifier;
            this.config = config;
            this.stats = new CircuitBreakerStats();
            this.consecutiveFailures = new AtomicInteger(0);
            this.lastOpenTime = new AtomicLong(0);
            this.halfOpenCalls = new AtomicInteger(0);
            this.state = CircuitBreakerState.CLOSED;
        }

        public <T> T execute(CircuitBreakerOperation<T> operation) throws Exception {
            if (state == CircuitBreakerState.OPEN) {
                if (shouldAttemptReset()) {
                    transitionToHalfOpen();
                } else {
                    stats.recordRejection();
                    throw new CircuitBreakerOpenException("Circuit breaker is OPEN for: " + identifier);
                }
            }

            if (state == CircuitBreakerState.HALF_OPEN) {
                if (halfOpenCalls.get() >= config.getHalfOpenMaxCalls()) {
                    stats.recordRejection();
                    throw new CircuitBreakerOpenException("Half-open limit reached for: " + identifier);
                }
            }

            try {
                if (state == CircuitBreakerState.HALF_OPEN) {
                    halfOpenCalls.incrementAndGet();
                }

                T result = operation.execute();
                onSuccess();
                return result;

            } catch (Exception e) {
                onFailure(e.getMessage());
                throw e;
            }
        }

        private void onSuccess() {
            stats.recordSuccess();
            consecutiveFailures.set(0);

            if (state == CircuitBreakerState.HALF_OPEN) {
                transitionToClosed();
                logger.info("Circuit breaker recovered: {}", identifier);
            }
        }

        private void onFailure(String reason) {
            stats.recordFailure(reason);
            int failures = consecutiveFailures.incrementAndGet();

            if (state == CircuitBreakerState.HALF_OPEN) {
                transitionToOpen();
                logger.warn("Circuit breaker failed during half-open state: {}", identifier);
            } else if (state == CircuitBreakerState.CLOSED && shouldOpen()) {
                transitionToOpen();
                logger.warn("Circuit breaker opened due to failures: {} (consecutive: {})",
                    identifier, failures);
            }
        }

        private boolean shouldOpen() {
            return consecutiveFailures.get() >= config.getFailureThreshold() &&
                   stats.getTotalRequests() >= config.getRequestVolumeThreshold() &&
                   stats.getErrorPercentage() >= config.getErrorPercentageThreshold();
        }

        private boolean shouldAttemptReset() {
            return System.currentTimeMillis() - lastOpenTime.get() >= config.getTimeoutMs();
        }

        private void transitionToOpen() {
            state = CircuitBreakerState.OPEN;
            lastOpenTime.set(System.currentTimeMillis());
        }

        private void transitionToHalfOpen() {
            state = CircuitBreakerState.HALF_OPEN;
            halfOpenCalls.set(0);
            logger.info("Circuit breaker transitioning to half-open: {}", identifier);
        }

        private void transitionToClosed() {
            state = CircuitBreakerState.CLOSED;
            consecutiveFailures.set(0);
        }

        public void reset() {
            state = CircuitBreakerState.CLOSED;
            consecutiveFailures.set(0);
            halfOpenCalls.set(0);
            lastOpenTime.set(0);
        }

        // Getters
        public CircuitBreakerState getState() { return state; }
        public CircuitBreakerStats getStats() { return stats; }
        public String getIdentifier() { return identifier; }
        public CircuitBreakerConfig getConfig() { return config; }
    }

    /**
     * Functional interface for operations protected by circuit breaker
     */
    @FunctionalInterface
    public interface CircuitBreakerOperation<T> {
        T execute() throws Exception;
    }

    /**
     * Exception thrown when circuit breaker is open
     */
    public static class CircuitBreakerOpenException extends RuntimeException {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}
