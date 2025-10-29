#pragma once

#include <vector>
#include <string>
#include <atomic>
#include <memory>
#include <functional>

/**
 * Logger - High-performance logging with periodic and crash-time persistence
 * 
 * This logger eliminates mutex contention during normal operation by buffering
 * log events in memory and periodically writing to disk with file appending.
 * It also maintains crash-time persistence when the process receives
 * SIGTERM or SIGINT signals, as allowed by the DA Project specifications.
 * 
 * Key benefits:
 * - No mutex locking during message delivery (lock-free operation)
 * - Periodic file appending for incremental persistence
 * - Batch I/O for better performance
 * - Crash-safe logging via signal handler
 * - Thread-safe append operations using atomic operations
 */
class Logger {
public:
    /**
     * Constructor
     * @param output_path Path to the output file where logs will be written on crash
     */
    explicit Logger(const std::string& output_path);
    
    /**
     * Destructor - ensures logs are flushed if not already done
     */
    ~Logger();
    
    /**
     * Log a broadcast event (thread-safe, lock-free)
     * @param sequence_number The sequence number of the broadcast message
     */
    void logBroadcast(uint32_t sequence_number);
    
    /**
     * Log a delivery event (thread-safe, lock-free)
     * @param sender_id The ID of the process that sent the message
     * @param sequence_number The sequence number of the delivered message
     */
    void logDelivery(uint32_t sender_id, uint32_t sequence_number);
    
    /**
     * Flush all buffered logs to disk (called from signal handler)
     * This is the only function that performs file I/O
     * Thread-safe and can be called from signal handlers
     */
    void flushOnCrash();
    
    /**
     * Perform periodic flush of buffered logs to disk with file appending
     * This method appends new log entries to the existing file without clearing it
     * Thread-safe and designed for periodic calls during normal operation
     * @param force_flush If true, flush all entries; if false, only flush when buffer reaches threshold
     */
    void periodicFlush(bool force_flush = false);
    
    /**
     * Check if periodic flush should be triggered based on buffer size
     * @return true if buffer has reached the threshold for periodic flushing
     */
    bool shouldPeriodicFlush() const;
    
    /**
     * Get the number of buffered log entries (for debugging/monitoring)
     */
    size_t getBufferedCount() const;
    
    /**
     * Create a delivery callback function that uses this logger
     * @return A callback function compatible with PerfectLinks
     */
    std::function<void(uint32_t, uint32_t)> createDeliveryCallback();

private:
    std::string output_path_;
    
    // Lock-free log buffer using atomic operations
    // We use a simple approach: pre-allocate a large buffer and use atomic index
    static constexpr size_t MAX_LOG_ENTRIES = 1000000; // 1M entries should be enough
    static constexpr size_t PERIODIC_FLUSH_THRESHOLD = 10000; // Flush every 10K entries
    
    std::vector<std::string> log_buffer_;
    std::atomic<size_t> log_count_{0};
    std::atomic<size_t> last_flushed_count_{0}; // Track last flushed position for periodic dumps
    std::atomic<bool> flushed_{false}; // For crash-time flush protection
    
    // Helper to format log entries
    std::string formatBroadcast(uint32_t sequence_number);
    std::string formatDelivery(uint32_t sender_id, uint32_t sequence_number);
};

/**
 * Global logger instance for signal handler access
 * This needs to be global so the signal handler can access it
 */
extern std::atomic<Logger*> g_optimized_logger;