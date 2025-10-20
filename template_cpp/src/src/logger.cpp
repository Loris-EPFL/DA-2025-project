#include "logger.hpp"
#include <fstream>
#include <iostream>
#include <sstream>

// Global logger instance for signal handler access
std::atomic<OptimizedLogger*> g_optimized_logger{nullptr};

OptimizedLogger::OptimizedLogger(const std::string& output_path) 
    : output_path_(output_path) {
    // Pre-allocate the log buffer to avoid reallocations
    log_buffer_.resize(MAX_LOG_ENTRIES);
    
    // Register this instance globally for signal handler access
    g_optimized_logger.store(this);
}

OptimizedLogger::~OptimizedLogger() {
    // Ensure logs are flushed if not already done
    if (!flushed_.load()) {
        flushOnCrash();
    }
    
    // Clear global reference
    OptimizedLogger* expected = this;
    g_optimized_logger.compare_exchange_strong(expected, nullptr);
}

void OptimizedLogger::logBroadcast(uint32_t sequence_number) {
    size_t index = log_count_.fetch_add(1, std::memory_order_relaxed);
    
    // Check bounds to prevent buffer overflow
    if (index < MAX_LOG_ENTRIES) {
        log_buffer_[index] = formatBroadcast(sequence_number);
        
        // Check if we should trigger periodic flush (non-blocking check)
        if ((index + 1) % PERIODIC_FLUSH_THRESHOLD == 0) {
            periodicFlush(false);
        }
    } else {
        // If we exceed the buffer, we need to flush immediately
        // This is a safety mechanism, though it should rarely happen
        flushOnCrash();
    }
}

void OptimizedLogger::logDelivery(uint32_t sender_id, uint32_t sequence_number) {
    size_t index = log_count_.fetch_add(1, std::memory_order_relaxed);
    
    // Check bounds to prevent buffer overflow
    if (index < MAX_LOG_ENTRIES) {
        log_buffer_[index] = formatDelivery(sender_id, sequence_number);
        
        // Check if we should trigger periodic flush (non-blocking check)
        if ((index + 1) % PERIODIC_FLUSH_THRESHOLD == 0) {
            periodicFlush(false);
        }
    } else {
        // If we exceed the buffer, we need to flush immediately
        // This is a safety mechanism, though it should rarely happen
        flushOnCrash();
    }
}

void OptimizedLogger::flushOnCrash() {
    // Use compare_exchange to ensure we only flush once
    bool expected = false;
    if (!flushed_.compare_exchange_strong(expected, true)) {
        // Already flushed by another thread
        return;
    }
    
    try {
        // For crash-time flush, we need to handle both scenarios:
        // 1. If periodic flushes have occurred, append remaining entries
        // 2. If no periodic flushes, write all entries (overwrite mode for compatibility)
        
        size_t last_flushed = last_flushed_count_.load();
        size_t current_count = log_count_.load();
        
        std::ofstream output_file;
        if (last_flushed > 0) {
            // Periodic flushes have occurred, append remaining entries
            output_file.open(output_path_, std::ios::app);
        } else {
            // No periodic flushes, write all entries (overwrite for compatibility)
            output_file.open(output_path_);
        }
        
        if (!output_file.is_open()) {
            std::cerr << "Failed to open output file for crash logging: " << output_path_ << std::endl;
            return;
        }
        
        // Write log entries (all if no periodic flush, or remaining if periodic flushes occurred)
        size_t start_index = (last_flushed > 0) ? last_flushed : 0;
        size_t entries_written = 0;
        
        for (size_t i = start_index; i < current_count && i < MAX_LOG_ENTRIES; ++i) {
            if (!log_buffer_[i].empty()) {
                output_file << log_buffer_[i] << '\n';
                entries_written++;
            }
        }
        
        // Ensure data is written to disk
        output_file.flush();
        output_file.close();
        
        std::cout << "Crash flush: wrote " << entries_written << " entries to " << output_path_ << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error during crash logging: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown error during crash logging" << std::endl;
    }
}

void OptimizedLogger::periodicFlush(bool force_flush) {
    size_t current_count = log_count_.load();
    size_t last_flushed = last_flushed_count_.load();
    
    // Check if we have new entries to flush
    if (!force_flush && current_count <= last_flushed) {
        return; // Nothing new to flush
    }
    
    // Check threshold for non-forced flushes
    if (!force_flush && (current_count - last_flushed) < PERIODIC_FLUSH_THRESHOLD) {
        return; // Not enough entries to warrant a flush
    }
    
    try {
        // Open file in append mode to preserve existing content
        std::ofstream output_file(output_path_, std::ios::app);
        if (!output_file.is_open()) {
            std::cerr << "Failed to open output file for periodic logging: " << output_path_ << std::endl;
            return;
        }
        
        // Write only new log entries since last flush
        size_t entries_written = 0;
        for (size_t i = last_flushed; i < current_count && i < MAX_LOG_ENTRIES; ++i) {
            if (!log_buffer_[i].empty()) {
                output_file << log_buffer_[i] << '\n';
                entries_written++;
            }
        }
        
        // Ensure data is written to disk
        output_file.flush();
        output_file.close();
        
        // Update last flushed count atomically
        last_flushed_count_.store(current_count);
        
        if (entries_written > 0) {
            std::cout << "Periodic flush: wrote " << entries_written << " new entries to " << output_path_ << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error during periodic logging: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown error during periodic logging" << std::endl;
    }
}

bool OptimizedLogger::shouldPeriodicFlush() const {
    size_t current_count = log_count_.load();
    size_t last_flushed = last_flushed_count_.load();
    return (current_count - last_flushed) >= PERIODIC_FLUSH_THRESHOLD;
}

size_t OptimizedLogger::getBufferedCount() const {
    return log_count_.load();
}

std::function<void(uint32_t, uint32_t)> OptimizedLogger::createDeliveryCallback() {
    // Return a lambda that captures this logger instance
    return [this](uint32_t sender_id, uint32_t sequence_number) noexcept {
        try {
            this->logDelivery(sender_id, sequence_number);
        } catch (...) {
            // Silently ignore errors in noexcept callback
        }
    };
}

std::string OptimizedLogger::formatBroadcast(uint32_t sequence_number) {
    std::ostringstream oss;
    oss << "b " << sequence_number;
    return oss.str();
}

std::string OptimizedLogger::formatDelivery(uint32_t sender_id, uint32_t sequence_number) {
    std::ostringstream oss;
    oss << "d " << sender_id << " " << sequence_number;
    return oss.str();
}