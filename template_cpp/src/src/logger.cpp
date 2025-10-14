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
        std::ofstream output_file(output_path_);
        if (!output_file.is_open()) {
            std::cerr << "Failed to open output file for crash logging: " << output_path_ << std::endl;
            return;
        }
        
        // Write all buffered log entries
        size_t count = log_count_.load();
        for (size_t i = 0; i < count && i < MAX_LOG_ENTRIES; ++i) {
            if (!log_buffer_[i].empty()) {
                output_file << log_buffer_[i] << '\n';
            }
        }
        
        // Ensure data is written to disk
        output_file.flush();
        output_file.close();
        
        std::cout << "Flushed " << count << " log entries to " << output_path_ << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error during crash logging: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown error during crash logging" << std::endl;
    }
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