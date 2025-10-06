#pragma once

#include <cstdint>
#include <vector>
#include <array>
#include <algorithm>

/**
 * Simple Vector Clock implementation for distributed systems
 * Uses a fixed-size array for efficiency (max 128 processes)
 */
class VectorClock {
public:
    static constexpr size_t MAX_PROCESSES = 128;
    
private:
    std::array<uint32_t, MAX_PROCESSES> clock_;
    
public:
    VectorClock() : clock_{} {}  // Initialize all to 0
    
    // Get clock value for a specific process
    uint32_t get(uint32_t process_id) const {
        return (process_id < MAX_PROCESSES) ? clock_[process_id] : 0;
    }
    
    // Set clock value for a specific process
    void set(uint32_t process_id, uint32_t value) {
        if (process_id < MAX_PROCESSES) {
            clock_[process_id] = value;
        }
    }
    
    // Increment clock for a specific process
    void increment(uint32_t process_id) {
        if (process_id < MAX_PROCESSES) {
            clock_[process_id]++;
        }
    }
    
    // Update this clock with another clock (take maximum of each element)
    void update(const VectorClock& other) {
        for (size_t i = 0; i < MAX_PROCESSES; ++i) {
            clock_[i] = std::max(clock_[i], other.clock_[i]);
        }
    }
    
    // Check if this clock happens before another clock
    bool happensBefore(const VectorClock& other) const {
        bool strictly_less = false;
        for (size_t i = 0; i < MAX_PROCESSES; ++i) {
            if (clock_[i] > other.clock_[i]) {
                return false;  // Not happens-before
            }
            if (clock_[i] < other.clock_[i]) {
                strictly_less = true;
            }
        }
        return strictly_less;
    }
    
    // Check if two clocks are concurrent (neither happens before the other)
    bool isConcurrent(const VectorClock& other) const {
        return !happensBefore(other) && !other.happensBefore(*this);
    }
    
    // Get raw array for serialization
    const std::array<uint32_t, MAX_PROCESSES>& getRawClock() const {
        return clock_;
    }
    
    // Set from raw array for deserialization
    void setFromRaw(const std::array<uint32_t, MAX_PROCESSES>& raw_clock) {
        clock_ = raw_clock;
    }
    
    // Comparison operators for use in containers
    bool operator==(const VectorClock& other) const {
        return clock_ == other.clock_;
    }
    
    bool operator!=(const VectorClock& other) const {
        return !(*this == other);
    }
    
    bool operator<(const VectorClock& other) const {
        return clock_ < other.clock_;  // Lexicographic comparison
    }
};
/**
 * Message types for Perfect Links protocol
 */
enum class MessageType : uint32_t {
    DATA = 0,           // Data message containing payload
    ACK = 1,            // Acknowledgment message
    HEARTBEAT = 2,      // Placeholder for future heartbeat messages
    CONTROL = 3         // Placeholder for future control messages
};

/**
 * Message structure for Perfect Links protocol
 * 
 * This structure defines the format of messages exchanged between processes
 * in the Perfect Links implementation. It includes all necessary metadata
 * for reliable message delivery with vector clock ordering.
 */
struct PLMessage {
    uint32_t sender_id;        // ID of the process that sent this message
    uint32_t peer_id;          // ID of the intended recipient process
    uint32_t sequence_number;  // Sequence number for message ordering
    VectorClock vector_clock;  // Vector clock for causal ordering
    MessageType message_type;  // Type of message (DATA, ACK, etc.)
    uint32_t payload;          // The actual message content
    bool ack_required;         // Whether this message requires acknowledgment
    
    // Default constructor
    PLMessage() : sender_id(0), peer_id(0), sequence_number(0), vector_clock(), 
                  message_type(MessageType::DATA), payload(0), ack_required(false) {}
    
    // Constructor with vector clock (for backward compatibility)
    PLMessage(uint32_t sid, uint32_t pid, const VectorClock& vclock, MessageType type, 
              uint32_t data, bool ack_req = true) 
        : sender_id(sid), peer_id(pid), sequence_number(0), vector_clock(vclock), 
          message_type(type), payload(data), ack_required(ack_req) {}
    
    // Constructor with sequence number (preferred for Perfect Links)
    PLMessage(uint32_t sid, uint32_t pid, uint32_t seq, MessageType type, 
              uint32_t data, bool ack_req = true) 
        : sender_id(sid), peer_id(pid), sequence_number(seq), vector_clock(), 
          message_type(type), payload(data), ack_required(ack_req) {
        // Set vector clock based on sequence number for compatibility
        vector_clock.set(sid, seq);
    }
};