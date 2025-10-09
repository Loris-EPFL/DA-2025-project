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
 * Perfect Links Message structure for reliable point-to-point communication
 * Contains all necessary information for message delivery and acknowledgment
 */
struct PLMessage {
    uint32_t sender_id;        // ID of the process that sent this message
    uint32_t peer_id;          // ID of the intended recipient process
    uint32_t sequence_number;  // Protocol-managed sequence number for ordering/deduplication
    VectorClock vector_clock;  // Vector clock for causal ordering
    MessageType message_type;  // Type of message (DATA, ACK, etc.)
    std::vector<uint8_t> payload;  // The actual message content (opaque data)
    bool ack_required;         // Whether this message requires acknowledgment
    
    // Default constructor
    PLMessage() : sender_id(0), peer_id(0), sequence_number(0), vector_clock(), 
                  message_type(MessageType::DATA), payload(), ack_required(true) {}
    
    // Constructor with vector clock
    PLMessage(uint32_t sid, uint32_t pid, uint32_t seq_num, const VectorClock& vclock, 
              MessageType type, const std::vector<uint8_t>& data, bool ack_req = true) 
        : sender_id(sid), peer_id(pid), sequence_number(seq_num), vector_clock(vclock), 
          message_type(type), payload(data), ack_required(ack_req) {}
    
    // Constructor without vector clock (for simple cases)
    PLMessage(uint32_t sid, uint32_t pid, uint32_t seq_num, MessageType type, 
              const std::vector<uint8_t>& data, bool ack_req = true) 
        : sender_id(sid), peer_id(pid), sequence_number(seq_num), vector_clock(), 
          message_type(type), payload(data), ack_required(ack_req) {}
    
    // Convenience constructor for ACK messages (empty payload)
    PLMessage(uint32_t sid, uint32_t pid, uint32_t seq_num, const VectorClock& vclock, 
              MessageType type, bool ack_req = false) 
        : sender_id(sid), peer_id(pid), sequence_number(seq_num), vector_clock(vclock), 
          message_type(type), payload(), ack_required(ack_req) {}
};