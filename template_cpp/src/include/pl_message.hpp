#pragma once

#include <cstdint>
#include <vector>
#include <algorithm>
#include <cstring>

/**
 * Message types for Perfect Links protocol
 */
enum class MessageType : uint8_t {
    DATA = 0,
    ACK = 1,
    HEARTBEAT = 2, //Heartbeat for future milsetone using a Eventually PFD
    CONTROL = 3
};

/**
 * Perfect Links Message structure
 * Contains information for message delivery and ack
 */
/**
 * Serializable header for PLMessage
 */
struct PLMessageHeader {
    uint32_t sender_id;
    uint32_t peer_id;
    uint32_t sequence_number;
    MessageType message_type;
    uint32_t payload_size;
    bool ack_required;
    
    PLMessageHeader() : sender_id(0), peer_id(0), sequence_number(0), message_type(MessageType::DATA), payload_size(0), ack_required(true) {}
};

struct PLMessage {
    uint32_t sender_id;
    uint32_t peer_id;
    uint32_t sequence_number;
    MessageType message_type;
    std::vector<uint8_t> payload;
    bool ack_required;
    
    // Default constructor
    PLMessage() : sender_id(0), peer_id(0), sequence_number(0), message_type(MessageType::DATA), payload(), ack_required(true) {}
    
    // Constructor with payload
    PLMessage(uint32_t sid, uint32_t pid, uint32_t seq_num, MessageType type, const std::vector<uint8_t>& data, bool ack_req = true) : sender_id(sid), peer_id(pid), sequence_number(seq_num), message_type(type), payload(data), ack_required(ack_req) {}
    
    // Constructor for ACK messages without payload
    PLMessage(uint32_t sid, uint32_t pid, uint32_t seq_num, MessageType type, bool ack_req = false) : sender_id(sid), peer_id(pid), sequence_number(seq_num), message_type(type), payload(), ack_required(ack_req) {}
    
    /**
     * Serialize message to a buffer for transmission
     * @param buffer Output buffer (will be resized as needed)
     * @return Size of serialized data
     */
    size_t serialize(std::vector<uint8_t>& buffer) const {
        PLMessageHeader header;
        //Header fields
        header.sender_id = sender_id;
        header.peer_id = peer_id;
        header.sequence_number = sequence_number;
        header.message_type = message_type;
        header.payload_size = static_cast<uint32_t>(payload.size());
        header.ack_required = ack_required;
        
        size_t total_size = sizeof(PLMessageHeader) + payload.size();
        buffer.resize(total_size);
        
        // Copy header
        std::memcpy(buffer.data(), &header, sizeof(PLMessageHeader));
        
        // Copy payload if present
        if (!payload.empty()) {
            std::memcpy(buffer.data() + sizeof(PLMessageHeader), payload.data(), payload.size());
        }
        
        return total_size;
    }
    
    /**
     * Deserialize message from a buffer
     * @param buffer Input buffer containing serialized data
     * @param size Size of the buffer
     * @return true if deserialization successful, false otherwise
     */
    bool deserialize(const uint8_t* buffer, size_t size) {
        if (size < sizeof(PLMessageHeader)) {
            return false;
        }
        
        const PLMessageHeader* header = reinterpret_cast<const PLMessageHeader*>(buffer);
        
        // Validate payload size
        if (sizeof(PLMessageHeader) + header->payload_size != size) {
            return false;
        }
        
        // Copy header fields
        sender_id = header->sender_id;
        peer_id = header->peer_id;
        sequence_number = header->sequence_number;
        message_type = header->message_type;
        ack_required = header->ack_required;
        
        // Copy payload
        payload.clear();
        if (header->payload_size > 0) {
            payload.resize(header->payload_size);
            std::memcpy(payload.data(), buffer + sizeof(PLMessageHeader), header->payload_size);
        }
        
        return true;
    }
};