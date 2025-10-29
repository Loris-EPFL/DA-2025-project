#pragma once

#include "pl_message.hpp"
#include <vector>
#include <cstdint>
#include <cstring>

/**
 * Batch message support for UDP packets
 * 
 * Packet Format:
 * [BatchHeader][PLMessage1][PLMessage2]...[PLMessageN] (N <= 8)
 */

/**
 * Header for batch messages
 */
struct BatchHeader {
    uint32_t magic_number;
    uint32_t message_count;
    uint32_t total_size;
    uint32_t reserved;
    
    static constexpr uint32_t MAGIC_NUMBER = 0xBA7C4001;
    static constexpr uint32_t MAX_MESSAGES_PER_BATCH = 8;
    
    BatchHeader() : magic_number(MAGIC_NUMBER), message_count(0), total_size(0), reserved(0) {}
    
    BatchHeader(uint32_t count, uint32_t size) 
        : magic_number(MAGIC_NUMBER), message_count(count), total_size(size), reserved(0) {}
    
    bool isValid() const {
        return magic_number == MAGIC_NUMBER && 
               message_count > 0 && 
               message_count <= MAX_MESSAGES_PER_BATCH;
    }
};

/**
 * Batch Message container for multiple PLMessage objects
 */
class BatchMessage {
public:
    static constexpr uint32_t MAX_MESSAGES_PER_BATCH = 8;
    static constexpr size_t MAX_UDP_PACKET_SIZE = 65507;
    
private:
    std::vector<PLMessage> messages_;
    
public:
    BatchMessage() {
        messages_.reserve(MAX_MESSAGES_PER_BATCH);
    }
    
    /**
     * Add a message to the batch
     * @param message The PLMessage to add
     * @return true if added successfully, false if batch is full
     */
    bool addMessage(const PLMessage& message) {
        if (messages_.size() >= MAX_MESSAGES_PER_BATCH) {
            return false;
        }
        messages_.push_back(message);
        return true;
    }
    
    /**
     * Get the number of messages in the batch
     */
    size_t getMessageCount() const {
        return messages_.size();
    }
    
    /**
     * Check if the batch is empty
     */
    bool isEmpty() const {
        return messages_.empty();
    }
    
    /**
     * Check if the batch is full
     */
    bool isFull() const {
        return messages_.size() >= MAX_MESSAGES_PER_BATCH;
    }
    
    /**
     * Get all messages in the batch
     */
    const std::vector<PLMessage>& getMessages() const {
        return messages_;
    }
    
    /**
     * Clear all messages from the batch
     */
    void clear() {
        messages_.clear();
    }
    
    /**
     * Serialize the entire batch into a buffer
     * @param buffer Output buffer to store serialized data
     * @return Size of serialized data, or 0 if serialization failed
     */
    size_t serialize(std::vector<uint8_t>& buffer) const {
        if (messages_.empty()) {
            return 0;
        }
        
        // Calculate total size needed
        size_t total_size = sizeof(BatchHeader);
        std::vector<std::vector<uint8_t>> serialized_messages;
        serialized_messages.reserve(messages_.size());
        
        for (const auto& message : messages_) {
            std::vector<uint8_t> msg_buffer;
            size_t msg_size = message.serialize(msg_buffer);
            if (msg_size == 0) {
                return 0; // Serialization failed
            }
            total_size += msg_size;
            serialized_messages.push_back(std::move(msg_buffer));
        }
        
        // Check if total size exceeds UDP packet limit
        if (total_size > MAX_UDP_PACKET_SIZE) {
            return 0; // Batch too large
        }
        
        // Create batch header
        BatchHeader header(static_cast<uint32_t>(messages_.size()), 
                          static_cast<uint32_t>(total_size));
        
        // Serialize to buffer
        buffer.resize(total_size);
        size_t offset = 0;
        
        // Copy header
        std::memcpy(buffer.data() + offset, &header, sizeof(BatchHeader));
        offset += sizeof(BatchHeader);
        
        // Copy all serialized messages
        for (const auto& msg_buffer : serialized_messages) {
            std::memcpy(buffer.data() + offset, msg_buffer.data(), msg_buffer.size());
            offset += msg_buffer.size();
        }
        
        return total_size;
    }
    
    /**
     * Deserialize a batch from a buffer
     * @param buffer Input buffer containing serialized batch data
     * @param size Size of the buffer
     * @return true if deserialization successful, false otherwise
     */
    bool deserialize(const uint8_t* buffer, size_t size) {
        messages_.clear();
        
        if (size < sizeof(BatchHeader)) {
            return false;
        }
        
        // Read and validate header
        const BatchHeader* header = reinterpret_cast<const BatchHeader*>(buffer);
        if (!header->isValid() || header->total_size != size) {
            return false;
        }
        
        // Deserialize messages
        size_t offset = sizeof(BatchHeader);
        for (uint32_t i = 0; i < header->message_count; ++i) {
            if (offset >= size) {
                return false; // Not enough data
            }
            
            PLMessage message;
            
            // Find the size of this message by reading its header first
            if (offset + sizeof(PLMessageHeader) > size) {
                return false;
            }
            
            const PLMessageHeader* msg_header = 
                reinterpret_cast<const PLMessageHeader*>(buffer + offset);
            size_t msg_size = sizeof(PLMessageHeader) + msg_header->payload_size;
            
            if (offset + msg_size > size) {
                return false; // Message extends beyond buffer
            }
            
            // Deserialize the message
            if (!message.deserialize(buffer + offset, msg_size)) {
                return false;
            }
            
            messages_.push_back(std::move(message));
            offset += msg_size;
        }
        
        return true;
    }
    
    /**
     * Estimate the serialized size of the current batch
     * @return Estimated size in bytes
     */
    size_t estimateSerializedSize() const {
        size_t size = sizeof(BatchHeader);
        for (const auto& message : messages_) {
            size += sizeof(PLMessageHeader) + message.payload.size();
        }
        return size;
    }
    
    /**
     * Check if adding a message would exceed the UDP packet size limit
     * @param message The message to potentially add
     * @return true if the message can be added without exceeding limits
     */
    bool canAddMessage(const PLMessage& message) const {
        if (isFull()) {
            return false;
        }
        
        size_t current_size = estimateSerializedSize();
        size_t message_size = sizeof(PLMessageHeader) + message.payload.size();
        
        return (current_size + message_size) <= MAX_UDP_PACKET_SIZE;
    }
};