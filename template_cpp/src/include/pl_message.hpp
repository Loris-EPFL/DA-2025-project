#pragma once

#include <cstdint>

/**
 * Message structure for Perfect Links protocol
 * 
 * This structure defines the format of messages exchanged between processes
 * in the Perfect Links implementation. It includes all necessary metadata
 * for reliable message delivery.
 */
struct PLMessage {
    uint32_t sender_id;        // ID of the process that sent this message
    uint32_t sequence_number;  // Sequence number for ordering and deduplication
    uint32_t message_type;     // 0 = DATA, 1 = ACK
    uint32_t payload;          // The actual message content
    
    /**
     * Default constructor - initializes all fields to zero
     */
    PLMessage() : sender_id(0), sequence_number(0), message_type(0), payload(0) {}
    
    /**
     * Parameterized constructor
     * @param sid Sender ID
     * @param seq Sequence number
     * @param type Message type (0 = DATA, 1 = ACK)
     * @param data Payload data
     */
    PLMessage(uint32_t sid, uint32_t seq, uint32_t type, uint32_t data) 
        : sender_id(sid), sequence_number(seq), message_type(type), payload(data) {}
};