#pragma once

#include "parser.hpp"
#include "pl_message.hpp"
#include "batch_message.hpp"
#include "host_utils.hpp"
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <atomic>
#include <fstream>
#include <map>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <functional>
#include <set>
#include <unordered_set>
#include <sys/socket.h>
#include <netinet/in.h>



/**
 * Perfect Links implementation for reliable point-to-point communication
 * 
 * Provides the following guarantees:
 * - PL1 (Reliable delivery): If a correct process p sends a message m to a correct process q, then q eventually delivers m
 * - PL2 (No duplication): No message is delivered by a process more than once
 * - PL3 (No creation): If a process q delivers a message m with sender p, then m was previously sent to q by process p
 */
class PerfectLinks {
public:
    /**
     * Modern constructor using Parser-based approach
     */
    PerfectLinks(Parser::Host localhost,
                std::function<void(uint32_t, uint32_t)> deliveryCallback,
                std::map<unsigned long, Parser::Host> idToPeer,
                const std::string& output_path);
    

    
    /**
     * Destructor - ensures proper cleanup
     */
    ~PerfectLinks();
    
    /**
     * Initialize the Perfect Links system
     * Creates UDP socket and binds to the process's address
     * @return true if initialization successful, false otherwise
     */
    bool initialize();
    
    /**
     * Start the Perfect Links system
     * Begins message processing threads
     */
    void start();
    
    /**
     * Stop the Perfect Links system
     * Gracefully shuts down all threads and closes resources
     */
    void stop();
    
    /**
     * Send a message to a specific destination using Perfect Links
     * @param destination_id ID of the destination process
     * @param payload The message payload to send (opaque data)
     */
    void send(uint8_t destination_id, const std::vector<uint8_t>& payload);
    
    /**
     * Broadcast a message to all other processes
     * @param payload The message payload to broadcast (opaque data)
     */
    void broadcast(const std::vector<uint8_t>& payload);
    
    /**
     * Convenience method: Send a message with integer payload
     * @param destination_id ID of the destination process
     * @param message The integer message to send
     */
    void send(uint8_t destination_id, uint32_t message);
    
    /**
     * Convenience method: Broadcast a message with integer payload
     * @param message The integer message to broadcast
     */
    /**
     * Broadcast a message to all peers
     * @param message The message to broadcast
     */
    void broadcast(uint32_t message);

    /**
     * Write all logged events to output file in chronological order
     */
    void writeLogsToFile();

private:
    // Core member variables
    uint8_t process_id_;
    Parser::Host localhost_;
    std::map<unsigned long, Parser::Host> id_to_peer_;
    std::function<void(uint32_t, uint32_t)> delivery_callback_;
    std::string output_path_;
    int socket_fd_;
    VectorClock local_vector_clock_;  // Local vector clock for this process
    std::atomic<bool> running_;
    std::atomic<uint32_t> next_sequence_number_;  // Protocol-managed sequence numbers
    
    // Timing constants for retransmission
    static constexpr std::chrono::milliseconds RETRANSMISSION_TIMEOUT{100};  // Reduced from 500ms for higher throughput
    static constexpr std::chrono::milliseconds RETRANSMISSION_SLEEP{2};      // Reduced from 5ms for more aggressive retransmission
    static constexpr std::chrono::milliseconds MAX_ADAPTIVE_TIMEOUT{1000};   // Reduced from 2000ms for faster recovery
    
    // Cleanup constants for delivered_messages_ memory management
    static constexpr size_t DELIVERED_MESSAGES_CLEANUP_THRESHOLD = 50000;  // Cleanup when we have this many delivered messages per sender
    static constexpr size_t DELIVERED_MESSAGES_KEEP_RECENT = 10000;        // Keep this many recent sequence numbers per sender after cleanup
    
    // Threading
    std::thread receiver_thread_;
    std::thread retransmission_thread_;
    
    // Message tracking with simplified identifiers
    struct MessageId {
        uint8_t sender_id;
        uint32_t sequence_number;
        
        bool operator<(const MessageId& other) const {
            if (sender_id != other.sender_id) return sender_id < other.sender_id;
            return sequence_number < other.sequence_number;
        }
        
        bool operator==(const MessageId& other) const {
            return sender_id == other.sender_id && sequence_number == other.sequence_number;
        }
    };
    
    struct PendingMessage {
        PLMessage message;
        Parser::Host destination;
        std::chrono::steady_clock::time_point last_sent;
        bool ack_received;
        uint32_t retransmit_count;  // Track retransmission attempts for adaptive timeout
        
        PendingMessage() : ack_received(false), retransmit_count(0) {}
    };
    
    std::map<std::pair<uint8_t, uint32_t>, PendingMessage> pending_messages_;
    std::mutex pending_messages_mutex_;
    
    // Track delivered messages to prevent duplicates (sender_id -> set of sequence numbers)
    std::map<uint8_t, std::set<uint32_t>> delivered_messages_;
    std::mutex delivered_messages_mutex_;
    
    // Message batching support for 8 messages per packet
    std::map<uint8_t, std::vector<PLMessage>> pending_batches_;  // Use destination_id as key instead of Host
    std::mutex pending_batches_mutex_;
    std::chrono::steady_clock::time_point last_batch_time_;
    static constexpr std::chrono::milliseconds BATCH_TIMEOUT{5}; // Send batch after 5ms
    static constexpr size_t MAX_BATCH_SIZE = 8; // Maximum messages per batch
    
    // Private helper methods
    
    /**
     * Send a message over UDP (original method for individual messages)
     * @param msg The message to send
     * @param destination The destination host
     */
    void sendMessage(const PLMessage& msg, const Parser::Host& destination);
    
    /**
     * Send a message using batching (up to 8 messages per packet)
     * @param msg The message to send
     * @param destination_id The destination process ID
     */
    void sendBatchedMessage(const PLMessage& msg, uint8_t destination_id);
    
    /**
     * Flush pending batch for a specific destination
     * @param destination_id The destination ID to flush batch for
     */
    void flushBatch(uint8_t destination_id);
    
    /**
     * Flush all pending batches
     */
    void flushAllBatches();
    
    /**
     * Send a batch to a specific destination
     * @param batch The batch to send
     * @param destination The destination host
     */
    void sendBatchToDestination(const BatchMessage& batch, const Parser::Host& destination);
    
    /**
     * Handle batched messages from received buffer
     * @param buffer The received buffer containing potential batch data
     * @param sender_addr The sender's socket address
     * @return true if successfully processed as batch, false otherwise
     */
    bool handleBatchedMessage(const std::vector<uint8_t>& buffer, const struct sockaddr_in& sender_addr);
    
    /**
     * Main receive loop - runs in separate thread
     */
    void receiveLoop();
    
    /**
     * Handle incoming message
     * @param msg The received message
     * @param sender_addr Address of the sender
     */
    void handleMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr);
    
    /**
     * Handle incoming data message
     * @param msg The data message
     * @param sender_addr Address of the sender
     */
    void handleDataMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr);
    
    /**
     * Handle incoming acknowledgment message
     * @param msg The ACK message
     */
    void handleAckMessage(const PLMessage& msg);
    
    /**
     * Send an ACK message to acknowledge receipt of a DATA message
     * @param sender_id ID of the process that sent the original message
     * @param sequence_number Sequence number of the message being acknowledged
     */
    void sendAck(uint8_t sender_id, uint32_t sequence_number);
    
    /**
     * Main retransmission loop - runs in separate thread
     */
    void retransmissionLoop();
    
    /**
     * Clean up old delivered messages that have been persisted to disk
     * This method safely removes old sequence numbers from delivered_messages_
     * while preserving recent ones needed for duplicate detection
     * @param sender_id The sender ID to clean up (if 0, clean up all senders)
     */
    void cleanupDeliveredMessages(uint8_t sender_id = 0);
    
    /**
     * Check if cleanup should be triggered based on delivered_messages_ size
     * @return true if cleanup is needed
     */
    bool shouldCleanupDeliveredMessages();
    
    /**
     * Helper method to clean up delivered messages for a specific sender
     * This method assumes delivered_messages_mutex_ is already locked
     * @param sender_id The sender ID to clean up
     * @param seq_set Reference to the sequence number set for this sender
     */
    void cleanupSenderDeliveredMessages(uint8_t sender_id, std::set<uint32_t>& seq_set);
    
    // Prevent copying
    PerfectLinks(const PerfectLinks&) = delete;
    PerfectLinks& operator=(const PerfectLinks&) = delete;
};

    /**
     * Write all logged events to output file in chronological order
     */
    void writeLogsToFile();