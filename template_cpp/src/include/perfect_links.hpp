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
#include <chrono>
#include <functional>
#include <set>
#include <sys/socket.h>
#include <netinet/in.h>



/**
 * 
 * We need to ensure the perfect links properties:
 * - PL1 (Reliable delivery): If a correct process p sends a message m to a correct process q, then q eventually delivers m
 * - PL2 (No duplication): No message is delivered by a process more than once
 * - PL3 (No creation): If a process q delivers a message m with sender p, then m was previously sent to q by process p
 */
class PerfectLinks {
public:
    /**
     * constructor with Parser given by TAs
     */
    PerfectLinks(Parser::Host localhost,
                std::function<void(uint32_t, uint32_t)> deliveryCallback,
                std::map<uint8_t, Parser::Host> idToPeer,
                const std::string& output_path);
    

    
    /**
     * Destructor
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
     * @param payload The message payload to send (we use a vector of uint8_t for arbitrary bytes)
     */
    void send(uint8_t destination_id, const std::vector<uint8_t>& payload);
    
    /**
     * Overlaoded method: Send a message with integer payload (just converts to vector of bytes and call above send)
     * @param destination_id ID of the destination process
     * @param message The integer message to send
     */
    void send(uint8_t destination_id, uint32_t message);
    
    /**
     * Broadcast a message to all other processes
     * @param payload The message payload to broadcast (we use a vector of uint8_t for arbitrary bytes)
     */
    void broadcast(const std::vector<uint8_t>& payload);
    
    /**
     * Convenience method: Broadcast an integer message to all other processes
     * @param message The integer message to broadcast
     */
    void broadcast(uint32_t message);

private:
    // Process variables
    uint8_t process_id_;
    Parser::Host localhost_;
    std::map<uint8_t, Parser::Host> id_to_peer_; // Map of process ID to peer host information, consistent uint8_t type
    std::function<void(uint32_t, uint32_t)> delivery_callback_; // Callback after message delivery, useful for logging
    std::string output_path_;
    int socket_fd_;
    VectorClock local_vector_clock_;  // Local vector clock for this process (kept for future causal ordering features)
    std::atomic<bool> running_;
    std::atomic<uint32_t> next_sequence_number_;  // Protocol-managed sequence numbers
    
    // Timing constants for retransmission
    static constexpr std::chrono::milliseconds RETRANSMISSION_TIMEOUT{100};  // Timeout after which we retransmit a message
    static constexpr std::chrono::milliseconds RETRANSMISSION_SLEEP{2};      // Pause between retransmission attempts
    static constexpr std::chrono::milliseconds MAX_ADAPTIVE_TIMEOUT{1000};   // Under the tc.py script, augment retransmission timeout to 1000ms to avoid timeout
    
    // Memory management constants for delivered_messages_ cleanup
    // These values balance memory usage vs. duplicate detection capability
    static constexpr size_t DELIVERED_MESSAGES_CLEANUP_THRESHOLD = 50000;  // Cleanup when we have this many delivered messages per sender (prevents unbounded growth)
    static constexpr size_t DELIVERED_MESSAGES_KEEP_RECENT = 10000;        // Keep this many recent sequence numbers per sender after cleanup (maintains duplicate detection window)
    
    // Threading
    std::thread receiver_thread_;
    std::thread retransmission_thread_;
    
    // Message tracking - using nested maps for consistent structure and efficient operations
    struct PendingMessage {
        PLMessage message;
        Parser::Host destination;
        std::chrono::steady_clock::time_point last_sent;
        bool ack_received;
        uint32_t retransmit_count;  // Track retransmission attempts for adaptive timeout
        
        PendingMessage() : ack_received(false), retransmit_count(0) {}
    };
    
    // Track pending messages: sender_id -> (seq_num -> PendingMessage)
    // Mutex protects against concurrent access from send/retransmission threads
    std::map<uint8_t, std::map<uint32_t, PendingMessage>> pending_messages_;
    std::mutex pending_messages_mutex_;
    
    // Track delivered messages to prevent duplicates: sender_id -> set of sequence numbers
    // Mutex protects against concurrent access from receive thread and cleanup operations
    std::map<uint8_t, std::set<uint32_t>> delivered_messages_;
    std::mutex delivered_messages_mutex_;
    
    // Message batching support for 8 messages per packet
    std::map<uint8_t, std::vector<PLMessage>> pending_batches_;  // destination_id -> batch of messages
    std::mutex pending_batches_mutex_;
    // Per-destination batch timing (moved from global to per-destination for correctness)
    std::map<uint8_t, std::chrono::steady_clock::time_point> last_batch_time_;
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