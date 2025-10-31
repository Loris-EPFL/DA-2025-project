#pragma once

#include "parser.hpp"
#include "pl_message.hpp"
#include "batch_message.hpp"
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <atomic>
#include <fstream>
#include <functional>
#include <map>
#include <unordered_set>

#include <sys/socket.h>
#include <netinet/in.h>



/**
 * Perfect Links implementation
 */
class PerfectLinks {
public:
    /**
     * constructor with Parser given by TAs
     */
    PerfectLinks(Parser::Host localhost, std::function<void(uint32_t, uint32_t)> deliveryCallback, const std::vector<Parser::Host>& hosts, const std::string& output_path);
    
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
     * Send a message to a specific destination
     * @param destination_id ID of the destination process
     * @param payload The message payload to send
     */
    void send(uint8_t destination_id, const std::vector<uint8_t>& payload);
    
    /**
     * Send a message with integer payload (overloaded for convenience) //TODO only use the send vector of bytes method ?
     * @param destination_id ID of the destination process
     * @param message The integer message to send
     */
    void send(uint8_t destination_id, uint32_t message);
    
    /**
     * Broadcast a message to all other processes
     * @param payload The message payload to broadcast
     */
    void broadcast(const std::vector<uint8_t>& payload);
    
    /**
     * Broadcast an integer message to all other processes
     * @param message The integer message to broadcast
     */
    void broadcast(uint32_t message);

private:
    // Process variables
    uint8_t process_id_;
    Parser::Host localhost_;
    std::map<uint8_t, Parser::Host> id_to_peer_;
    std::function<void(uint32_t, uint32_t)> delivery_callback_;
    std::string output_path_;
    int socket_fd_;
    VectorClock local_vector_clock_; //Not used for now
    std::atomic<bool> running_;
    std::atomic<uint32_t> next_sequence_number_;
    
    // Timing constants for retransmission
    static constexpr std::chrono::milliseconds RETRANSMISSION_TIMEOUT{100};
    static constexpr std::chrono::milliseconds RETRANSMISSION_SLEEP{2};
    static constexpr std::chrono::milliseconds MAX_ADAPTIVE_TIMEOUT{1000};
    
    // Memory management constants for delivered_messages_ cleanup (In practise should never run for our current test, may be useful for super high applications with tons of seq id)
    static constexpr size_t DELIVERED_MESSAGES_CLEANUP_THRESHOLD = 50000;
    static constexpr size_t DELIVERED_MESSAGES_KEEP_RECENT = 10000;
    
    // Threading
    std::thread receiver_thread_;
    std::thread retransmission_thread_;
    
    // Message tracking
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
    
    // Track delivered messages to prevent duplicates: sender_id -> unordered_set of sequence numbers
    // Mutex protects against concurrent access from receive thread and cleanup operations
    std::map<uint8_t, std::unordered_set<uint32_t>> delivered_messages_;
    std::mutex delivered_messages_mutex_;
    
    // Batching for efficiency
    std::map<uint8_t, std::vector<PLMessage>> pending_batches_;
    std::mutex pending_batches_mutex_;
    std::map<uint8_t, std::chrono::steady_clock::time_point> last_batch_time_;
    static constexpr std::chrono::milliseconds BATCH_TIMEOUT{5};
    static constexpr size_t MAX_BATCH_SIZE = 8; // Max 8 messages per packet like in the ED post
    
    // Private helper methods
    
    //Individual version of send messages. Keep it in case batching doesn't work for some reason
    // /**
    //  * Send a message over UDP (method for individual messages one by one)
    //  * @param msg The message to send
    //  * @param destination The destination host
    //  */
    // void sendMessage(const PLMessage& msg, const Parser::Host& destination);
    
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
     * removes old sequence numbers from delivered_messages_
     * while keeping recent ones needed for duplicate detection
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
    void cleanupSenderDeliveredMessages(uint8_t sender_id, std::unordered_set<uint32_t>& seq_set);
    
    // Prevent copying
    PerfectLinks(const PerfectLinks&) = delete;
    PerfectLinks& operator=(const PerfectLinks&) = delete;
};