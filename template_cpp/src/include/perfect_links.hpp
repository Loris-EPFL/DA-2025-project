#pragma once

#include "parser.hpp"
#include "pl_message.hpp"
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
     * @param message The message payload to send
     */
    void send(uint8_t destination_id, uint32_t message);
    
    /**
     * Broadcast a message to all other processes
     * @param message The message payload to broadcast
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
    
    // Lock-free in-memory storage for broadcast and delivery events
    std::vector<uint32_t> broadcast_events_;  // Only this process's broadcasts
    std::vector<std::pair<uint8_t, uint32_t>> delivery_events_;   // All deliveries to this process
    
    // Lock-free delivery tracking using simple set
    std::unordered_set<uint64_t> delivered_messages_;  // Simple set for delivered message tracking
    
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
        std::atomic<bool> ack_received;
        uint32_t retransmission_count;
        
        PendingMessage() : ack_received(false), retransmission_count(0) {}
        
        // Copy constructor for atomic member
        PendingMessage(const PendingMessage& other) 
            : message(other.message), destination(other.destination), 
              last_sent(other.last_sent), ack_received(other.ack_received.load()),
              retransmission_count(other.retransmission_count) {}
        
        // Assignment operator for atomic member
        PendingMessage& operator=(const PendingMessage& other) {
            if (this != &other) {
                message = other.message;
                destination = other.destination;
                last_sent = other.last_sent;
                ack_received.store(other.ack_received.load());
                retransmission_count = other.retransmission_count;
            }
            return *this;
        }
    };
    
    std::map<MessageId, PendingMessage> pending_messages_;
    // Note: All mutexes removed - using atomic operations and lock-free algorithms
    
    // Helper function to convert MessageId to uint64_t for duplicate detection
    uint64_t messageIdToKey(uint8_t sender_id, uint32_t sequence_number) const {
        return (static_cast<uint64_t>(sender_id) << 32) | sequence_number;
    }
    
    // Private helper methods
    
    /**
     * Send a message over UDP
     * @param msg The message to send
     * @param destination The destination host
     */
    void sendMessage(const PLMessage& msg, const Parser::Host& destination);
    
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
     * Log a broadcast event in memory
     */
    void logBroadcast(uint32_t sequence_number);
    
    /**
     * Log a delivery event in memory
     */
    void logDelivery(uint32_t sender_id, uint32_t sequence_number);
    
    // Prevent copying
    PerfectLinks(const PerfectLinks&) = delete;
    PerfectLinks& operator=(const PerfectLinks&) = delete;
};

    /**
     * Write all logged events to output file in chronological order
     */
    void writeLogsToFile();