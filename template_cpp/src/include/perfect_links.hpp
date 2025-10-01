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
#include <chrono>
#include <functional>
#include <set>
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
    void broadcast(uint32_t message);

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
    
    // In-memory logging for better performance
    struct LogEntry {
        char type;  // 'b' for broadcast, 'd' for delivery
        uint32_t sender_id;  // For delivery events
        uint32_t sequence_number;
        std::chrono::steady_clock::time_point timestamp;
        
        LogEntry(char t, uint32_t sender, uint32_t seq) 
            : type(t), sender_id(sender), sequence_number(seq), 
              timestamp(std::chrono::steady_clock::now()) {}
    };
    
    std::vector<LogEntry> log_entries_;
    std::mutex log_entries_mutex_;
    
    // Threading
    std::thread receiver_thread_;
    std::thread retransmission_thread_;
    
    // Message tracking
    struct PendingMessage {
        PLMessage message;
        Parser::Host destination;
        std::chrono::steady_clock::time_point last_sent;
        bool ack_received;
        
        PendingMessage() : ack_received(false) {}
    };
    
    std::map<std::pair<uint8_t, VectorClock>, PendingMessage> pending_messages_;
    std::mutex pending_messages_mutex_;
    
    // Delivery tracking - using vector clocks for ordering
    std::map<uint8_t, std::set<VectorClock>> delivered_messages_;
    std::mutex delivered_messages_mutex_;
    
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
     * Main retransmission loop - runs in separate thread
     */
    void retransmissionLoop();
    
    // Prevent copying
    PerfectLinks(const PerfectLinks&) = delete;
    PerfectLinks& operator=(const PerfectLinks&) = delete;
};

    /**
     * Write all logged events to output file in chronological order
     */
    void writeLogsToFile();
    
    /**
     * Log a broadcast event in memory
     */
    void logBroadcast(uint32_t sequence_number);
    
    /**
     * Log a delivery event in memory
     */
    void logDelivery(uint32_t sender_id, uint32_t sequence_number);