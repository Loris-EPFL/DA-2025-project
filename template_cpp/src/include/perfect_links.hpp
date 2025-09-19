#pragma once

#include "parser.hpp"
#include "pl_message.hpp"
#include <vector>
#include <thread>
#include <mutex>
#include <map>
#include <set>
#include <fstream>
#include <chrono>
#include <atomic>
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
     * Constructor
     * @param process_id The ID of this process
     * @param hosts List of all processes in the system
     * @param output_path Path to the output file for logging
     */
    PerfectLinks(uint8_t process_id, const std::vector<Parser::Host>& hosts, 
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
    std::vector<Parser::Host> hosts_;
    std::string output_path_;
    int socket_fd_;
    std::atomic<uint32_t> next_sequence_number_;
    std::atomic<bool> running_;
    
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
    
    std::map<std::pair<uint8_t, uint32_t>, PendingMessage> pending_messages_;
    std::mutex pending_messages_mutex_;
    
    // Delivery tracking
    std::map<uint8_t, uint32_t> expected_sequence_numbers_;
    std::map<uint8_t, std::set<uint32_t>> delivered_messages_;
    std::mutex delivered_messages_mutex_;
    
    // Output handling
    std::ofstream output_file_;
    std::mutex output_mutex_;
    
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
     * Retransmission loop - runs in separate thread
     */
    void retransmissionLoop();
    
    /**
     * Log a broadcast event
     * @param sequence_number The sequence number of the broadcast message
     */
    void logBroadcast(uint32_t sequence_number);
    
    /**
     * Log a delivery event
     * @param sender_id ID of the message sender
     * @param sequence_number Sequence number of the delivered message
     */
    void logDelivery(uint32_t sender_id, uint32_t sequence_number);
    
    // Prevent copying
    PerfectLinks(const PerfectLinks&) = delete;
    PerfectLinks& operator=(const PerfectLinks&) = delete;
};