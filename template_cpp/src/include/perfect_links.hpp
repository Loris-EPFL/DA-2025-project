#pragma once

#include "parser.hpp"
#include "pl_message.hpp"
#include <functional>
#include <map>
#include <vector>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <fstream>
#include <string>

// Structure to track pending messages for retransmission
struct PendingMessage {
    PLMessage message;
    Parser::Host destination;
    std::chrono::steady_clock::time_point last_sent;
    int retransmission_count;
    
    PendingMessage() : retransmission_count(0) {
        last_sent = std::chrono::steady_clock::now();
    }
    
    PendingMessage(const PLMessage& msg, const Parser::Host& dest) 
        : message(msg), destination(dest), retransmission_count(0) {
        last_sent = std::chrono::steady_clock::now();
    }
};

class PerfectLinks {
public:
    // Constructor
    PerfectLinks(Parser::Host localhost,
                std::function<void(uint32_t, uint32_t)> deliveryCallback,
                std::map<unsigned long, Parser::Host> idToPeer,
                const std::string& output_path);
    
    // Destructor
    ~PerfectLinks();
    
    // Core interface methods
    bool initialize();
    void start();
    void stop();
    void send(uint8_t destination_id, uint32_t message);
    void broadcast(uint32_t message);
    void writeLogsToFile();

private:
    // Core member variables
    uint8_t process_id_;
    Parser::Host localhost_;
    std::map<unsigned long, Parser::Host> id_to_peer_;
    std::function<void(uint32_t, uint32_t)> delivery_callback_;
    std::string output_path_;
    
    // Network components
    int socket_fd_;
    
    // Vector clock for message ordering
    VectorClock local_vector_clock_;
    
    // Threading components
    std::atomic<bool> running_;
    std::thread receiver_thread_;
    std::thread retransmission_thread_;
    
    // Message tracking with thread safety
    mutable std::mutex delivered_messages_mutex_;
    std::unordered_set<std::string> delivered_messages_set_;  // Fast O(1) lookup
    
    std::mutex pending_messages_mutex_;
    std::map<std::string, PendingMessage> pending_messages_;
    
    // Logging components with thread safety
    mutable std::mutex logging_mutex_;
    std::vector<uint32_t> broadcast_events_;
    std::vector<std::pair<uint32_t, uint32_t>> delivery_events_;
    
    // Helper methods
    std::string createMessageKey(const PLMessage& message) const {
        // Fixed key creation - use sender_id and peer_id correctly
        return std::to_string(message.sender_id) + "_" + 
               std::to_string(message.peer_id) + "_" + 
               std::to_string(message.sequence_number) + "_" + 
               std::to_string(message.payload);
    }
    
    void sendMessage(const PLMessage& msg, const Parser::Host& destination);
    void receiveLoop();
    void handleMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr);
    void handleDataMessage(const PLMessage& message, const struct sockaddr_in& sender_addr);
    void handleAckMessage(const PLMessage& message);
    void sendAck(const PLMessage& original_message, const struct sockaddr_in& sender_addr);
    void retransmissionLoop();
    bool isMessageDelivered(const PLMessage& message) const;
    void logBroadcast(uint32_t sequence_number);
    void logDelivery(uint32_t sender_id, uint32_t sequence_number);
};