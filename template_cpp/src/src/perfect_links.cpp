#include "perfect_links.hpp"
#include "host_utils.hpp"
#include <iostream>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>

// Modern constructor using Parser-based approach
PerfectLinks::PerfectLinks(Parser::Host localhost,
                          std::function<void(uint32_t, uint32_t)> deliveryCallback,
                          std::map<unsigned long, Parser::Host> idToPeer,
                          const std::string& output_path)
    : process_id_(static_cast<uint8_t>(localhost.id)), 
      localhost_(localhost),
      id_to_peer_(std::move(idToPeer)),
      delivery_callback_(std::move(deliveryCallback)),
      output_path_(output_path),
      socket_fd_(-1), 
      local_vector_clock_(),
      running_(false) {
    
    // Initialize local vector clock - set this process's clock to 0
    // (it will be incremented when sending first message)
}

void PerfectLinks::sendAck(uint8_t sender_id, uint32_t sequence_number) {
    // Create ACK message - the ACK should contain the original sender_id and sequence_number
    // so the sender can match it with their pending message
    PLMessage ack_msg;
    ack_msg.sender_id = sender_id;  // Original sender's ID (for matching)
    ack_msg.peer_id = static_cast<uint32_t>(process_id_);  // Who is sending the ACK
    ack_msg.sequence_number = sequence_number;  // Original sequence number
    ack_msg.message_type = MessageType::ACK;
    ack_msg.payload = 0;
    ack_msg.ack_required = false;
    
    // Send ACK to the original sender
    auto it = id_to_peer_.find(sender_id);
    if (it != id_to_peer_.end()) {
        sendMessage(ack_msg, it->second);
    }
}

// Destructor
PerfectLinks::~PerfectLinks() {
    stop();
    // Write all logs to file before destruction
    writeLogsToFile();
}

// Initialize the Perfect Links system
bool PerfectLinks::initialize() {
    // Create UDP socket
    socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd_ < 0) {
        std::cerr << "Failed to create socket: " << std::strerror(errno) << std::endl;
        return false;
    }
    
    // Make socket non-blocking
    int flags = fcntl(socket_fd_, F_GETFL, 0);
    if (flags < 0 || fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
        std::cerr << "Failed to set socket non-blocking: " << std::strerror(errno) << std::endl;
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }
    
    // Bind to localhost port
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = localhost_.ip;
    addr.sin_port = localhost_.port;
    
    if (bind(socket_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "Failed to bind socket to " << localhost_.ipReadable() 
                  << ":" << localhost_.portReadable() 
                  << " - " << std::strerror(errno) << std::endl;
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }
    
    std::cout << "Perfect Links initialized on " << localhost_.ipReadable() 
              << ":" << localhost_.portReadable() << std::endl;
    
    return true;
}

// Start the Perfect Links system
void PerfectLinks::start() {
    if (socket_fd_ < 0) {
        std::cerr << "Perfect Links not initialized" << std::endl;
        return;
    }
    
    running_ = true;
    
    // Start receiver thread
    receiver_thread_ = std::thread(&PerfectLinks::receiveLoop, this);
    
    // Start retransmission thread
    retransmission_thread_ = std::thread(&PerfectLinks::retransmissionLoop, this);
    
    std::cout << "Perfect Links started" << std::endl;
}

// Stop the Perfect Links system
void PerfectLinks::stop() {
    running_ = false;
    
    if (receiver_thread_.joinable()) {
        receiver_thread_.join();
    }
    
    if (retransmission_thread_.joinable()) {
        retransmission_thread_.join();
    }
    
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
    
    std::cout << "Perfect Links stopped" << std::endl;
}

// Send a message using Perfect Links
void PerfectLinks::send(uint8_t destination_id, uint32_t message) {
    if (!running_ || socket_fd_ < 0) {
        std::cerr << "Perfect Links not running" << std::endl;
        return;
    }
    
    // Create message with sequence number
    PLMessage msg;
    msg.sender_id = static_cast<uint32_t>(process_id_);
    msg.peer_id = destination_id;
    msg.sequence_number = message;  // Use message as sequence number for MS1
    msg.message_type = MessageType::DATA;
    msg.payload = message;
    msg.ack_required = true;
    
    // Find destination host
    auto it = id_to_peer_.find(destination_id);
    if (it == id_to_peer_.end()) {
        return; // Invalid destination
    }
    
    // Log broadcast
    logBroadcast(message);
    
    // Add to pending messages for retransmission (Stubborn Links)
    MessageId msg_id = {static_cast<uint8_t>(process_id_), message};
    PendingMessage pending;
    pending.message = msg;
    pending.destination = it->second;
    pending.last_sent = std::chrono::steady_clock::now();
    pending.ack_received.store(false);  // Use atomic store
    
    // Insert into pending messages (this is the only place we add, so minimal locking needed)
    pending_messages_[msg_id] = std::move(pending);
    
    // Send the message immediately (Stubborn Links will handle retransmission)
    sendMessage(msg, it->second);
}

// Broadcast a message to all other processes
void PerfectLinks::broadcast(uint32_t message) {
    for (const auto& [id, host] : id_to_peer_) {
        if (id != process_id_) {
            send(static_cast<uint8_t>(id), message);
        }
    }
}

// Private helper methods

void PerfectLinks::sendMessage(const PLMessage& msg, const Parser::Host& destination) {
    struct sockaddr_in dest_addr;
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_addr.s_addr = destination.ip;
    dest_addr.sin_port = destination.port;
    
    ssize_t sent = sendto(socket_fd_, &msg, sizeof(msg), 0, 
                         reinterpret_cast<struct sockaddr*>(&dest_addr), sizeof(dest_addr));
    
    if (sent < 0 && errno != EAGAIN) {
        std::cerr << "Failed to send message: " << strerror(errno) << std::endl;
    }
}

void PerfectLinks::receiveLoop() {
    PLMessage msg;
    struct sockaddr_in sender_addr;
    socklen_t addr_len = sizeof(sender_addr);
    
    while (running_) {
        ssize_t received = recvfrom(socket_fd_, &msg, sizeof(msg), 0,
                                   reinterpret_cast<struct sockaddr*>(&sender_addr), &addr_len);
        
        if (received < 0) {
            if (errno != EAGAIN) {
                std::cerr << "Failed to receive message: " << strerror(errno) << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        
        if (received == sizeof(msg)) {
            handleMessage(msg, sender_addr);
        }
    }
}

void PerfectLinks::handleMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr) {
    if (msg.message_type == MessageType::DATA) {
        handleDataMessage(msg, sender_addr);
    } else if (msg.message_type == MessageType::ACK) {
        handleAckMessage(msg);
    }
}

void PerfectLinks::handleDataMessage(const PLMessage& message, const sockaddr_in& sender_addr) {
    // Eliminate Duplicates Algorithm: Check if message already delivered
    MessageId msg_id = {static_cast<uint8_t>(message.sender_id), message.sequence_number};
    
    // Always send ACK first (even for duplicates) - this is crucial for reliability
    sendAck(static_cast<uint8_t>(message.sender_id), message.sequence_number);
    
    // Check for duplicate delivery (PL2: No Duplication)
    // Use atomic operations to avoid mutex in core algorithm
    bool already_delivered = false;
    {
        std::lock_guard<std::mutex> lock(delivered_mutex_);
        if (delivered_messages_.find(msg_id) != delivered_messages_.end()) {
            already_delivered = true;
        } else {
            // Mark as delivered to prevent future duplicates
            delivered_messages_.insert(msg_id);
        }
    }
    
    // Only deliver if not a duplicate
    if (!already_delivered) {
        // Deliver the message (PL1: Reliable Delivery)
        logDelivery(message.sender_id, message.sequence_number);
        
        // Call delivery callback if set
        if (delivery_callback_) {
            delivery_callback_(message.sender_id, message.payload);
        }
    }
}

void PerfectLinks::handleAckMessage(const PLMessage& message) {
    // The ACK message contains the original sender_id and sequence_number
    // Find and mark the corresponding pending message as acknowledged
    MessageId msg_id = {static_cast<uint8_t>(message.sender_id), message.sequence_number};
    
    // Use atomic operation to mark as acknowledged (lock-free)
    auto it = pending_messages_.find(msg_id);
    if (it != pending_messages_.end()) {
        it->second.ack_received.store(true);
        // Message will be cleaned up in retransmissionLoop
    }
}

void PerfectLinks::retransmissionLoop() {
    // Stubborn Links Algorithm: "Retransmit Forever"
    // Keep retransmitting all pending messages until ACK received
    while (running_) {
        // Retransmit every 100ms (reasonable for network conditions)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // Create a copy of pending messages to avoid holding locks during transmission
        std::vector<std::pair<MessageId, PendingMessage>> messages_to_retransmit;
        
        // Collect messages that need retransmission (lock-free read where possible)
        for (const auto& [msg_id, pending] : pending_messages_) {
            if (!pending.ack_received.load()) {
                messages_to_retransmit.push_back({msg_id, pending});
            }
        }
        
        // Retransmit all pending messages (Stubborn Links property)
        for (const auto& [msg_id, pending] : messages_to_retransmit) {
            if (!pending.ack_received.load()) {
                sendMessage(pending.message, pending.destination);
            }
        }
        
        // Clean up acknowledged messages
        for (auto it = pending_messages_.begin(); it != pending_messages_.end();) {
            if (it->second.ack_received.load()) {
                it = pending_messages_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void PerfectLinks::logBroadcast(uint32_t sequence_number) {
    std::lock_guard<std::mutex> lock(log_entries_mutex_);
    log_entries_.emplace_back('b', process_id_, sequence_number);
}

void PerfectLinks::logDelivery(uint32_t sender_id, uint32_t sequence_number) {
    std::lock_guard<std::mutex> lock(log_entries_mutex_);
    log_entries_.emplace_back('d', sender_id, sequence_number);
}

void PerfectLinks::writeLogsToFile() {
    std::lock_guard<std::mutex> lock(log_entries_mutex_);
    
    if (log_entries_.empty()) {
        return;
    }
    
    // Sort log entries by timestamp to ensure chronological order
    std::sort(log_entries_.begin(), log_entries_.end(), 
              [](const LogEntry& a, const LogEntry& b) {
                  return a.timestamp < b.timestamp;
              });
    
    // Write to output file
    std::ofstream output_file(output_path_);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open output file: " << output_path_ << std::endl;
        return;
    }
    
    for (const auto& entry : log_entries_) {
        if (entry.type == 'b') {
            output_file << "b " << entry.sequence_number << std::endl;
        } else if (entry.type == 'd') {
            output_file << "d " << entry.sender_id << " " << entry.sequence_number << std::endl;
        }
    }
    
    output_file.close();
}