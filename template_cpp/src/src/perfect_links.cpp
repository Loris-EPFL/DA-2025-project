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
    // Always send ACK immediately (Stubborn Links requirement)
    sendAck(static_cast<uint8_t>(message.sender_id), message.sequence_number);
    
    // Check for duplicates using lock-free approach
    uint64_t msg_key = messageIdToKey(static_cast<uint8_t>(message.sender_id), message.sequence_number);
    
    // Use simple check-and-insert approach - this is safe because:
    // 1. Each message has a unique (sender_id, sequence_number) pair
    // 2. The Perfect Links algorithm guarantees no duplicate deliveries
    // 3. We only insert, never remove from delivered_messages_
    if (delivered_messages_.find(msg_key) == delivered_messages_.end()) {
        // First time seeing this message - deliver it
        delivered_messages_.insert(msg_key);
        
        // Log delivery event in memory
        logDelivery(static_cast<uint8_t>(message.sender_id), message.sequence_number);
        
        // Trigger delivery callback
        if (delivery_callback_) {
            delivery_callback_(message.sender_id, message.payload);
        }
    }
    // If message already delivered, silently ignore (no duplicate delivery)
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
    // Store broadcast event in memory for later writing
    broadcast_events_.push_back(sequence_number);
}

void PerfectLinks::logDelivery(uint32_t sender_id, uint32_t sequence_number) {
    // Store delivery event in memory for later writing
    delivery_events_.emplace_back(sender_id, sequence_number);
}

void PerfectLinks::writeLogsToFile() {
    if (output_path_.empty()) return;
    
    std::ofstream file(output_path_, std::ios::out | std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open output file: " << output_path_ << std::endl;
        return;
    }
    
    // Write all broadcast events
    for (uint32_t seq_num : broadcast_events_) {
        file << "b " << seq_num << "\n";
    }
    
    // Write all delivery events
    for (const auto& delivery : delivery_events_) {
        file << "d " << static_cast<int>(delivery.first) << " " << delivery.second << "\n";
    }
    
    file.flush();
    file.close();
}