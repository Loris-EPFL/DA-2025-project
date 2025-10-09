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

void PerfectLinks::sendAck(const PLMessage& original_message, const sockaddr_in& sender_addr) {
    // Create ACK message preserving all original packet fields like reference implementation
    PLMessage ack_message;
    ack_message.sender_id = original_message.sender_id;
    ack_message.peer_id = original_message.peer_id;
    ack_message.sequence_number = original_message.sequence_number;
    ack_message.vector_clock = original_message.vector_clock;
    ack_message.message_type = MessageType::ACK;
    ack_message.payload = original_message.payload;
    ack_message.ack_required = false;
    
    // Send ACK directly using UDP socket
    ssize_t bytes_sent = sendto(socket_fd_, &ack_message, sizeof(ack_message), 0,
                               reinterpret_cast<const struct sockaddr*>(&sender_addr), sizeof(sender_addr));
    if (bytes_sent < 0) {
        std::cerr << "Failed to send ACK: " << strerror(errno) << std::endl;
    }
}

bool PerfectLinks::isMessageDelivered(const PLMessage& message) const {
    // Use fast string-based key lookup instead of linear search
    std::string key = createMessageKey(message);
    std::lock_guard<std::mutex> lock(delivered_messages_mutex_);
    return delivered_messages_set_.find(key) != delivered_messages_set_.end();
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
    
    // Set socket buffer sizes to handle high load
    int buffer_size = 1024 * 1024;  // 1MB buffer
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        std::cerr << "Warning: Failed to set receive buffer size: " << std::strerror(errno) << std::endl;
    }
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        std::cerr << "Warning: Failed to set send buffer size: " << std::strerror(errno) << std::endl;
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
    // Increment local vector clock for this process
    local_vector_clock_.increment(process_id_);
    
    // Create message with proper fields like reference implementation
    PLMessage msg;
    msg.sender_id = static_cast<uint32_t>(process_id_);
    msg.peer_id = static_cast<uint32_t>(destination_id);
    msg.sequence_number = message;  // Use message as sequence number
    msg.message_type = MessageType::DATA;
    msg.payload = message;
    msg.ack_required = true;
    msg.vector_clock = local_vector_clock_;
    
    // Find destination host
    auto peer_it = id_to_peer_.find(destination_id);
    if (peer_it == id_to_peer_.end()) {
        std::cerr << "Unknown destination ID: " << static_cast<int>(destination_id) << std::endl;
        return;
    }
    
    // Send message immediately
    sendMessage(msg, peer_it->second);
    
    // Add to pending messages for retransmission like reference implementation
    std::string key = createMessageKey(msg);
    
    std::lock_guard<std::mutex> lock(pending_messages_mutex_);
    pending_messages_[key] = PendingMessage(msg, peer_it->second);
    
    // Log broadcast event
    logBroadcast(message);
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
            std::this_thread::sleep_for(std::chrono::microseconds(100));  // Shorter sleep for better responsiveness
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

void PerfectLinks::handleDataMessage(const PLMessage& message, const struct sockaddr_in& sender_addr) {
    // Always send ACK first (even for duplicates) to prevent retransmissions
    sendAck(message, sender_addr);
    
    // Check if message was already delivered using fast set lookup
    std::string key = createMessageKey(message);
    bool deliveryReady = false;
    
    {
        std::lock_guard<std::mutex> lock(delivered_messages_mutex_);
        if (delivered_messages_set_.find(key) == delivered_messages_set_.end()) {
            // First time receiving this message - mark as delivered
            delivered_messages_set_.insert(key);
            deliveryReady = true;
        }
    }
    
    if (deliveryReady) {
        // Only deliver if it's the first time we receive this message
        // Avoid self-delivery like reference implementation
        if (!(message.peer_id == process_id_ && message.sender_id == process_id_)) {
            // Log delivery event
            logDelivery(message.sender_id, message.sequence_number);
            
            // Call delivery callback
            if (delivery_callback_) {
                delivery_callback_(message.sender_id, message.sequence_number);
            }
        }
    }
}

void PerfectLinks::handleAckMessage(const PLMessage& message) {
    // Create key for the ACK like reference implementation
    std::string key = createMessageKey(message);
    
    std::lock_guard<std::mutex> lock(pending_messages_mutex_);
    
    // Find and remove the pending message
    auto it = pending_messages_.find(key);
    if (it != pending_messages_.end()) {
        pending_messages_.erase(it);
    }
}

void PerfectLinks::retransmissionLoop() {
    while (running_) {
        // Sleep for retransmission interval (200ms for more aggressive retransmission under high load)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        
        if (!running_) {
            break;
        }
        
        // Copy pending messages to avoid holding lock during retransmission
        std::map<std::string, PendingMessage> toResend;
        {
            std::lock_guard<std::mutex> lock(pending_messages_mutex_);
            toResend = pending_messages_;
        }
        
        // Retransmit all pending messages like reference implementation
        unsigned long count = 0;
        for (auto& [key, pending_msg] : toResend) {
            if (!running_) {
                break;
            }
            
            count++;
            // Add small delay every 10 messages to avoid overwhelming network under high load
            if ((count % 10) == 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(500));  // Shorter delay
            }
            
            // Send the message again
            sendMessage(pending_msg.message, pending_msg.destination);
            
            // Update retransmission count
            {
                std::lock_guard<std::mutex> lock(pending_messages_mutex_);
                auto it = pending_messages_.find(key);
                if (it != pending_messages_.end()) {
                    it->second.retransmission_count++;
                    it->second.last_sent = std::chrono::steady_clock::now();
                }
            }
        }
    }
}

void PerfectLinks::logBroadcast(uint32_t sequence_number) {
    // Store broadcast event in memory for later writing (thread-safe)
    std::lock_guard<std::mutex> lock(logging_mutex_);
    broadcast_events_.push_back(sequence_number);
}

void PerfectLinks::logDelivery(uint32_t sender_id, uint32_t sequence_number) {
    // Store delivery event in memory for later writing (thread-safe)
    std::lock_guard<std::mutex> lock(logging_mutex_);
    delivery_events_.emplace_back(sender_id, sequence_number);
}

void PerfectLinks::writeLogsToFile() {
    if (output_path_.empty()) return;
    
    std::ofstream file(output_path_, std::ios::out | std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open output file: " << output_path_ << std::endl;
        return;
    }
    
    // Thread-safe access to logging data
    std::lock_guard<std::mutex> lock(logging_mutex_);
    
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