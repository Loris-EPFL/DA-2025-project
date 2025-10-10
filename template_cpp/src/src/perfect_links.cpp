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
      running_(false),
      next_sequence_number_(1) {  // Start sequence numbers from 1
    
    // Initialize local vector clock - set this process's clock to 0
    // (it will be incremented when sending first message)
}

// Destructor
PerfectLinks::~PerfectLinks() {
    stop();
}

// Initialize the Perfect Links system
bool PerfectLinks::initialize() {
    if (localhost_.port == 0 || localhost_.ip == 0) {
        std::cerr << "Invalid localhost configuration" << std::endl;
        return false;
    }
    
    // Create UDP socket
    socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd_ < 0) {
        std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
        return false;
    }
    
    // Set socket to non-blocking mode
    int flags = fcntl(socket_fd_, F_GETFL, 0);
    if (flags < 0) {
        std::cerr << "Failed to get socket flags: " << strerror(errno) << std::endl;
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }
    
    if (fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
        std::cerr << "Failed to set socket non-blocking: " << strerror(errno) << std::endl;
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }
    
    // Increase socket buffer sizes for high-load scenarios
    int send_buffer_size = 1024 * 1024;  // 1MB send buffer
    int recv_buffer_size = 1024 * 1024;  // 1MB receive buffer
    
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_SNDBUF, &send_buffer_size, sizeof(send_buffer_size)) < 0) {
        std::cerr << "Warning: Failed to set send buffer size: " << strerror(errno) << std::endl;
        // Continue anyway - this is not critical
    }
    
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &recv_buffer_size, sizeof(recv_buffer_size)) < 0) {
        std::cerr << "Warning: Failed to set receive buffer size: " << strerror(errno) << std::endl;
        // Continue anyway - this is not critical
    }
    
    // Bind socket to localhost
    struct sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = localhost_.ip;
    local_addr.sin_port = localhost_.port;
    
    if (bind(socket_fd_, reinterpret_cast<struct sockaddr*>(&local_addr), sizeof(local_addr)) < 0) {
        std::cerr << "Failed to bind socket to " << localhost_.ipReadable() 
                  << ":" << localhost_.portReadable() 
                  << " - " << strerror(errno) << std::endl;
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

// Send a message to a specific destination using Perfect Links
void PerfectLinks::send(uint8_t destination_id, const std::vector<uint8_t>& payload) {
    if (!running_) {
        return;
    }
    
    // Find destination host
    auto it = id_to_peer_.find(destination_id);
    if (it == id_to_peer_.end()) {
        return;  // Invalid destination
    }
    const Parser::Host& dest_host = it->second;
    
    // Get next sequence number
    uint32_t seq_num = next_sequence_number_.fetch_add(1);
    
    // Create message with updated vector clock
    local_vector_clock_.increment(process_id_);  // Increment our own clock
    PLMessage msg(static_cast<uint32_t>(process_id_), destination_id, seq_num, local_vector_clock_, MessageType::DATA, payload, true);
    
    // Store for retransmission
    {
        std::lock_guard<std::mutex> lock(pending_messages_mutex_);
        PendingMessage pending;
        pending.message = msg;
        pending.destination = dest_host;
        pending.last_sent = std::chrono::steady_clock::now();
        pending.ack_received = false;
        
        pending_messages_[std::make_pair(destination_id, seq_num)] = pending;
    }
    
    // Send immediately
    sendMessage(msg, dest_host);
}

// Convenience method: Send a message with integer payload
void PerfectLinks::send(uint8_t destination_id, uint32_t message) {
    // Convert integer to byte vector
    std::vector<uint8_t> payload(sizeof(uint32_t));
    std::memcpy(payload.data(), &message, sizeof(uint32_t));
    send(destination_id, payload);
}

// Broadcast a message to all other processes
void PerfectLinks::broadcast(const std::vector<uint8_t>& payload) {
    for (const auto& [id, host] : id_to_peer_) {
        if (id != process_id_) {
            send(static_cast<uint8_t>(id), payload);
        }
    }
}

// Convenience method: Broadcast a message with integer payload
void PerfectLinks::broadcast(uint32_t message) {
    // Convert integer to byte vector
    std::vector<uint8_t> payload(sizeof(uint32_t));
    std::memcpy(payload.data(), &message, sizeof(uint32_t));
    broadcast(payload);
}

// Private helper methods

void PerfectLinks::sendMessage(const PLMessage& msg, const Parser::Host& destination) {
    if (socket_fd_ < 0 || !running_) {
        return;  // Socket not initialized or system stopped
    }
    
    struct sockaddr_in dest_addr;
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_addr.s_addr = destination.ip;
    dest_addr.sin_port = destination.port;
    
    // Serialize message to buffer
    std::vector<uint8_t> buffer;
    try {
        size_t msg_size = msg.serialize(buffer);
        
        if (msg_size == 0 || msg_size > 65536) {  // Sanity check
            std::cerr << "Invalid message size: " << msg_size << std::endl;
            return;
        }
        
        ssize_t sent = sendto(socket_fd_, buffer.data(), msg_size, 0, 
                             reinterpret_cast<struct sockaddr*>(&dest_addr), sizeof(dest_addr));
        
        if (sent < 0) {
            if (errno != EAGAIN) {
                std::cerr << "Failed to send message: " << strerror(errno) << std::endl;
            }
        } else if (static_cast<size_t>(sent) != msg_size) {
            std::cerr << "Partial send: " << sent << " of " << msg_size << " bytes" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error serializing message: " << e.what() << std::endl;
    }
}

void PerfectLinks::receiveLoop() {
    const size_t MAX_MESSAGE_SIZE = 65536;  // 64KB max message size
    std::vector<uint8_t> buffer(MAX_MESSAGE_SIZE);
    struct sockaddr_in sender_addr;
    socklen_t addr_len = sizeof(sender_addr);
    
    while (running_) {
        if (socket_fd_ < 0) {
            std::cerr << "Socket closed during receive loop" << std::endl;
            break;
        }
        
        ssize_t received = recvfrom(socket_fd_, buffer.data(), buffer.size(), 0,
                                   reinterpret_cast<struct sockaddr*>(&sender_addr), &addr_len);
        
        if (received < 0) {
            if (errno == EAGAIN) {
                // Non-blocking socket, no data available
                std::this_thread::sleep_for(std::chrono::microseconds(500));  // Increased from 100 microseconds for better stability
                continue;
            } else if (errno == EINTR) {
                // Interrupted by signal, continue
                continue;
            } else {
                std::cerr << "Failed to receive message: " << strerror(errno) << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
        }
        
        if (received > 0) {
            try {
                PLMessage msg;
                if (msg.deserialize(buffer.data(), static_cast<size_t>(received))) {
                    handleMessage(msg, sender_addr);
                } else {
                    std::cerr << "Failed to deserialize message of size " << received << std::endl;
                }
            } catch (const std::exception& e) {
                std::cerr << "Error handling received message: " << e.what() << std::endl;
            }
        }
    }
}

void PerfectLinks::handleMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr) {
    if (msg.message_type == MessageType::DATA) {  // DATA message
        handleDataMessage(msg, sender_addr);
    } else if (msg.message_type == MessageType::ACK) {  // ACK message
        handleAckMessage(msg);
    }
}

void PerfectLinks::handleDataMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr) {
    uint8_t sender_id = static_cast<uint8_t>(msg.sender_id);
    uint32_t seq_num = msg.sequence_number;
    const VectorClock& msg_clock = msg.vector_clock;
    
    // Update our local vector clock with the received message's clock
    // This needs to be thread-safe as multiple threads might access it
    {
        std::lock_guard<std::mutex> clock_lock(delivered_messages_mutex_);  // Reuse mutex for simplicity
        local_vector_clock_.update(msg_clock);
    }
    
    // Check if we've already delivered this message
    bool already_delivered = false;
    {
        std::lock_guard<std::mutex> lock(delivered_messages_mutex_);
        already_delivered = (delivered_messages_[sender_id].count(seq_num) > 0);
    }
    
    if (already_delivered) {
        // Already delivered, just send ACK
        PLMessage ack_msg(static_cast<uint32_t>(process_id_), sender_id, seq_num, msg_clock, MessageType::ACK, false);
        
        // Find sender host using O(1) lookup
        auto it = id_to_peer_.find(sender_id);
        if (it != id_to_peer_.end()) {
            sendMessage(ack_msg, it->second);
        }
        return;
    }
    
    // Send ACK
    PLMessage ack_msg(static_cast<uint32_t>(process_id_), sender_id, seq_num, msg_clock, MessageType::ACK, false);
    
    // Find sender host using O(1) lookup
    auto it = id_to_peer_.find(sender_id);
    if (it != id_to_peer_.end()) {
        sendMessage(ack_msg, it->second);
        
        // Mark as delivered and log
        {
            std::lock_guard<std::mutex> lock(delivered_messages_mutex_);
            delivered_messages_[sender_id].insert(seq_num);
        }
        
        // Use delivery callback - use sequence number for logging
        // Callback should be thread-safe as it's called from multiple threads
        if (delivery_callback_) {
            try {
                delivery_callback_(sender_id, seq_num);
            } catch (const std::exception& e) {
                std::cerr << "Error in delivery callback: " << e.what() << std::endl;
            }
        }
    }
}

void PerfectLinks::handleAckMessage(const PLMessage& msg) {
    uint8_t sender_id = static_cast<uint8_t>(msg.sender_id);
    uint32_t seq_num = msg.sequence_number;
    
    // Mark the corresponding message as acknowledged
    std::lock_guard<std::mutex> lock(pending_messages_mutex_);
    auto it = pending_messages_.find(std::make_pair(sender_id, seq_num));
    if (it != pending_messages_.end()) {
        it->second.ack_received = true;
    }
    // Note: If message not found, it might have been already cleaned up by retransmission thread
}

void PerfectLinks::retransmissionLoop() {
    const auto timeout = std::chrono::milliseconds(1000);  // 1000ms timeout for better reliability under stress
    
    while (running_) {
        auto now = std::chrono::steady_clock::now();
        
        // Process retransmissions and cleanup in a single critical section
        // to avoid race conditions between retransmission and ACK handling
        {
            std::lock_guard<std::mutex> lock(pending_messages_mutex_);
            
            // First pass: retransmit messages that need it
            for (auto& pair : pending_messages_) {
                PendingMessage& pending = pair.second;
                
                if (!pending.ack_received && 
                    (now - pending.last_sent) > timeout) {
                    
                    // Retransmit
                    sendMessage(pending.message, pending.destination);
                    pending.last_sent = now;
                }
            }
            
            // Second pass: clean up acknowledged messages
            auto it = pending_messages_.begin();
            while (it != pending_messages_.end()) {
                if (it->second.ack_received) {
                    it = pending_messages_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        
        // Sleep outside the critical section to reduce lock contention
        std::this_thread::sleep_for(std::chrono::milliseconds(10));  // Increased back to 10ms for stability
    }
}