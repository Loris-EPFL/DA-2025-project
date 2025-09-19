#include "perfect_links.hpp"
#include <iostream>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>

// Constructor
PerfectLinks::PerfectLinks(uint8_t process_id, const std::vector<Parser::Host>& hosts, 
                          const std::string& output_path)
    : process_id_(process_id), hosts_(hosts), output_path_(output_path), 
      socket_fd_(-1), next_sequence_number_(1), running_(false) {
    
    // Initialize expected sequence numbers for each host
    for (const auto& host : hosts_) {
        expected_sequence_numbers_[static_cast<uint8_t>(host.id)] = 1;
        delivered_messages_[static_cast<uint8_t>(host.id)] = std::set<uint32_t>();
    }
    
    // Open output file
    output_file_.open(output_path_, std::ios::out | std::ios::trunc);
    if (!output_file_.is_open()) {
        throw std::runtime_error("Failed to open output file: " + output_path_);
    }
}

// Destructor
PerfectLinks::~PerfectLinks() {
    stop();
    if (output_file_.is_open()) {
        output_file_.close();
    }
}

// Initialize the Perfect Links system
bool PerfectLinks::initialize() {
    try {
        // Create UDP socket
        socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        if (socket_fd_ < 0) {
            std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
            return false;
        }
        
        // Set socket to non-blocking mode
        int flags = fcntl(socket_fd_, F_GETFL, 0);
        if (flags < 0 || fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
            std::cerr << "Failed to set socket to non-blocking: " << strerror(errno) << std::endl;
            close(socket_fd_);
            socket_fd_ = -1;
            return false;
        }
        
        // Find our host information
        Parser::Host my_host;
        bool found = false;
        for (const auto& host : hosts_) {
            if (host.id == process_id_) {
                my_host = host;
                found = true;
                break;
            }
        }
        
        if (!found) {
            std::cerr << "Process ID " << process_id_ << " not found in hosts list" << std::endl;
            close(socket_fd_);
            socket_fd_ = -1;
            return false;
        }
        
        // Bind socket to our address
        struct sockaddr_in bind_addr;
        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_addr.s_addr = my_host.ip;
        bind_addr.sin_port = my_host.port;
        
        if (bind(socket_fd_, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) < 0) {
            std::cerr << "Failed to bind socket: " << strerror(errno) << std::endl;
            close(socket_fd_);
            socket_fd_ = -1;
            return false;
        }
        
        std::cout << "Perfect Links initialized on " << my_host.ipReadable() 
                  << ":" << my_host.portReadable() << std::endl;
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception during initialization: " << e.what() << std::endl;
        if (socket_fd_ >= 0) {
            close(socket_fd_);
            socket_fd_ = -1;
        }
        return false;
    }
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
    
    // Flush output file
    if (output_file_.is_open()) {
        output_file_.flush();
    }
    
    std::cout << "Perfect Links stopped" << std::endl;
}

// Send a message using Perfect Links
void PerfectLinks::send(uint8_t destination_id, uint32_t message) {
    if (!running_ || socket_fd_ < 0) {
        std::cerr << "Perfect Links not running" << std::endl;
        return;
    }
    
    // Find destination host
    Parser::Host dest_host;
    bool found = false;
    for (const auto& host : hosts_) {
        if (host.id == destination_id) {
            dest_host = host;
            found = true;
            break;
        }
    }
    
    if (!found) {
        std::cerr << "Destination host " << destination_id << " not found" << std::endl;
        return;
    }
    
    // Create message
    uint32_t seq_num = next_sequence_number_++;
    PLMessage msg(static_cast<uint32_t>(process_id_), seq_num, 0, message);  // type 0 = DATA
    
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
    
    // Log broadcast
    logBroadcast(seq_num);
}

// Broadcast a message to all other processes
void PerfectLinks::broadcast(uint32_t message) {
    for (const auto& host : hosts_) {
        if (host.id != process_id_) {
            send(static_cast<uint8_t>(host.id), message);
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
    if (msg.message_type == 0) {  // DATA message
        handleDataMessage(msg, sender_addr);
    } else if (msg.message_type == 1) {  // ACK message
        handleAckMessage(msg);
    }
}

void PerfectLinks::handleDataMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr) {
    // Send ACK immediately
    PLMessage ack(static_cast<uint32_t>(process_id_), msg.sequence_number, 1, 0);  // type 1 = ACK
    
    struct sockaddr_in ack_addr = sender_addr;
    sendto(socket_fd_, &ack, sizeof(ack), 0, reinterpret_cast<struct sockaddr*>(&ack_addr), sizeof(ack_addr));
    
    // Check for duplicate delivery
    {
        std::lock_guard<std::mutex> lock(delivered_messages_mutex_);// Check for duplicates
        if (delivered_messages_[static_cast<uint8_t>(msg.sender_id)].count(msg.sequence_number) > 0) {
            return; // Already delivered
        }
        delivered_messages_[static_cast<uint8_t>(msg.sender_id)].insert(msg.sequence_number);
    }
    
    // Deliver message
    logDelivery(msg.sender_id, msg.sequence_number);
}

void PerfectLinks::handleAckMessage(const PLMessage& msg) {
    std::lock_guard<std::mutex> lock(pending_messages_mutex_);
    auto key = std::make_pair(static_cast<unsigned long>(msg.sender_id), msg.sequence_number);
    auto it = pending_messages_.find(key);
    
    if (it != pending_messages_.end()) {
        it->second.ack_received = true;
    }
}

void PerfectLinks::retransmissionLoop() {
    const auto timeout = std::chrono::milliseconds(100);  // 100ms timeout
    
    while (running_) {
        auto now = std::chrono::steady_clock::now();
        
        {
            std::lock_guard<std::mutex> lock(pending_messages_mutex_);
            for (auto& pair : pending_messages_) {
                PendingMessage& pending = pair.second;
                
                if (!pending.ack_received && 
                    (now - pending.last_sent) > timeout) {
                    
                    // Retransmit
                    sendMessage(pending.message, pending.destination);
                    pending.last_sent = now;
                }
            }
            
            // Clean up acknowledged messages
            auto it = pending_messages_.begin();
            while (it != pending_messages_.end()) {
                if (it->second.ack_received) {
                    it = pending_messages_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void PerfectLinks::logBroadcast(uint32_t sequence_number) {
    std::lock_guard<std::mutex> lock(output_mutex_);
    output_file_ << "b " << sequence_number << std::endl;
    output_file_.flush();
}

void PerfectLinks::logDelivery(uint32_t sender_id, uint32_t sequence_number) {
    std::lock_guard<std::mutex> lock(output_mutex_);
    output_file_ << "d " << sender_id << " " << sequence_number << std::endl;
    output_file_.flush();
}