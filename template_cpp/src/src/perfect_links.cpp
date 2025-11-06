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
PerfectLinks::PerfectLinks(Parser::Host localhost,std::function<void(uint32_t, uint32_t, const std::vector<uint8_t>&)> deliveryCallback, const std::vector<Parser::Host>& hosts, const std::string& output_path): process_id_(static_cast<uint8_t>(localhost.id)), localhost_(localhost), id_to_peer_(), delivery_callback_(std::move(deliveryCallback)), output_path_(output_path), socket_fd_(-1), local_vector_clock_(), running_(false), next_sequence_number_(1)
{  
    // Create ID to peer map from hosts vector
    for (const auto& host : hosts) {
        id_to_peer_[static_cast<uint8_t>(host.id)] = host;
    }
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
    
    // We need non-blocking sockets, otherwise the recvfrom() call in the main loop will
    // block forever and the whole process will hang.
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
    
    // socket buffer sizes
    int send_buffer_size = 1024 * 1024;  // 1MB send buffer
    int recv_buffer_size = 1024 * 1024;  // 1MB receive buffer
    
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_SNDBUF, &send_buffer_size, sizeof(send_buffer_size)) < 0) {
        std::cerr << "Warning: Failed to set send buffer size: " << strerror(errno) << std::endl;
        // Continue
    }
    
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &recv_buffer_size, sizeof(recv_buffer_size)) < 0) {
        std::cerr << "Warning: Failed to set receive buffer size: " << strerror(errno) << std::endl;
        // Continue
    }
    
    // Bind socket to localhost
    struct sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));

    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = localhost_.ip;
    local_addr.sin_port = localhost_.port;
    
    if (bind(socket_fd_, reinterpret_cast<struct sockaddr*>(&local_addr), sizeof(local_addr)) < 0) {
        std::cerr << "Failed to bind socket to " << localhost_.ipReadable() << ":" << localhost_.portReadable()  << " - " << strerror(errno) << std::endl;
        //Then close the socket and return false
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }
    
    std::cout << "Perfect Links initialized on " << localhost_.ipReadable() << ":" << localhost_.portReadable() << std::endl;
    
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

    if (!running_.load()) {
        return;  // Already stopped
    }
    
    std::cout << "Stopping Perfect Links..." << std::endl;
    
    // Flush all pending batches before stopping
    flushAllBatches();
    
    // Set running flag to false to stop all loops
    running_.store(false);
    
    // Close socket to interrupt any blocking operations
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
    
    // Wait for threads to finish with timeout
    if (receiver_thread_.joinable()) {
        receiver_thread_.join();
    }
    // Join retransmission thread if joinable
    if (retransmission_thread_.joinable()) {
        retransmission_thread_.join();
    }
    
    std::cout << "Perfect Links stopped." << std::endl;
}

/**
 * Send a message with a generic payload vector of bytes to a specific destination
 * @param destination_id The ID of the destination process
 * @param payload The payload vector of bytes to send
 */
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
    
    // TODO: Vector clock increment - not needed for now (Also can I use external libraries for that ?)
    // Will be re-enabled for future milestones if needed for causal ordering
    // local_vector_clock_.increment(process_id_);
    PLMessage msg(static_cast<uint32_t>(process_id_), destination_id, seq_num, local_vector_clock_, MessageType::DATA, payload, true);
    
    // Store for retransmission
    {
        std::lock_guard<std::mutex> lock(pending_messages_mutex_);
        PendingMessage pending;
        //Store the message fields
        pending.message = msg;
        pending.destination = dest_host;
        pending.last_sent = std::chrono::steady_clock::now();
        pending.ack_received = false;
        //Store the pending message in the map
        pending_messages_[destination_id][seq_num] = pending;
    }
    
    // Send using batching
    sendBatchedMessage(msg, destination_id);
}

// Send a message with integer payload (overloaded method that uses the send method with byte vector) //TODO keep or just use the one with byte vector ?
/**
 * Send a message with an integer payload to a specific destination
 * @param destination_id The ID of the destination process
 * @param message The integer message to send
 */
void PerfectLinks::send(uint8_t destination_id, uint32_t message) {
    // Convert integer to byte vector
    std::vector<uint8_t> payload(sizeof(uint32_t));
    std::memcpy(payload.data(), &message, sizeof(uint32_t)); //Copy the integer message to the byte vector
    send(destination_id, payload); //then send using the general bytes vector method
}

// Broadcast a message to all other processes
/**
 * Broadcast a message to all other processes except the sender
 * @param payload The byte vector payload of the message to broadcast
 */
void PerfectLinks::broadcast(const std::vector<uint8_t>& payload) {
    for (const auto& [id, host] : id_to_peer_) {
        if (id != process_id_) {
            send(static_cast<uint8_t>(id), payload);
        }
    }
}

// Broadcast a message with integer payload (overloaded from broadcast with byte vector)
/**
 * Broadcast a message with an integer payload to all other processes except the sender
 * @param message The integer message to broadcast
 */
void PerfectLinks::broadcast(uint32_t message) {
    // Convert integer to byte vector
    std::vector<uint8_t> payload(sizeof(uint32_t));
    std::memcpy(payload.data(), &message, sizeof(uint32_t));
    broadcast(payload); //then broadcast using the general bytes vector method
}

// Send message one by one.I will Keep It in case batching doesn't work
// void PerfectLinks::sendMessage(const PLMessage& msg, const Parser::Host& destination) {

//     if (socket_fd_ < 0 || !running_) {
//         return;
//     }
    
//     struct sockaddr_in dest_addr;
//     memset(&dest_addr, 0, sizeof(dest_addr));

//     // Set up destination address
//     dest_addr.sin_family = AF_INET;
//     dest_addr.sin_addr.s_addr = destination.ip;
//     dest_addr.sin_port = destination.port;
    
//     // Serialize message to buffer
//     std::vector<uint8_t> buffer;
//     try {
//         size_t msg_size = msg.serialize(buffer);
        
//         if (msg_size == 0 || msg_size > 65536) {  // Sanity check of size
//             std::cerr << "Invalid message size: " << msg_size << std::endl;
//             return;
//         }
        
//         // Then Send message to destination
//         ssize_t sent = sendto(socket_fd_, buffer.data(), msg_size, 0, 
//                              reinterpret_cast<struct sockaddr*>(&dest_addr), sizeof(dest_addr));
        
//         if (sent < 0) {
//             // fatal error can't do anything without a socket.
//             if (errno != EAGAIN) {
//                 std::cerr << "FATAL : Failed to send message: " << strerror(errno) << std::endl;
//             }
//             // This shouldn't really happen with UDP, but log it just in case.
//         } else if (static_cast<size_t>(sent) != msg_size) {
//             std::cerr << "WARNING : Partial send: " << sent << " of " << msg_size << " bytes" << std::endl;
//         }
//     } catch (const std::exception& e) {
//         std::cerr << "Error serializing message: " << e.what() << std::endl;
//     }
// }

/**
 * Main receive loop that continuously listens for incoming messages
 *  run in a separate thread
 */
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
        // Receive message from socket
        ssize_t received = recvfrom(socket_fd_, buffer.data(), buffer.size(), 0, reinterpret_cast<struct sockaddr*>(&sender_addr), &addr_len);
        
        if (received < 0) {
            if (errno == EAGAIN) {
                // Non-blocking socket, no data available
                // Use exponential backoff to reduce CPU usage when idle
                static thread_local auto idle_sleep = std::chrono::microseconds(10);
                std::this_thread::sleep_for(idle_sleep);
                idle_sleep = std::min(idle_sleep * 2, std::chrono::microseconds(1000));
                continue;
            } else if (errno == EINTR) {
                // Interrupted by signal, continue
                continue;
            } else {
                std::cerr << "Failed to receive message: " << strerror(errno) << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
        } else {
            // Reset idle sleep when we receive data
            static thread_local auto idle_sleep = std::chrono::microseconds(10);
        }
        
        if (received > 0) {
            try {
                // Resize buffer to actual received size for processing
                std::vector<uint8_t> received_buffer(buffer.begin(), buffer.begin() + received);
                
                // Try to handle as batched message first (backward compatible)
                handleBatchedMessage(received_buffer, sender_addr);
            } catch (const std::exception& e) {
                std::cerr << "Error handling received message: " << e.what() << std::endl;
            }
        }
    }
}

//Handlers for different message types (ack, payload, etc)

/**
 * Handle  general received message (acknowledgment or data payload)
 * @param msg The message to handle
 * @param sender_addr The address of the sender
 */
void PerfectLinks::handleMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr) {
    if (msg.message_type == MessageType::DATA) {  // DATA message
        handleDataMessage(msg, sender_addr);
    } else if (msg.message_type == MessageType::ACK) {  // ACK message
        handleAckMessage(msg);
    }
}

/**
 * Handle a received data message
 * @param msg The data message to handle
 * @param sender_addr The address of the sender
 */
void PerfectLinks::handleDataMessage(const PLMessage& msg, const struct sockaddr_in& sender_addr) {
    uint8_t sender_id = static_cast<uint8_t>(msg.sender_id);
    uint32_t seq_num = msg.sequence_number;
    const VectorClock& msg_clock = msg.vector_clock;
    
    // Only process messages from known senders (defined in hosts configuration)
    if (id_to_peer_.find(sender_id) == id_to_peer_.end()) {
        // ignore messages from unknown senders
    }
    
    // TODO: Vector clock update - not needed for now
    // Will be re-enabled for future milestones if needed for causal ordering
    // Update our local vector clock with the received message's clock
    // This needs to be thread-safe as multiple threads might access it
    // {
    //     std::lock_guard<std::mutex> clock_lock(delivered_messages_mutex_);  // Reuse mutex for simplicity
    //     local_vector_clock_.update(msg_clock);
    // }
    
    // IMPORTANT: Mark as delivered *before* calling the callback. If we do it after,
    // a race condition can occur where we receive the same message again and process it twice.
    bool already_delivered = false;
    {
        std::lock_guard<std::mutex> lock(delivered_messages_mutex_);
        already_delivered = (delivered_messages_[sender_id].count(seq_num) > 0);
        
        // If not already delivered, mark it now to prevent race conditions
        if (!already_delivered) {
            delivered_messages_[sender_id].insert(seq_num);
        }
    }
    
    // send ACK (whether duplicate or new message), doesn't violate PL2 since its only the acks
    PLMessage ack_msg(static_cast<uint32_t>(process_id_), sender_id, seq_num, msg_clock, MessageType::ACK, false);
    sendBatchedMessage(ack_msg, sender_id);
    
    //  deliver if this is a new message not already delivered
    if (!already_delivered) {
        // Pass the message up to the next layer
        if (delivery_callback_) {
            try {
                delivery_callback_(sender_id, seq_num, msg.payload); //Let callback handle it
            } catch (const std::exception& e) {
                std::cerr << "Error in delivery callback: " << e.what() << std::endl;
            }
        }
    }
}

/**
 * Handle a received acknowledgment message
 * @param msg The acknowledgment message to handle
 */
void PerfectLinks::handleAckMessage(const PLMessage& msg) {
    uint8_t sender_id = static_cast<uint8_t>(msg.sender_id);
    uint32_t seq_num = msg.sequence_number;
    
    // Only process ACKs from known senders
    if (id_to_peer_.find(sender_id) == id_to_peer_.end()) {
        // ignore ACKs from unknown senders
        return;
    }
    
    // Mark the message as acknowledged
    std::lock_guard<std::mutex> lock(pending_messages_mutex_); //needs mutex to prevent race conditions
    auto dest_it = pending_messages_.find(sender_id);
    if (dest_it != pending_messages_.end()) {
        auto msg_it = dest_it->second.find(seq_num);
        if (msg_it != dest_it->second.end()) {
            msg_it->second.ack_received = true; // Mark as acknowledged
        }
    }
    // If message not found, it might have been already cleaned up by retransmission thread
}

/**
 * Handle a received batched message (calls each underlying handler for each message in the batch)
 * @param buffer The buffer containing the batched message
 * @param sender_addr The address of the sender
 * @return True if the message was a batch, false if it was an individual message
 */
bool PerfectLinks::handleBatchedMessage(const std::vector<uint8_t>& buffer, const struct sockaddr_in& sender_addr) {
    BatchMessage batch;
    if (!batch.deserialize(buffer.data(), buffer.size())) {
        // Not a batch message, try to handle as individual message batch
        PLMessage msg;
        if (msg.deserialize(buffer.data(), buffer.size())) {
            handleMessage(msg, sender_addr); //Then pass message to handleMessage
        }
        return false;
    }
    
    // Process each message in the batch
    const std::vector<PLMessage>& messages = batch.getMessages();
    //For loop for every message in the batch (at most 8)
    for (const PLMessage& msg : messages) {
        handleMessage(msg, sender_addr); //Then pass message to handleMessage and let it handle the different cases
    }
    return true;
}
/**
 * Retransmission loop that periodically checks for timed-out messages and retransmits them
 */
void PerfectLinks::retransmissionLoop() {
    // Adaptive timeout: start with configurable base timeout, increase under adverse conditions for the tc.py script
    auto base_timeout = RETRANSMISSION_TIMEOUT;
    auto current_timeout = base_timeout;
    
    // Cleanup tracking: perform cleanup from time to time to avoid excessive overhead
    auto last_cleanup_time = std::chrono::steady_clock::now();
    constexpr auto CLEANUP_INTERVAL = std::chrono::seconds(1); 
    
    while (running_) {
        auto now = std::chrono::steady_clock::now();
        
        // Collect messages to retransmit in a smaller critical section
        std::vector<std::pair<PLMessage, Parser::Host>> messages_to_retransmit;
        
        {
            std::lock_guard<std::mutex> lock(pending_messages_mutex_);
            
            // First pass: collect messages that need retransmission
            for (auto& dest_pair : pending_messages_) {
                for (auto& msg_pair : dest_pair.second) {
                    PendingMessage& pending = msg_pair.second;
                    
                    if (!pending.ack_received && 
                        (now - pending.last_sent) > current_timeout) {
                        
                        // Collect for retransmission outside the lock
                        messages_to_retransmit.emplace_back(pending.message, pending.destination);
                        pending.last_sent = now;
                        
                        // Adaptive timeout: increase timeout for persistent failures after 5 retransmissions
                        if (pending.retransmit_count > 5) {
                            current_timeout = std::min(
                                MAX_ADAPTIVE_TIMEOUT, 
                                base_timeout * (1 + pending.retransmit_count / 10)
                            );
                        }
                        pending.retransmit_count++;
                    }
                }
            }
        }
        
        // Send messages first once outside the lock section to reduce lock usage
        for (const auto& [msg, dest] : messages_to_retransmit) {
            // Use batching for retransmissions
            uint8_t dest_id = static_cast<uint8_t>(dest.id);
            sendBatchedMessage(msg, dest_id);
        }
        
        {   //Need mutex since multiple threads might be cleaning up the set
            std::lock_guard<std::mutex> lock(pending_messages_mutex_);
            // Second pass: ONLY clean up acknowledged messages
            // keep retransmitting unacknowledged messages forever
            for (auto dest_it = pending_messages_.begin(); dest_it != pending_messages_.end();) {
                for (auto msg_it = dest_it->second.begin(); msg_it != dest_it->second.end();) {
                    if (msg_it->second.ack_received) {
                        msg_it = dest_it->second.erase(msg_it);
                    } else {
                        ++msg_it;
                    }
                }
                
                // Remove empty destination maps
                if (dest_it->second.empty()) {
                    dest_it = pending_messages_.erase(dest_it);
                } else {
                    ++dest_it;
                }
            }
        }
        
        // Periodic cleanup of delivered_messages_ 
        // Only perform cleanup if enough time has passed and cleanup is needed
        if ((now - last_cleanup_time) > CLEANUP_INTERVAL && shouldCleanupDeliveredMessages()) {
            cleanupDeliveredMessages(); // Clean up all senders
            last_cleanup_time = now;
        }
        
        // Sleep outside the critical section to reduce lock contention

        // Old way, was causing too much delay
        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::this_thread::sleep_for(RETRANSMISSION_SLEEP);
    }
}

/**
 * Decides when to send a message using batching (up to 8 messages per packet)
 * @param msg The message to send
 * @param destination_id The ID of the destination host
 */
void PerfectLinks::sendBatchedMessage(const PLMessage& msg, uint8_t destination_id) {
    std::lock_guard<std::mutex> lock(pending_batches_mutex_); //Need to lock to avoid race conditions for the pending_batches_ map
    
    // Add message to pending batch for this destination
    pending_batches_[destination_id].push_back(msg);
    
    // Check if we should flush the batch (size limit or timeout)
    auto now = std::chrono::steady_clock::now();
    bool should_flush = false;
    
    if (pending_batches_[destination_id].size() >= BatchMessage::MAX_MESSAGES_PER_BATCH) {
        should_flush = true;
    } else {
        auto last_time_it = last_batch_time_.find(destination_id);
        if (last_time_it != last_batch_time_.end() && 
            std::chrono::duration_cast<std::chrono::milliseconds>(now - last_time_it->second) >= BATCH_TIMEOUT) {
            should_flush = true;
        }
    }
    
    if (should_flush) {
        flushBatch(destination_id);
        last_batch_time_[destination_id] = now;
    }
}

/**
 * Performs the actual sending of a batch message to a destination (8 messages per packet)
 * @param batch The batch message to send
 * @param destination The destination host
 */
void PerfectLinks::sendBatchToDestination(const BatchMessage& batch, const Parser::Host& destination) {
    std::vector<uint8_t> buffer;
    if (!batch.serialize(buffer)) {
        std::cerr << "Failed to serialize batch message" << std::endl;
        return;
    }
    
    struct sockaddr_in dest_addr;
    memset(&dest_addr, 0, sizeof(dest_addr));

    // Set up destination address fields
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_addr.s_addr = destination.ip;
    dest_addr.sin_port = destination.port;
    //Send the batch message to socket
    ssize_t sent = sendto(socket_fd_, buffer.data(), buffer.size(), 0, reinterpret_cast<struct sockaddr*>(&dest_addr), sizeof(dest_addr)); //returns number of bytes sent from socket
    

    if (sent == -1) {
        // If number of bytes returned is -1, then there has been an error accroding to https://pubs.opengroup.org/onlinepubs/009604499/functions/sendto.html
        std::cerr << "Failed to send batch: " << strerror(errno) << std::endl;
    }
}



//CLEANUP functions : required because the servers have limited memory capacity (4GB Ram)

/**
 * Flushes a batch message to the destination host
 * @param destination_id The ID of the destination host
 */
void PerfectLinks::flushBatch(uint8_t destination_id) {
    // This method should be called with pending_batches_mutex_ already locked
    auto it = pending_batches_.find(destination_id);
    if (it == pending_batches_.end() || it->second.empty()) {
        return;
    }
    
    // Get the destination host from the ID
    auto peer_it = id_to_peer_.find(destination_id);
    if (peer_it == id_to_peer_.end()) {
        return; // Invalid destination ID
    }
    const Parser::Host& destination = peer_it->second;
    
    // Create batch message
    BatchMessage batch;
    for (const auto& msg : it->second) {
        if (!batch.addMessage(msg)) {
            // If we can't add more messages, send current batch and start new one
            sendBatchToDestination(batch, destination);
            batch = BatchMessage();
            batch.addMessage(msg);
        }
    }
    
    // Send the final batch
    if (batch.getMessageCount() > 0) {
        sendBatchToDestination(batch, destination);
    }
    
    // Clear the pending batch
    it->second.clear();
}

/**
 * Flushes all pending batch messages to their respective destination hosts
 */
void PerfectLinks::flushAllBatches() {
    //Needed for race conditions where several threads are flushing the batches at the same time
    std::lock_guard<std::mutex> lock(pending_batches_mutex_);
    
    // Flush all pending batches
    for (auto& [destination_id, messages] : pending_batches_) {
        if (!messages.empty()) {
            flushBatch(destination_id);
        }
    }
}

/**
 * Cleans up the delivered messages set for a specific sender or all senders
 * @param sender_id The ID of the sender to clean up, or 0 to clean up all senders
 */
void PerfectLinks::cleanupDeliveredMessages(uint8_t sender_id) {
    //Mutex needed for concurent access to delivered_messages_ set
    std::lock_guard<std::mutex> lock(delivered_messages_mutex_);
    
    if (sender_id == 0) {
        // Clean up all senders
        for (auto& [sid, seq_set] : delivered_messages_) {
            if (seq_set.size() > DELIVERED_MESSAGES_CLEANUP_THRESHOLD) { //Only if above threshold, should not happen sin practise unless we send a ton of messages
                cleanupSenderDeliveredMessages(sid, seq_set); //clean all messages except the last DELIVERED_MESSAGES_KEEP_RECENT
            }
        }
    } else {
        // Clean up specific sender
        auto it = delivered_messages_.find(sender_id);
        if (it != delivered_messages_.end() && it->second.size() > DELIVERED_MESSAGES_CLEANUP_THRESHOLD) {  //Only if above threshold, should not happen sin practise unless we send a ton of messages
            cleanupSenderDeliveredMessages(sender_id, it->second); //clean all messages for a specific sender except the last DELIVERED_MESSAGES_KEEP_RECENT
        }
    }
}

/**
 * Cleans up the delivered messages set for a specific sender
 * @param sender_id The ID of the sender to clean up
 * @param seq_set The set of sequence numbers to clean up
 */
void PerfectLinks::cleanupSenderDeliveredMessages(uint8_t sender_id, std::unordered_set<uint32_t>& seq_set) {
    //assumes delivered_messages_mutex_ is already locked, otherwise we have race conditions
    
    if (seq_set.size() <= DELIVERED_MESSAGES_KEEP_RECENT) {
        return; // Nothing to clean up
    }
    
    // Convert set to vector for easier manipulation
    std::vector<uint32_t> seq_numbers(seq_set.begin(), seq_set.end());
    
    // Sort to ensure we keep the most recent sequence numbers
    std::sort(seq_numbers.begin(), seq_numbers.end());
    
    // Calculate how many to remove
    size_t to_remove = seq_numbers.size() - DELIVERED_MESSAGES_KEEP_RECENT;
    
    // Remove the oldest sequence numbers (smallest values)
    for (size_t i = 0; i < to_remove; ++i) {
        seq_set.erase(seq_numbers[i]);
    }
    
    std::cout << "Cleaned up " << to_remove << " old delivered message records for sender " << static_cast<int>(sender_id) << ", kept " << seq_set.size() << " recent ones" << std::endl;
}

/**
 * Bool indicator function to check if any sender has exceeded the cleanup threshold for delivered messages
 * @return True if any sender has more messages than the cleanup threshold, false otherwise
 */
bool PerfectLinks::shouldCleanupDeliveredMessages() {
    std::lock_guard<std::mutex> lock(delivered_messages_mutex_);
    
    // Check if any sender has exceeded the cleanup threshold
    for (const auto& [sender_id, seq_set] : delivered_messages_) {
        if (seq_set.size() > DELIVERED_MESSAGES_CLEANUP_THRESHOLD) {
            return true;
        }
    }
    
    return false;
}