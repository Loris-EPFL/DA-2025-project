#include "urb.hpp"
#include "perfect_links.hpp"
#include "logger.hpp"
#include <cstring>
#include <mutex>
#include <fstream>


/**
 * FIFO implementation on top of URB
 * 
 * Need to respect FRB properties:
 * - FRB1 Validity: If pi and pj are correct, every m broadcast by pi is eventually delivered by pj .
 * - FRB2 No Duplication: No m delivered more than once.
 * - FRB3 No Creation: No m delivered unless it was broadcast.
 * - FRB4 Uniform Agreement: For any message m , if any process (correct or faulty) delivers m , then every correct process delivers m . (Stronger than URB4: if sender crashes after some process delivers, all correct processes must still deliver).
 * - FRB5 delivery: If some process broadcasts message m1 before it broadcasts message m2, then no correct process delivers m2 unless it has already delivered m1.
 */

UniformReliableBroadcast::UniformReliableBroadcast(uint8_t process_id, const std::vector<Parser::Host>& hosts, Logger& logger) : process_id_(process_id), hosts_(hosts), logger_(logger) {
    // Majority = floor(N/2) + 1
    majority_threshold_ = static_cast<uint32_t>(hosts_.size() / 2 + 1);
    // Initialize FIFO state: expect sequence 1 from every origin
    for (const auto& h : hosts_) {
        uint32_t host_id = static_cast<uint32_t>(h.id);
        next_expected_seq_[host_id] = 1;
    }
}

/*
* Set the perfect links instance to use for broadcasting (underlying calls are payload agnostic with byte vectors)
*/
void UniformReliableBroadcast::setPerfectLinks(PerfectLinks* pl) {
    pl_ = pl;
}

/**
 * Encode origin_id and sequence into a byte vector
 * @param origin_id The origin process ID
 * @param sequence The sequence number
 * @return Byte vector containing origin_id and sequence
 */
std::vector<uint8_t> UniformReliableBroadcast::encode(uint32_t origin_id, uint32_t sequence) {
    std::vector<uint8_t> buf(sizeof(uint32_t) * 2); //Create buffer for two 32-bit integers
    std::memcpy(buf.data(), &origin_id, sizeof(uint32_t)); //Copy origin_id into buffer at 1st location
    std::memcpy(buf.data() + sizeof(uint32_t), &sequence, sizeof(uint32_t)); //Copy sequence into buffer at 2nd location
    return buf;
}

/**
 * Decode a byte vector into origin_id and sequence
 * @param payload The byte vector to decode
 * @param origin_id_out Output parameter for origin_id
 * @param sequence_out Output parameter for sequence
 * @return true if decoding was successful, false otherwise
 */
bool UniformReliableBroadcast::decode(const std::vector<uint8_t>& payload, uint32_t& origin_id_out, uint32_t& sequence_out) {
    if (payload.size() != sizeof(uint32_t) * 2) {
        return false; //Needs to be exactly 8 bytes for two 32-bit integers
    }
    std::memcpy(&origin_id_out, payload.data(), sizeof(uint32_t)); //Copy origin_id from payload at 1st location
    std::memcpy(&sequence_out, payload.data() + sizeof(uint32_t), sizeof(uint32_t)); //Copy sequence from payload at 2nd location
    return true;
}

/**
 * Broadcast a message to all other processes
 * @param message The integer message to broadcast 
 * //TODO should also be bytes vector if PL is already payload agnostic ? Can we only rely on agnostic PL ?
 */
void UniformReliableBroadcast::broadcast(uint32_t message) {
    if (!pl_) return; //If PL is not initialized, do nothing
    // origin_id = this process
    uint32_t origin = static_cast<uint32_t>(process_id_);
    uint32_t seq = message; // assume app uses 1..M as sequence numbers
    auto payload = encode(origin, seq); // encode the origin and sequence number as a byte vector

    // ATOMIC OPERATION: Log broadcast, update state, and send to network atomically
    // This ensures that if we get SIGSTOP/SIGTERM during stress.py, the operation is truly atomic
    {
        // Lock needed to prevent race conditions with other threads
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        // Log broadcast first
        logger_.logBroadcast(seq);
        // Force immediate flush to disk to prevent race conditions with SIGSTOP/SIGTERM
        logger_.periodicFlush(true);
        
        // Track that we broadcast this message (for signal-safe resumption)
        own_broadcasts_.insert(seq);
        
        // Count ourselves towards majority
        MsgKey key{origin, seq};
        seen_forwarders_[key].insert(static_cast<uint32_t>(process_id_)); //Add ourselves to the set of forwarders
        rebroadcasted_.insert(key); //Add ourselves to the set of rebroadcasted messages
        
        // Mark message ready when seen by majority (including ourself)
        if (seen_forwarders_[key].size() >= majority_threshold_) {
            ready_to_deliver_[origin].insert(seq);
        }
        
        // Best-effort broadcast using Perfect Links only AFTER updating state!!!
        pl_->broadcast(payload);
    }
}

/**
 * Callback from Perfect Links delivery
 * @param pl_sender_id The ID of the sender process
 * @param pl_seq_num The sequence number of the message (unused in URB)
 * @param payload The payload vector of bytes containing URB message
 */
void UniformReliableBroadcast::onPerfectLinksDeliver(uint32_t pl_sender_id, uint32_t pl_seq_num, const std::vector<uint8_t>& payload) {
    uint32_t origin{}, seq{};
    if (!decode(payload, origin, seq)) {
        // Ignore payloads that we can't decode correctly
        return;
    }

    {
        // Lock the entire state to prevent race conditions
        // This is critical when multiple PL deliveries happen concurrently
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        MsgKey key{origin, seq};
        
        // Keep track of who forwarded this message to us
        // Even if we already delivered it, we still need this for the majority counting to work
        auto& seen = seen_forwarders_[key];
        bool first_time_from_this_forwarder = seen.insert(pl_sender_id).second;

        // if this is the first time we see this message from anyone,
        // Re-broadcast once upon first reception of this message from ANY process
        // This ensures uniform agreement - if we see it, everyone should see it
        if (first_time_from_this_forwarder && pl_) {
            if (rebroadcasted_.find(key) == rebroadcasted_.end()) {
                pl_->broadcast(payload); // Send it to everyone else
                rebroadcasted_.insert(key); // Remember we already rebroadcasted this message
            }
        }

        // Mark message ready for delivery when seen by majority for agreement
        if (seen.size() >= majority_threshold_) {
            ready_to_deliver_[origin].insert(seq);
        }

        // FIFO delivery: deliver messages in sequence order per origin
        auto it = next_expected_seq_.find(origin);
        if (it == next_expected_seq_.end()) {
            next_expected_seq_[origin] = 1;  // Start expecting sequence 1 after end of sequence
            it = next_expected_seq_.find(origin);
        }
        
        //next sequence number we're expecting from this sender (second one)
        uint32_t& next_expected = it->second;
        auto& ready_set = ready_to_deliver_[origin];
        
        // Deliver all consecutive ready messages starting from next_expected
        // ensures FIFO delivery order per origin
        while (ready_set.find(next_expected) != ready_set.end()) {
            MsgKey delivery_key{origin, next_expected};
            
            // Only log delivery once per message  (no duplication)
            if (delivered_.find(delivery_key) == delivered_.end()) {
                delivered_.insert(delivery_key);
                logger_.logDelivery(origin, next_expected);
                
                // Clean up old stuff periodically so we don't run out of memory
                // Probably overkill for the test cases but better safe than sorry
                deliveries_since_last_gc_++;
                if (deliveries_since_last_gc_ >= GC_INTERVAL) {
                    gcOnDelivery(origin);
                    deliveries_since_last_gc_ = 0;
                }
            }
            
            // Always clean up and advance (even for duplicates)
            ready_set.erase(next_expected);
            ++next_expected;
        }
    }
}

/**
 * Garbage collection on delivery
 * @param origin The origin of the message
 //TODO keep or no for memory constraints ?
 */
void UniformReliableBroadcast::gcOnDelivery(uint32_t origin) {
    // Find the minimum delivered sequence for this origin to use as watermark
    uint32_t min_delivered_seq = UINT32_MAX;
    bool found_any = false;
    for (const auto& key : delivered_) {
        if (key.origin_id == origin) {
            min_delivered_seq = std::min(min_delivered_seq, key.sequence);
            found_any = true;
        }
    }
    
    if (!found_any) return; //Nothing to cleanup
    
    // Use conservative watermark (allow some margin)
    uint32_t watermark = min_delivered_seq > GC_MARGIN ? min_delivered_seq - GC_MARGIN : 0;
    if (watermark == 0) return;
    
    // Prune seen_forwarders_
    std::vector<MsgKey> keys_to_erase;
    keys_to_erase.reserve(64);
    for (const auto& kv : seen_forwarders_) {
        const MsgKey& k = kv.first;
        if (k.origin_id == origin && k.sequence <= watermark) {
            keys_to_erase.push_back(k); // Mark for removal
        }
    }
    for (const auto& k : keys_to_erase) {
        seen_forwarders_.erase(k); //actually delete
    }

    // Prune rebroadcasted_
    std::vector<MsgKey> rb_to_erase;
    rb_to_erase.reserve(64);
    for (const auto& k : rebroadcasted_) {
        if (k.origin_id == origin && k.sequence <= watermark) {
            rb_to_erase.push_back(k); // Mark for removal
        }
    }
    for (const auto& k : rb_to_erase) {
        rebroadcasted_.erase(k); //actually delete
    }

    //Debug
    std::cout << "GC: Cleaned up rebroadcasted_ and seen_forwarders_ for origin " << origin << std::endl;

}

/*
 * Getter for next sequential broadcast
 * Mutex is required to prevent race conditions
 */
uint32_t UniformReliableBroadcast::getNextSequentialBroadcast() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return next_broadcast_seq_;
}

/*
 * Broadcast next sequential message
 * Mutex is required to prevent race conditions
 */
uint32_t UniformReliableBroadcast::broadcastNextSequential(uint32_t max_messages) {
    uint32_t seq_to_broadcast;
    
    // Atomic check and increment
    {
        //Mutex required to avoid race conditions
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        // Check if we're done broadcasting
        if (next_broadcast_seq_ > max_messages) {
            return 0; // No more messages to broadcast
        }
        
        seq_to_broadcast = next_broadcast_seq_;
        
        // Check if already broadcast (shouldn't happen with sequential approach, but safety check)
        if (own_broadcasts_.find(seq_to_broadcast) != own_broadcasts_.end()) {
            next_broadcast_seq_++;
            return 0; // Skip already broadcast message
        }
        
        // Increment counter BEFORE releasing lock to prevent race conditions
        next_broadcast_seq_++;
    } // Lock released here
    
    // Now broadcast the message outside the lock (this will update own_broadcasts_ internally)
    broadcast(seq_to_broadcast);
    
    return seq_to_broadcast;
}