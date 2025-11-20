#include "urb.hpp"
#include "perfect_links.hpp"
#include "logger.hpp"
#include <cstring>
#include <mutex>
#include <fstream>

UniformReliableBroadcast::UniformReliableBroadcast(uint8_t process_id,
                                                   const std::vector<Parser::Host>& hosts,
                                                   Logger& logger)
    : process_id_(process_id), hosts_(hosts), logger_(logger) {
    // Majority = floor(N/2) + 1
    majority_threshold_ = static_cast<uint32_t>(hosts_.size() / 2 + 1);
    // Initialize FIFO state: expect sequence 1 from every origin
    for (const auto& h : hosts_) {
        uint32_t host_id = static_cast<uint32_t>(h.id);
        next_expected_seq_[host_id] = 1;
    }
}

void UniformReliableBroadcast::setPerfectLinks(PerfectLinks* pl) {
    pl_ = pl;
}

std::vector<uint8_t> UniformReliableBroadcast::encode(uint32_t origin_id, uint32_t sequence) {
    std::vector<uint8_t> buf(sizeof(uint32_t) * 2);
    std::memcpy(buf.data(), &origin_id, sizeof(uint32_t));
    std::memcpy(buf.data() + sizeof(uint32_t), &sequence, sizeof(uint32_t));
    return buf;
}

bool UniformReliableBroadcast::decode(const std::vector<uint8_t>& payload, uint32_t& origin_id_out, uint32_t& sequence_out) {
    if (payload.size() != sizeof(uint32_t) * 2) {
        return false;
    }
    std::memcpy(&origin_id_out, payload.data(), sizeof(uint32_t));
    std::memcpy(&sequence_out, payload.data() + sizeof(uint32_t), sizeof(uint32_t));
    return true;
}

void UniformReliableBroadcast::broadcast(uint32_t message) {
    if (!pl_) return;
    // origin_id = this process
    uint32_t origin = static_cast<uint32_t>(process_id_);
    uint32_t seq = message; // assume app uses 1..M as sequence numbers
    auto payload = encode(origin, seq);

    // ATOMIC OPERATION: Log broadcast, update state, and send to network atomically
    // This ensures that if we get SIGSTOP/SIGTERM, the operation is truly atomic
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        // Log broadcast first
        logger_.logBroadcast(seq);
        // Force immediate flush to disk to prevent race conditions with SIGSTOP/SIGTERM
        logger_.periodicFlush(true);
        
        // Track that we broadcast this message (for signal-safe resumption)
        own_broadcasts_.insert(seq);
        
        // Count ourselves towards majority
        MsgKey key{origin, seq};
        seen_forwarders_[key].insert(static_cast<uint32_t>(process_id_));
        rebroadcasted_.insert(key);
        
        // Mark message ready when seen by majority (includes self)
        if (seen_forwarders_[key].size() >= majority_threshold_) {
            ready_to_deliver_[origin].insert(seq);
        }
        
        // Best-effort broadcast using Perfect Links AFTER updating state
        pl_->broadcast(payload);
    }
}

void UniformReliableBroadcast::onPerfectLinksDeliver(uint32_t pl_sender_id, uint32_t /*pl_seq_num*/, const std::vector<uint8_t>& payload) {
    uint32_t origin{}, seq{};
    if (!decode(payload, origin, seq)) {
        // Ignore non-URB payloads
        return;
    }

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        // Check if we've already delivered this message
        MsgKey key{origin, seq};
        if (delivered_.find(key) != delivered_.end()) {
            // Already delivered, but we still need to process for majority counting
        }
        auto& seen = seen_forwarders_[key];
        bool first_time_for_this_forwarder = seen.insert(pl_sender_id).second;

        // Re-broadcast once upon first reception of this message from ANY process
        if (first_time_for_this_forwarder && pl_) {
            if (rebroadcasted_.find(key) == rebroadcasted_.end()) {
                pl_->broadcast(payload);
                rebroadcasted_.insert(key);
            }
        }

        // Mark message ready when seen by majority
        if (seen.size() >= majority_threshold_) {
            ready_to_deliver_[origin].insert(seq);
        }

        // Enforce FIFO: deliver in increasing order per origin only when ready
        auto it = next_expected_seq_.find(origin);
        if (it == next_expected_seq_.end()) {
            next_expected_seq_[origin] = 1;
            it = next_expected_seq_.find(origin);
        }
        uint32_t& next_seq = it->second;
        auto& ready_set = ready_to_deliver_[origin];
        
        while (ready_set.find(next_seq) != ready_set.end()) {
            // Simple duplicate check using delivered set
            MsgKey key{origin, next_seq};
            if (delivered_.find(key) == delivered_.end()) {
                // This is a new delivery
                delivered_.insert(key);
                logger_.logDelivery(origin, next_seq);
                
                // Trigger GC periodically
                deliveries_since_last_gc_++;
                if (deliveries_since_last_gc_ >= GC_INTERVAL) {
                    gcOnDelivery(origin);
                    deliveries_since_last_gc_ = 0;
                }
            }
            
            // Always remove from ready set and advance sequence
            ready_set.erase(next_seq);
            ++next_seq;
        }
    }
}

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
    
    if (!found_any) return;
    
    // Use conservative watermark (allow some margin)
    uint32_t watermark = min_delivered_seq > GC_MARGIN ? min_delivered_seq - GC_MARGIN : 0;
    if (watermark == 0) return;
    
    // Prune seen_forwarders_
    std::vector<MsgKey> keys_to_erase;
    keys_to_erase.reserve(64);
    for (const auto& kv : seen_forwarders_) {
        const MsgKey& k = kv.first;
        if (k.origin_id == origin && k.sequence <= watermark) {
            keys_to_erase.push_back(k);
        }
    }
    for (const auto& k : keys_to_erase) {
        seen_forwarders_.erase(k);
    }

    // Prune rebroadcasted_
    std::vector<MsgKey> rb_to_erase;
    rb_to_erase.reserve(64);
    for (const auto& k : rebroadcasted_) {
        if (k.origin_id == origin && k.sequence <= watermark) {
            rb_to_erase.push_back(k);
        }
    }
    for (const auto& k : rb_to_erase) {
        rebroadcasted_.erase(k);
    }

    auto stats = snapshotMemStats();
    std::cout << "URB GC origin=" << origin
              << " watermark=" << watermark
              << " seen_keys=" << stats.seen_keys
              << " seen_total=" << stats.seen_total
              << " rebroadcasted=" << stats.rebroadcasted
              << " ready_total=" << stats.ready_total
              << " rss_kb=" << stats.rss_kb
              << std::endl;
}

UniformReliableBroadcast::MemStats UniformReliableBroadcast::snapshotMemStats() {
    MemStats stats;
    stats.seen_keys = seen_forwarders_.size();
    stats.seen_total = 0;
    for (const auto& kv : seen_forwarders_) {
        stats.seen_total += kv.second.size();
    }
    stats.rebroadcasted = rebroadcasted_.size();
    stats.ready_total = 0;
    for (const auto& kv : ready_to_deliver_) {
        stats.ready_total += kv.second.size();
    }
    
    // Get RSS from /proc/self/status
    stats.rss_kb = 0;
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            size_t start = line.find_first_of("0123456789");
            if (start != std::string::npos) {
                stats.rss_kb = std::stoull(line.substr(start));
            }
            break;
        }
    }
    
    return stats;
}

uint32_t UniformReliableBroadcast::getNextSequentialBroadcast() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return next_broadcast_seq_;
}

uint32_t UniformReliableBroadcast::broadcastNextSequential(uint32_t max_messages) {
    uint32_t seq_to_broadcast;
    
    // Atomic check and increment
    {
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
    
    // Now broadcast the message (this will update own_broadcasts_ internally)
    broadcast(seq_to_broadcast);
    
    return seq_to_broadcast;
}