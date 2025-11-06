#include "urb.hpp"
#include "perfect_links.hpp"
#include "logger.hpp"
#include <cstring>

UniformReliableBroadcast::UniformReliableBroadcast(uint8_t process_id,
                                                   const std::vector<Parser::Host>& hosts,
                                                   Logger& logger)
    : process_id_(process_id), hosts_(hosts), logger_(logger) {
    // Majority = floor(N/2) + 1
    majority_threshold_ = static_cast<uint32_t>(hosts_.size() / 2 + 1);
    // Initialize FIFO state: expect sequence 1 from every origin
    for (const auto& h : hosts_) {
        next_expected_seq_[static_cast<uint32_t>(h.id)] = 1;
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

    // Count ourselves towards majority
    MsgKey key{origin, seq};
    seen_forwarders_[key].insert(static_cast<uint32_t>(process_id_));

    // Best-effort broadcast using Perfect Links
    pl_->broadcast(payload);
}

void UniformReliableBroadcast::onPerfectLinksDeliver(uint32_t pl_sender_id, uint32_t /*pl_seq_num*/, const std::vector<uint8_t>& payload) {
    uint32_t origin{}, seq{};
    if (!decode(payload, origin, seq)) {
        // Ignore non-URB payloads
        return;
    }

    MsgKey key{origin, seq};
    auto& seen = seen_forwarders_[key];
    bool first_time_for_this_forwarder = seen.insert(pl_sender_id).second;

    // Re-broadcast once upon first reception of this message at this process
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
        MsgKey deliver_key{origin, next_seq};
        if (delivered_.find(deliver_key) == delivered_.end()) {
            delivered_.insert(deliver_key);
            logger_.logDelivery(origin, next_seq);
        }
        ready_set.erase(next_seq);
        ++next_seq;
    }
}