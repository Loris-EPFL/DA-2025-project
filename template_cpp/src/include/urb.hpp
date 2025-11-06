#pragma once

#include <cstdint>
#include <vector>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <functional>

#include "parser.hpp"

// Forward declaration to avoid circular include
class PerfectLinks;
class Logger;

/**
 * Minimal Uniform Reliable Broadcast (URB) built on Perfect Links.
 * URB ensures uniform agreement: if any process delivers m, all correct deliver m.
 *
 * Approach:
 * - Payload carries {origin_id, sequence} as two uint32_t.
 * - On first reception of a message key (origin, seq), rebroadcast it using PerfectLinks.
 * - Deliver once the message has been seen from a majority of processes.
 */
class UniformReliableBroadcast {
public:
    UniformReliableBroadcast(uint8_t process_id,
                             const std::vector<Parser::Host>& hosts,
                             Logger& logger);

    // Set PerfectLinks instance to use for send/rebroadcast
    void setPerfectLinks(PerfectLinks* pl);

    // Broadcast an integer message as URB payload (origin = self)
    void broadcast(uint32_t message);

    // Callback to be connected to PerfectLinks deliveries
    void onPerfectLinksDeliver(uint32_t /*pl_sender_id*/, uint32_t /*pl_seq_num*/, const std::vector<uint8_t>& payload);

private:
    struct MsgKey {
        uint32_t origin_id;
        uint32_t sequence;
        bool operator==(const MsgKey& other) const {
            return origin_id == other.origin_id && sequence == other.sequence;
        }
    };

    struct MsgKeyHash {
        std::size_t operator()(const MsgKey& k) const {
            return (static_cast<std::size_t>(k.origin_id) << 32) ^ static_cast<std::size_t>(k.sequence);
        }
    };

    // Encode {origin_id, sequence} into bytes
    static std::vector<uint8_t> encode(uint32_t origin_id, uint32_t sequence);
    // Decode bytes into {origin_id, sequence}; returns false on failure
    static bool decode(const std::vector<uint8_t>& payload, uint32_t& origin_id_out, uint32_t& sequence_out);

    uint8_t process_id_;
    std::vector<Parser::Host> hosts_;
    PerfectLinks* pl_{nullptr};
    Logger& logger_;
    uint32_t majority_threshold_;

    // Track which forwarders have been seen for each (origin, seq)
    std::unordered_map<MsgKey, std::unordered_set<uint32_t>, MsgKeyHash> seen_forwarders_;
    // Track delivered messages to avoid duplicates
    std::unordered_set<MsgKey, MsgKeyHash> delivered_;

    // FIFO ordering state: next expected sequence per origin and ready set
    std::unordered_map<uint32_t, uint32_t> next_expected_seq_;
    std::unordered_map<uint32_t, std::unordered_set<uint32_t>> ready_to_deliver_;

    // Track messages this process has already rebroadcasted to avoid multiple re-sends
    std::unordered_set<MsgKey, MsgKeyHash> rebroadcasted_;
};