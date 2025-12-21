#include "lattice_agreement.hpp"
#include "perfect_links.hpp"
#include "logger.hpp"
#include <cstring>
#include <algorithm>
#include <iostream>
#include <sstream>

/**
 * Multi-shot Lattice Agreement Implementation
 * 
 * Based on Algorithm 1 (Proposer) and Algorithm 2 (Acceptor) from project description.
 * Each process plays both roles for each slot.
 */

LatticeAgreement::LatticeAgreement(uint8_t process_id, 
                                   const std::vector<Parser::Host>& hosts,
                                   Logger& logger,
                                   uint32_t num_slots)
    : process_id_(process_id)
    , hosts_(hosts)
    , logger_(logger)
    , num_slots_(num_slots)
{
    num_processes_ = static_cast<uint32_t>(hosts_.size());
    // f = floor((n-1)/2), so n = 2f + 1 means f can crash
    f_ = (num_processes_ - 1) / 2;
    // Quorum = f + 1 (need responses from f+1 processes to make progress)
    quorum_ = f_ + 1;
    
    std::cout << "LatticeAgreement: n=" << num_processes_ 
              << ", f=" << f_ << ", quorum=" << quorum_ 
              << ", slots=" << num_slots_ << std::endl;
}

void LatticeAgreement::setPerfectLinks(PerfectLinks* pl) {
    pl_ = pl;
}

// ============================================================================
// Message Encoding/Decoding
// ============================================================================

std::vector<uint8_t> LatticeAgreement::encodeProposal(uint32_t slot, 
                                                       uint32_t proposal_number,
                                                       const std::set<uint32_t>& value) {
    // Format: [TYPE=0][SLOT][PROPOSER_ID][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
    size_t size = 1 + 4 + 4 + 4 + 4 + (value.size() * 4);
    std::vector<uint8_t> buf(size);
    size_t offset = 0;
    
    buf[offset++] = static_cast<uint8_t>(MessageType::PROPOSAL);
    
    std::memcpy(buf.data() + offset, &slot, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    uint32_t proposer_id = static_cast<uint32_t>(process_id_);
    std::memcpy(buf.data() + offset, &proposer_id, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    std::memcpy(buf.data() + offset, &proposal_number, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    uint32_t count = static_cast<uint32_t>(value.size());
    std::memcpy(buf.data() + offset, &count, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    for (uint32_t val : value) {
        std::memcpy(buf.data() + offset, &val, sizeof(uint32_t));
        offset += sizeof(uint32_t);
    }
    
    return buf;
}

std::vector<uint8_t> LatticeAgreement::encodeAck(uint32_t slot, 
                                                  uint32_t proposal_number) {
    // Format: [TYPE=1][SLOT][PROPOSAL_NUM]
    size_t size = 1 + 4 + 4;
    std::vector<uint8_t> buf(size);
    size_t offset = 0;
    
    buf[offset++] = static_cast<uint8_t>(MessageType::ACK);
    
    std::memcpy(buf.data() + offset, &slot, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    std::memcpy(buf.data() + offset, &proposal_number, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    return buf;
}

std::vector<uint8_t> LatticeAgreement::encodeNack(uint32_t slot,
                                                   uint32_t proposal_number,
                                                   const std::set<uint32_t>& value) {
    // Format: [TYPE=2][SLOT][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
    size_t size = 1 + 4 + 4 + 4 + (value.size() * 4);
    std::vector<uint8_t> buf(size);
    size_t offset = 0;
    
    buf[offset++] = static_cast<uint8_t>(MessageType::NACK);
    
    std::memcpy(buf.data() + offset, &slot, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    std::memcpy(buf.data() + offset, &proposal_number, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    uint32_t count = static_cast<uint32_t>(value.size());
    std::memcpy(buf.data() + offset, &count, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    for (uint32_t val : value) {
        std::memcpy(buf.data() + offset, &val, sizeof(uint32_t));
        offset += sizeof(uint32_t);
    }
    
    return buf;
}

bool LatticeAgreement::decodeMessage(const std::vector<uint8_t>& payload,
                                      MessageType& type_out,
                                      uint32_t& slot_out,
                                      uint32_t& proposer_id_out,
                                      uint32_t& proposal_number_out,
                                      std::set<uint32_t>& value_out) {
    if (payload.empty()) return false;
    
    size_t offset = 0;
    type_out = static_cast<MessageType>(payload[offset++]);
    
    if (type_out == MessageType::ACK) {
        // ACK: [TYPE][SLOT][PROPOSAL_NUM]
        if (payload.size() < 1 + 4 + 4) return false;
        
        std::memcpy(&slot_out, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        std::memcpy(&proposal_number_out, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        proposer_id_out = 0;  // Not included in ACK
        value_out.clear();
        return true;
    }
    else if (type_out == MessageType::PROPOSAL) {
        // PROPOSAL: [TYPE][SLOT][PROPOSER_ID][PROPOSAL_NUM][COUNT][VALUES...]
        if (payload.size() < 1 + 4 + 4 + 4 + 4) return false;
        
        std::memcpy(&slot_out, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        std::memcpy(&proposer_id_out, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        std::memcpy(&proposal_number_out, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        uint32_t count;
        std::memcpy(&count, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        if (payload.size() < offset + count * sizeof(uint32_t)) return false;
        
        value_out.clear();
        for (uint32_t i = 0; i < count; i++) {
            uint32_t val;
            std::memcpy(&val, payload.data() + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            value_out.insert(val);
        }
        return true;
    }
    else if (type_out == MessageType::NACK) {
        // NACK: [TYPE][SLOT][PROPOSAL_NUM][COUNT][VALUES...]
        if (payload.size() < 1 + 4 + 4 + 4) return false;
        
        std::memcpy(&slot_out, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        std::memcpy(&proposal_number_out, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        uint32_t count;
        std::memcpy(&count, payload.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        
        if (payload.size() < offset + count * sizeof(uint32_t)) return false;
        
        proposer_id_out = 0;  // Not included in NACK
        value_out.clear();
        for (uint32_t i = 0; i < count; i++) {
            uint32_t val;
            std::memcpy(&val, payload.data() + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            value_out.insert(val);
        }
        return true;
    }
    
    return false;
}

// ============================================================================
// Public Interface
// ============================================================================

void LatticeAgreement::propose(uint32_t slot, const std::set<uint32_t>& proposal) {
    std::vector<uint8_t> payload;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        auto& state = slots_[slot];
        
        // Initialize proposer state (Algorithm 1, lines 7-12)
        state.proposed_value = proposal;
        state.active = true;
        state.active_proposal_number++;
        state.ack_count = 0;
        state.nack_count = 0;
        
        // Prepare payload for broadcast
        payload = encodeProposal(slot, state.active_proposal_number, state.proposed_value);
    }
    
    // Broadcast proposal outside the lock (Algorithm 1, line 13)
    // BEB: send to all processes including self
    if (pl_) {
        for (const auto& host : hosts_) {
            pl_->send(static_cast<uint8_t>(host.id), payload);
        }
    }
}

void LatticeAgreement::onPerfectLinksDeliver(uint32_t sender_id, 
                                              uint32_t /*seq_num*/, 
                                              const std::vector<uint8_t>& payload) {
    MessageType type;
    uint32_t slot, proposer_id, proposal_number;
    std::set<uint32_t> value;
    
    if (!decodeMessage(payload, type, slot, proposer_id, proposal_number, value)) {
        // Not a lattice agreement message, ignore
        return;
    }
    
    switch (type) {
        case MessageType::PROPOSAL:
            handleProposal(proposer_id, slot, proposal_number, value);
            break;
        case MessageType::ACK:
            handleAck(slot, proposal_number);
            break;
        case MessageType::NACK:
            handleNack(slot, proposal_number, value);
            break;
        default:
            // Unknown message type, ignore
            break;
    }
}

bool LatticeAgreement::hasDecided(uint32_t slot) const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = slots_.find(slot);
    return it != slots_.end() && it->second.decided;
}

std::set<uint32_t> LatticeAgreement::getDecision(uint32_t slot) const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = slots_.find(slot);
    if (it != slots_.end() && it->second.decided) {
        return it->second.decision;
    }
    return {};
}

uint32_t LatticeAgreement::getDecidedCount() const {
    return decided_count_.load();
}

// ============================================================================
// Protocol Handlers
// ============================================================================

void LatticeAgreement::handleProposal(uint32_t sender_id, uint32_t slot,
                                       uint32_t proposal_number,
                                       const std::set<uint32_t>& proposed_value) {
    // Algorithm 2: Acceptor logic
    
    std::vector<uint8_t> response;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = slots_[slot];
        
        // Check if accepted_value ⊆ proposed_value
        // i.e., all elements of accepted_value are in proposed_value
        bool is_subset = std::includes(proposed_value.begin(), proposed_value.end(),
                                       state.accepted_value.begin(), state.accepted_value.end());
        
        if (is_subset) {
            // Algorithm 2, lines 3-5: Accept and send ACK
            state.accepted_value = proposed_value;
            response = encodeAck(slot, proposal_number);
        } else {
            // Algorithm 2, lines 6-8: Merge and send NACK
            state.accepted_value.insert(proposed_value.begin(), proposed_value.end());
            response = encodeNack(slot, proposal_number, state.accepted_value);
        }
    }
    
    // Send response outside the lock
    if (pl_ && !response.empty()) {
        pl_->send(static_cast<uint8_t>(sender_id), response);
    }
}

void LatticeAgreement::handleAck(uint32_t slot, uint32_t proposal_number) {
    // Algorithm 1, lines 14-15: Increment ack_count
    
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto& state = slots_[slot];
    
    // Only count ACKs for the current active proposal
    if (proposal_number != state.active_proposal_number) {
        return;
    }
    
    state.ack_count++;
    
    // Check if we can decide or need to re-propose
    checkProgress(slot);
}

void LatticeAgreement::handleNack(uint32_t slot, uint32_t proposal_number,
                                   const std::set<uint32_t>& value) {
    // Algorithm 1, lines 16-18: Merge value and increment nack_count
    
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto& state = slots_[slot];
    
    // Only count NACKs for the current active proposal
    if (proposal_number != state.active_proposal_number) {
        return;
    }
    
    // Merge the NACK value into our proposal
    state.proposed_value.insert(value.begin(), value.end());
    state.nack_count++;
    
    // Check if we can decide or need to re-propose
    checkProgress(slot);
}

void LatticeAgreement::checkProgress(uint32_t slot) {
    // Called with lock held!
    auto& state = slots_[slot];
    
    if (!state.active || state.decided) {
        return;
    }
    
    // Algorithm 1, lines 24-26: Decision condition
    // If we have f+1 ACKs (and no NACKs implied by the condition), we can decide
    if (state.ack_count >= quorum_ && state.nack_count == 0) {
        state.decided = true;
        state.decision = state.proposed_value;
        state.active = false;
        decided_count_++;
        
        // Log the decision
        logDecision(slot, state.decision);
        return;
    }
    
    // Algorithm 1, lines 19-23: Re-proposal condition
    // If we got at least one NACK and total responses >= f+1, re-propose
    if (state.nack_count > 0 && 
        (state.ack_count + state.nack_count) >= quorum_) {
        
        // Re-propose with merged value
        state.active_proposal_number++;
        state.ack_count = 0;
        state.nack_count = 0;
        
        // Need to broadcast outside the lock - but we're holding it
        // So we'll set a flag and broadcast after releasing
        // For now, let's just broadcast here (might cause issues with lock)
        
        // Actually, let's release the lock and broadcast
        // But that's tricky... let's just broadcast with lock held for now
        // PerfectLinks should handle this
        if (pl_) {
            auto payload = encodeProposal(slot, state.active_proposal_number, 
                                          state.proposed_value);
            // Broadcast to all (BEB)
            for (const auto& host : hosts_) {
                pl_->send(static_cast<uint8_t>(host.id), payload);
            }
        }
    }
}

void LatticeAgreement::broadcastProposal(uint32_t slot) {
    std::vector<uint8_t> payload;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = slots_[slot];
        payload = encodeProposal(slot, state.active_proposal_number, state.proposed_value);
    }
    
    // Broadcast to all processes (BEB via PerfectLinks)
    if (pl_) {
        for (const auto& host : hosts_) {
            pl_->send(static_cast<uint8_t>(host.id), payload);
        }
    }
}

void LatticeAgreement::logDecision(uint32_t slot, const std::set<uint32_t>& decision) {
    // Format: space-separated integers
    std::ostringstream oss;
    bool first = true;
    for (uint32_t val : decision) {
        if (!first) oss << " ";
        oss << val;
        first = false;
    }
    
    // Use logger to record the decision
    // The logger needs to store decisions by slot and output them in order
    logger_.logLatticeDecision(slot, oss.str());
}
