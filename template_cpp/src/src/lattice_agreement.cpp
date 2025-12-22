#include "lattice_agreement.hpp"
#include "perfect_links.hpp"
#include "logger.hpp"
#include <cstring>
#include <algorithm>
#include <iostream>
#include <sstream>

/**
 * Multi-shot Lattice Agreement
 * 
 * Based on Algorithm 1 (Proposer) and Algorithm 2 (Acceptor) from the project description.
 * Each process plays both roles for each slot.
 */

/**
 * Constructor:
 * 
 * sets up the multi-shot lattice agreement for a given process.
 * calculate the fault tolerance parameters based on the number of processes:
 * - With n = 2f + 1 processes, tolerate up to f crashes
 * - A quorum of f + 1 responses is needed to have majority
 * 
 * Each slot runs an independent instance of single-shot lattice agreement, allowing processes to agree on multiple values in sequence.
 * 
 * @param process_id The ID of this process (1 to n)
 * @param hosts List of all hosts in the system
 * @param logger Reference to the logger for recording decisions
 * @param num_slots Number of lattice agreement slots to run
 */
LatticeAgreement::LatticeAgreement(uint8_t process_id,  const std::vector<Parser::Host>& hosts, Logger& logger, uint32_t num_slots) : process_id_(process_id) , hosts_(hosts), logger_(logger), num_slots_(num_slots)
{
    num_processes_ = static_cast<uint32_t>(hosts_.size());
    // Calculate fault tolerance: f = floor((n-1)/2)
    // In a system with n = 2f + 1 processes, up to f can crash
    f_ = (num_processes_ - 1) / 2;
    // Quorum = f + 1 (need responses from f+1 processes to make progress)
    // This ensures we have a majority, which is necessary for consistency
    quorum_ = f_ + 1;
    
    std::cout << "LatticeAgreement: n=" << num_processes_ << ", f=" << f_ << ", quorum=" << quorum_ << ", slots=" << num_slots_ << std::endl;
}

/**
 * Set the Perfect Links instance for message delivery
 * 
 * Lattice agreement uses Perfect Links for:
 * - Best-Effort Broadcast (BEB) for proposals (by sending to all processes)
 * - Point-to-point communication for ACK/NACK responses
 * 
 * @param pl Pointer to the initialized Perfect Links instance
 */
void LatticeAgreement::setPerfectLinks(PerfectLinks* pl) {
    pl_ = pl;
}


// Message Encoding/Decoding


/**
 * Encode a PROPOSAL message for broadcast
 * 
 * A proposal is sent via Best-Effort Broadcast (BEB) to all processes when:
 * 1. A process initially proposes a value (Algorithm 1, line 13 -> trigger beb.broadcast(⟨proposal, proposed_value_i, active_proposal_number_i⟩))
 * 2. A process re-proposes after receiving NACKs (Algorithm 1, line 23 -> trigger beb.broadcast(⟨proposal, proposed_value_i, active_proposal_number_i⟩))
 * 
 * The message format includes all information needed for acceptors to decide
 * whether to accept the proposal or send back a NACK with their accepted value.
 * 
 * Message format: [TYPE=0][SLOT][PROPOSER_ID][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
 * - TYPE: 1 byte identifying this as a PROPOSAL
 * - SLOT: 4 bytes for the slot number (which instance of lattice agreement)
 * - PROPOSER_ID: 4 bytes for who is proposing
 * - PROPOSAL_NUM: 4 bytes for the proposal number (increases on re-proposals)
 * - COUNT: 4 bytes for how many values are in the set
 * - VALUES: COUNT * 4 bytes for the actual integer values
 * 
 * @param slot The slot number for this proposal
 * @param proposal_number The proposal number (increments on retries)
 * @param value The set of values being proposed
 * @return Encoded message ready to send via Perfect Links
 */
std::vector<uint8_t> LatticeAgreement::encodeProposal(uint32_t slot,  uint32_t proposal_number, const std::set<uint32_t>& value) {
    // Format: [TYPE=0][SLOT][PROPOSER_ID][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
    size_t size = 1 + 4 + 4 + 4 + 4 + (value.size() * 4); //Total size needed to be allocated 
    std::vector<uint8_t> buf(size); //Initialize buffer with size
    size_t offset = 0;
    
    // Message type identifier
    buf[offset++] = static_cast<uint8_t>(MessageType::PROPOSAL);
    
    // Which slot this proposal is for (multi-shot support)
    std::memcpy(buf.data() + offset, &slot, sizeof(uint32_t)); // Copy slot number
    offset += sizeof(uint32_t); // Move past slot field 
    
    // Who is making this proposal (needed for sending responses back)
    uint32_t proposer_id = static_cast<uint32_t>(process_id_);
    std::memcpy(buf.data() + offset, &proposer_id, sizeof(uint32_t)); // Copy proposer ID number
    offset += sizeof(uint32_t); // Move past proposer ID field 
    
    // Proposal number (increases on re-proposals to distinguish attempts)
    std::memcpy(buf.data() + offset, &proposal_number, sizeof(uint32_t)); // Copy proposal number
    offset += sizeof(uint32_t); // Move past proposal number field ;
    
    // Number of values in the proposed set
    uint32_t count = static_cast<uint32_t>(value.size());
    std::memcpy(buf.data() + offset, &count, sizeof(uint32_t)); // Copy count of values
    offset += sizeof(uint32_t); // Move past count field 
    
    // The actual values (std::set maintains sorted order)
    //TODO: check if it can overflow from perfect links when too many values
    for (uint32_t val : value) {
        std::memcpy(buf.data() + offset, &val, sizeof(uint32_t)); // Copy each value
        offset += sizeof(uint32_t); // for each value in the set, move past val slot
    }
    
    return buf;
}

/**
 * Encode an ACK message for point-to-point response
 * 
 * An ACK is sent back to the proposer when an acceptor accepts a proposal (Algorithm 2, line 5).
 * This happens when the acceptor's previously accepted value is a subset of the proposed value.
 * 
 * ACKs are much smaller than NACKs because they don't need to include any values, they're just a confirmation that the proposal num was accepted.
 * 
 * Message format: [TYPE=1][SLOT][PROPOSAL_NUM]
 * - TYPE: 1 byte identifying this as an ACK
 * - SLOT: 4 bytes for the slot number
 * - PROPOSAL_NUM: 4 bytes to identify which proposal we're acknowledging
 * 
 * @param slot The slot number for this ACK
 * @param proposal_number The proposal number being acknowledged
 * @return Encoded ACK message ready to send back to proposer
 */
std::vector<uint8_t> LatticeAgreement::encodeAck(uint32_t slot, uint32_t proposal_number) {
    // Format: [TYPE=1][SLOT][PROPOSAL_NUM]
    size_t size = 1 + 4 + 4; // Total size needed to be allocated 
    std::vector<uint8_t> buf(size); //Initialize buffer with size
    size_t offset = 0; 
    
    // Message type identifier
    buf[offset++] = static_cast<uint8_t>(MessageType::ACK); // 1 byte for the message type
    
    // Which slot we're acknowledging
    std::memcpy(buf.data() + offset, &slot, sizeof(uint32_t)); // Store slot number in 4 bytes in the right slot
    offset += sizeof(uint32_t); // Move past slot field
    
    // Which proposal number we're acknowledging
    std::memcpy(buf.data() + offset, &proposal_number, sizeof(uint32_t)); // Store proposal number in 4 bytes
    offset += sizeof(uint32_t); // Move past proposal number field
    
    return buf;
}

/**
 * Encode a NACK message for point-to-point response
 * 
 * A NACK is sent back to the proposer when an acceptor cannot accept a proposal (Algorithm 2, line 8).
 * happens when the acceptor has previously accepted values that are NOT a subset of the proposed value.
 * 
 * The NACK includes the acceptor's merged accepted value (union of proposed and previously accepted).
 * helps the proposer converge faster by learning what other values need to be included in the next proposal.
 * 
 * Message format: [TYPE=2][SLOT][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
 * - TYPE: 1 byte identifying this as a NACK
 * - SLOT: 4 bytes for the slot number
 * - PROPOSAL_NUM: 4 bytes to identify which proposal we're rejecting
 * - COUNT: 4 bytes for how many values are in the merged set
 * - VALUES: COUNT * 4 bytes for the merged accepted values
 * 
 * @param slot The slot number for this NACK
 * @param proposal_number The proposal number being rejected
 * @param value The merged accepted value (what the proposer should include)
 * @return Encoded NACK message ready to send back to proposer
 */
std::vector<uint8_t> LatticeAgreement::encodeNack(uint32_t slot, uint32_t proposal_number, const std::set<uint32_t>& value) {
    // Format: [TYPE=2][SLOT][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
    size_t size = 1 + 4 + 4 + 4 + (value.size() * 4); // Total size needed to be allocated 
    std::vector<uint8_t> buf(size); //Initialize buffer with size
    size_t offset = 0;
    
    // Message type identifier
    buf[offset++] = static_cast<uint8_t>(MessageType::NACK); // 1 byte for the message type
    
    // Which slot we're rejecting
    std::memcpy(buf.data() + offset, &slot, sizeof(uint32_t)); // 4 bytes for the slot number
    offset += sizeof(uint32_t); // Move past slot number field
    
    // Which proposal number we're rejecting
    std::memcpy(buf.data() + offset, &proposal_number, sizeof(uint32_t)); // 4 bytes for the proposal number
    offset += sizeof(uint32_t); // Move past proposal number field
    
    // Number of values in the merged accepted set
    uint32_t count = static_cast<uint32_t>(value.size());
    std::memcpy(buf.data() + offset, &count, sizeof(uint32_t)); // 4 bytes for count
    offset += sizeof(uint32_t); // Move past count field
    
    // The merged accepted values (what should be included in next proposal)
    //TODO: check if it can overflow from perfect links when too many values
    for (uint32_t val : value) {
        std::memcpy(buf.data() + offset, &val, sizeof(uint32_t)); // Copy each value
        offset += sizeof(uint32_t); // for each value in the set, move past val slot
    }
    
    return buf;
}

/**
 * Decode a lattice agreement message from raw bytes
 * 
 * This function parses incoming messages and extracts all relevant information.
 * It handles three message types: PROPOSAL, ACK, and NACK, each with different formats and field requirements.
 * 
 * The function performs validation to ensure:
 * - The payload is not empty
 * - The message type is recognized
 * - The payload has enough bytes for all expected fields
 * 
 * If decoding fails (invalid format or unknown type), we return false and the message is ignored.
 * safe because lattice agreement can tolerate lost messages and the proposer will eventually re-propose if needed.
 * 
 * @param payload Raw bytes received from Perfect Links
 * @param type_out Output: The message type (PROPOSAL, ACK, or NACK)
 * @param slot_out Output: The slot number
 * @param proposer_id_out Output: The proposer's ID (only for PROPOSAL)
 * @param proposal_number_out Output: The proposal number
 * @param value_out Output: The set of values (for PROPOSAL and NACK)
 * @return true if decoding succeeded, false if message is invalid
 */
bool LatticeAgreement::decodeMessage(const std::vector<uint8_t>& payload, MessageType& type_out,  uint32_t& slot_out, uint32_t& proposer_id_out, uint32_t& proposal_number_out, std::set<uint32_t>& value_out) {
    // Basic validation: need at least one byte for message type
    if (payload.empty()) return false;
    
    size_t offset = 0;
    // First byte tells us what kind of message this is
    type_out = static_cast<MessageType>(payload[offset++]);

    switch (type_out) {
        case MessageType::ACK: {
            // ACK format: [TYPE][SLOT][PROPOSAL_NUM]
            // Validate we have enough bytes for all fields
            if (payload.size() < 1 + 4 + 4) return false;
            
            // Extract slot number
            std::memcpy(&slot_out, payload.data() + offset, sizeof(uint32_t));  // Copy slot number
            offset += sizeof(uint32_t); // Move past slot field
            
            // Extract proposal number being acknowledged
            std::memcpy(&proposal_number_out, payload.data() + offset, sizeof(uint32_t)); // Copy proposal number
            offset += sizeof(uint32_t); // Move past proposal number field
            
            // ACKs don't include proposer ID or values
            proposer_id_out = 0;
            value_out.clear();
            return true;
        }
        
        case MessageType::PROPOSAL: {
            // PROPOSAL format: [TYPE][SLOT][PROPOSER_ID][PROPOSAL_NUM][COUNT][VALUES...]
            // Validate we have enough bytes for header fields
            if (payload.size() < 1 + 4 + 4 + 4 + 4) return false;
            
            // Extract slot number
            std::memcpy(&slot_out, payload.data() + offset, sizeof(uint32_t)); // Copy slot number
            offset += sizeof(uint32_t); // Move past slot field
            
            // Extract proposer ID (who is making this proposal)
            std::memcpy(&proposer_id_out, payload.data() + offset, sizeof(uint32_t)); // Copy proposer ID
            offset += sizeof(uint32_t); // Move past proposer ID field
            
            // Extract proposal number (which attempt this is)
            std::memcpy(&proposal_number_out, payload.data() + offset, sizeof(uint32_t)); // Copy proposal number
            offset += sizeof(uint32_t); // Move past proposal number field
            
            // Extract count of values in the proposed set
            uint32_t count;
            std::memcpy(&count, payload.data() + offset, sizeof(uint32_t)); // Copy count
            offset += sizeof(uint32_t); // Move past count field
            
            // Validate we have enough bytes for all the values
            if (payload.size() < offset + count * sizeof(uint32_t)) return false;
            
            // Extract all values into the set (std::set maintains sorted order)
            value_out.clear();
            for (uint32_t i = 0; i < count; i++) {
                uint32_t val; 
                std::memcpy(&val, payload.data() + offset, sizeof(uint32_t)); // Copy each value
                offset += sizeof(uint32_t); // for each value in the set, move past val slot 
                value_out.insert(val); // Insert into the set
            }
            return true;
        }
        
        case MessageType::NACK: {
            // NACK format: [TYPE][SLOT][PROPOSAL_NUM][COUNT][VALUES...]
            // Validate we have enough bytes for header fields
            if (payload.size() < 1 + 4 + 4 + 4) return false;
            
            // Extract slot number
            std::memcpy(&slot_out, payload.data() + offset, sizeof(uint32_t)); // Copy slot number
            offset += sizeof(uint32_t); // Move past slot field
            
            // Extract proposal number being rejected
            std::memcpy(&proposal_number_out, payload.data() + offset, sizeof(uint32_t)); // Copy proposal number
            offset += sizeof(uint32_t); // Move past proposal number field
            
            // Extract count of values in the merged accepted set
            uint32_t count;
            std::memcpy(&count, payload.data() + offset, sizeof(uint32_t)); // Copy count
            offset += sizeof(uint32_t); // Move past count field
            
            // Validate we have enough bytes for all the values
            if (payload.size() < offset + count * sizeof(uint32_t)) return false;
            
            // NACKs don't include proposer ID (we know who sent it from Perfect Links)
            proposer_id_out = 0;
            
            // Extract all values from the merged accepted set
            value_out.clear();
            for (uint32_t i = 0; i < count; i++) {
                uint32_t val; 
                std::memcpy(&val, payload.data() + offset, sizeof(uint32_t)); // Copy each value
                offset += sizeof(uint32_t); // for each value in the set, move past val slot
                value_out.insert(val); // Insert into the set
            }
            return true;
        }
        
        default:
            // Unknown message type. ignore it
            return false;
    }
    // Unknown message type. ignore it
    return false;
}


// Public Interface


/**
 * Propose a value for a specific slot (Algorithm 1, lines 7-13)
 * 
 * the main entry point for starting a lattice agreement instance.
 * 1. Initialize the proposer state for this slot
 * 2. Set the proposal as our proposed value
 * 3. Mark ourselves as active (we're trying to get this value decided)
 * 4. Increment the proposal number (starts at 1 for first proposal)
 * 5. Reset ACK/NACK counters
 * 6. Broadcast the proposal to all processes via BEB
 * 
 * The broadcast uses Best-Effort Broadcast (BEB) implemented by sending to all processes via Perfect Links. 
 * This includes sending to ourselves, which triggers our own acceptor logic.
 * 
 * We prepare the payload inside the lock but broadcast outside to avoid holding the lock during network operations.
 * 
 * @param slot The slot number for this proposal (1 to num_slots)
 * @param proposal The set of values we want to propose
 */
void LatticeAgreement::propose(uint32_t slot, const std::set<uint32_t>& proposal) {
    std::vector<uint8_t> payload;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_); // Need to lock because otherwise we might access shared state concurrently and have race conditions
        
        auto& state = slots_[slot]; // Get reference to the slot state
        
        // Initialize proposer state (Algorithm 1, lines 7-12)
        state.proposed_value = proposal; // Value to be proposed
        state.active = true;  // set state as trying to decide
        state.active_proposal_number++;  // Increment for this attempt
        state.ack_count = 0;  // Reset ack count for new proposal
        state.nack_count = 0; // Reset nack count for new proposal
        
        // Prepare the proposal message while holding the lock
        payload = encodeProposal(slot, state.active_proposal_number, state.proposed_value);
    }
    
    // Broadcast proposal outside the lock (Algorithm 1, line 13)
    // BEB: Best-Effort Broadcast via Perfect Links to all processes
    // This includes sending to ourselves to trigger our own acceptor
    if (pl_) {
        for (const auto& host : hosts_) {
            pl_->send(static_cast<uint8_t>(host.id), payload);
        }
    }
}

/**
 * Callback invoked when Perfect Links delivers a message
 * 
 * main message handler for lattice agreement. 
 * Perfect Links calls this function whenever it delivers a message to us.
 * 1. Decode the message to determine its type and extract fields
 * 2. Route it to the appropriate handler based on message type
 * 
 * We ignore messages that fail to decode because they might be from other protocols or corrupted.
 * safe because lattice agreement can tolerate message loss and correct message will eventually be delivered.
 * 
 * The sender_id from Perfect Links tells us who sent the message, but for PROPOSAL messages we also have the proposer_id embedded in the message itself (they should match).
 * For ACK/NACK, we use sender_id to know who responded.
 * 
 * Note: We ignore the seq_num parameter because lattice agreement doesn't need Perfect Links sequence numbers since we have our own proposal numbers.
 * 
 * @param sender_id The ID of the process that sent this message (from Perfect Links)
 * @param seq_num The Perfect Links sequence number (unused)
 * @param payload The raw message bytes to decode and process
 */
void LatticeAgreement::onPerfectLinksDeliver(uint32_t sender_id, uint32_t seq_num, const std::vector<uint8_t>& payload) { 
    MessageType type;
    uint32_t slot, proposer_id, proposal_number;
    std::set<uint32_t> value;
    
    // Try to decode the message. if it fails, it's not for us
    if (!decodeMessage(payload, type, slot, proposer_id, proposal_number, value)) {
        // Not a valid lattice agreement message, ignore it
        // This could be a message from another protocol or corrupted data
        return;
    }
    
    // Route to appropriate handler based on message type
    switch (type) {
        case MessageType::PROPOSAL:
            // Someone is proposing a value - act as acceptor
            handleProposal(proposer_id, slot, proposal_number, value);
            break;
        case MessageType::ACK:
            // Someone accepted our proposal - count it
            handleAck(slot, proposal_number);
            break;
        case MessageType::NACK:
            // Someone rejected our proposal - merge their value and retry
            handleNack(slot, proposal_number, value);
            break;
        default:
            // Unknown message type (shouldn't happen after successful decode)
            break;
    }
}

/**
 * Get the total number of slots that have decided
 * 
 * This is a lock-free atomic counter that tracks how many slots have completed.
 * Useful for progress monitoring and knowing when all slots are done (when count == num_slots_).
 * 
 * @return The number of slots that have made decisions
 */
uint32_t LatticeAgreement::getDecidedCount() const {
    return decided_count_.load();
}


// Protocol Handlers


/**
 * Handle a PROPOSAL message (Algorithm 2: Acceptor logic)
 * 
 * When we receive a proposal, we act as an acceptor and decide whether to accept it or reject it based on what we've previously accepted.
 * 
 * The key insight of lattice agreement is the subset check:
 * - If our accepted_value is included in proposed_value: We can accept (send ACK), this means the proposal includes everything we've accepted so far
 * - Otherwise: We must reject (send NACK with merged value), this means we've accepted something the proposer doesn't know about
 * 
 * When we NACK, we merge our accepted value with the proposal and send it back.
 * This helps the proposer converge faster by learning what needs to be included.
 * 
 * We use std::includes to check the subset relationship  because std::set maintains sorted order.
 * 
 * The response is prepared inside the lock but sent outside to avoid holding the lock during network operations.
 * 
 * @param sender_id The ID of the proposer (where to send response)
 * @param slot The slot number for this proposal
 * @param proposal_number The proposal number (for response matching)
 * @param proposed_value The set of values being proposed
 */
void LatticeAgreement::handleProposal(uint32_t sender_id, uint32_t slot, uint32_t proposal_number, const std::set<uint32_t>& proposed_value) {
    // Algorithm 2: Acceptor logic
    
    std::vector<uint8_t> response;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_); // Lock state to prevent race conditions
        auto& state = slots_[slot];
        
        // Check if accepted_value is incuded in proposed_value
        // std::includes checks if all elements of accepted_value are in proposed_value
        // Both sets must be sorted (which std::set guarantees)
        bool is_subset = std::includes(proposed_value.begin(), proposed_value.end(), state.accepted_value.begin(), state.accepted_value.end());
        
        if (is_subset) {
            // Algorithm 2, lines 3-5: Our accepted value is a subset, so we can accept
            state.accepted_value = proposed_value; // Update our accepted value to the proposal (it's larger/equal)
           
            response = encodeAck(slot, proposal_number);  // Send ACK back to the proposer
        } else {
            // Algorithm 2, lines 6-8: Our accepted value is NOT a subset, must reject
            state.accepted_value.insert(proposed_value.begin(), proposed_value.end()); // Merge the proposal into our accepted value (union operation)
            
            response = encodeNack(slot, proposal_number, state.accepted_value); // Send NACK with the merged value so proposer knows what to include
        }
    }
    
    // Send response outside the lock to avoid holding lock during network I/O
    if (pl_ && !response.empty()) {
        pl_->send(static_cast<uint8_t>(sender_id), response);
    }
}

/**
 * Handle an ACK message (Algorithm 1, lines 14-15)
 * 
 * When we receive an ACK, it means an acceptor has accepted our proposal.
 * We increment the ACK counter and check if we have enough ACKs to decide.
 * 
 * We only count ACKs for our CURRENT active proposal number.
 * If we receive an ACK for an old proposal (because we already re-proposed), we ignore it, preventing counting stale responses.
 * 
 * After incrementing the counter, we call checkProgress to see if:
 * - We have f+1 ACKs with no NACKs -> decide
 * - We have f+1 total responses with some NACKs -> re-propose
 * 
 * @param slot The slot number for this ACK
 * @param proposal_number The proposal number being acknowledged
 */
void LatticeAgreement::handleAck(uint32_t slot, uint32_t proposal_number) {
    // Algorithm 1, lines 14-15: Increment ack_count
    std::lock_guard<std::mutex> lock(state_mutex_); // Lock state to prevent race conditions
    auto& state = slots_[slot];
    
    // Only count ACKs for the current active proposal
    if (proposal_number != state.active_proposal_number) {
        return; // Ignore ACKs for old proposals (we may have already re-proposed)
    }
    
    state.ack_count++;
    
    // Check if we can decide or need to re-propose
    checkProgress(slot);
}

/**
 * Handle a NACK message (Algorithm 1, lines 16-18)
 * 
 * When we receive a NACK, it means an acceptor rejected our proposal because they've accepted values we didn't include. 
 * The NACK contains their merged accepted value, which tells us what we need to include in the next proposal.
 * 
 * 1. Merge the NACK value into our proposed value (union operation)
 * 2. Increment the NACK counter
 * 3. Check if we have enough responses to re-propose
 * 
 * We only count NACKs for our CURRENT active proposal number,and stale NACKs from old proposals are ignored.
 * 
 * The merge operation ensures our next proposal will include all values that any acceptor has seen, helping us converge toward a decision.
 * 
 * @param slot The slot number for this NACK
 * @param proposal_number The proposal number being rejected
 * @param value The merged accepted value from the acceptor
 */
void LatticeAgreement::handleNack(uint32_t slot, uint32_t proposal_number, const std::set<uint32_t>& value) {
    // Algorithm 1, lines 16-18: Merge value and increment nack_count
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto& state = slots_[slot];
    
    // Only count NACKs for the current active proposal
    if (proposal_number != state.active_proposal_number) {
        return;    // Ignore NACKs for old proposals (we may have already re-proposed)
    }
    
    state.proposed_value.insert(value.begin(), value.end());     // Merge the NACK value into our proposal (union operation)
    // This ensures our next proposal includes all values any acceptor has seen
    state.nack_count++;
    
    // Check if we can decide or need to re-propose
    checkProgress(slot);
}

/**
 * Check if we can make progress (decide or re-propose)
 * 
 * core decision logic of the lattice agreement algorithm.
 * called after receiving each ACK or NACK to check if we've reached conditions:
 * 
 * 1. DECISION CONDITION (Algorithm 1, lines 24-26):
 *    - We have f+1 ACKs AND no NACKs
 *    - means a quorum accepted our proposal without conflicts
 *    - safely decide on our proposed value
 * 
 * 2. RE-PROPOSAL CONDITION (Algorithm 1, lines 19-23):
 *    - We have at least one NACK
 *    - We have f+1 total responses (ACKs + NACKs)
 *    - means a quorum responded, but there were conflicts
 *    - re-propose with the merged value
 * 
 * The re-proposal increments the proposal number and resets counters.
 * broadcast the new proposal with the merged value, which now includes all values that any acceptor has seen.
 * 
 * Note: This function is called with state_mutex_ already held by the caller.
 * We broadcast while holding the lock, which is safe because Perfect Links handles its own locking.
 * 
 * @param slot The slot number to check progress for
 */
void LatticeAgreement::checkProgress(uint32_t slot) {
    // Called with lock held by caller
    auto& state = slots_[slot];
    
    // Don't do anything if we're not active or already decided
    if (!state.active || state.decided) {
        return;
    }
    
    // Algorithm 1, lines 24-26: Decision condition
    // If we have f+1 ACKs with NO NACKs, we can decide
    // This means a quorum accepted our proposal without any conflicts
    if (state.ack_count >= quorum_ && state.nack_count == 0) {
        state.decided = true; // We successfully reached consensus
        state.decision = state.proposed_value; // Value of the consensus
        state.active = false;  // No longer actively proposing
        decided_count_++;  // Atomic increment of global counter
        
        // Log the decision to output file
        logDecision(slot, state.decision);
        return;
    }
    
    // Algorithm 1, lines 19-23: Re-proposal condition
    // If we got at least one NACK and have f+1 total responses, re-propose
    // This means we have a quorum but there were conflicts that need resolving
    if (state.nack_count > 0 && (state.ack_count + state.nack_count) >= quorum_) {
        
        // Prepare for re-proposal with merged value
        state.active_proposal_number++;  // Increment to distinguish from previous attempt
        state.ack_count = 0;  // Reset ack counters for new proposal
        state.nack_count = 0; // Reset nack counters for new proposal
        
        // Broadcast the new proposal with merged value
        // We hold the lock during broadcast, which is safe because Perfect Links uses its own locking and won't cause deadlock
        if (pl_) {
            auto payload = encodeProposal(slot, state.active_proposal_number, state.proposed_value);
            // Broadcast to all processes (BEB)using Perfect Links
            for (const auto& host : hosts_) {
                pl_->send(static_cast<uint8_t>(host.id), payload);
            }
        }
    }
}

/**
 * Log a decision to the output file
 * 
 * When a decision is made, we record it in the output file:
 * - Space-separated integers
 * - Sorted in ascending order (std::set guarantees this)
 * - One line per decision
 * - Decisions must appear in slot order (1, 2, 3, ...)
 * 
 * @param slot The slot number for this decision
 * @param decision The decided set of values (already sorted by std::set)
 */
void LatticeAgreement::logDecision(uint32_t slot, const std::set<uint32_t>& decision) {
    // Format as space-separated integers
    std::ostringstream oss;
    bool first = true;
    for (uint32_t val : decision) {
        if (!first) oss << " ";  // Add space before each value except first
        oss << val;
        first = false;
    }
    
    // Pass the formatted decision to the logger
    // The logger stores it by slot and ensures it's written in order
    logger_.logLatticeDecision(slot, oss.str());
}
