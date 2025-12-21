#pragma once

#include <cstdint>
#include <vector>
#include <set>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <functional>
#include <string>

#include "parser.hpp"

class PerfectLinks;
class Logger;

/**
 * Multi-shot Lattice Agreement implementation
 * 
 * Each slot runs independent single-shot lattice agreement using the algorithm from the project pseudo code.
 * Uses BEB (via PerfectLinks broadcast) for proposals and point-to-point for ACK/NACK responses.
 * 
 * Properties:
 * - LA1 Validity: Decision includes proposal and only proposed values
 * - LA2 Consistency: Decisions are comparable (subset relationship)
 * - LA3 Termination: All correct processes eventually decide
 */
class LatticeAgreement {
public:
    // Message types for the protocol
    enum class MessageType : uint8_t {
        PROPOSAL = 0,
        ACK = 1,
        NACK = 2
    };

    /**
     * Constructor
     * @param process_id This process's ID
     * @param hosts List of all hosts in the system
     * @param logger Logger for output
     * @param num_slots Number of slots (proposals) to run
     */
    LatticeAgreement(uint8_t process_id, const std::vector<Parser::Host>& hosts, Logger& logger, uint32_t num_slots);

    /**
     * Set the PerfectLinks instance for communication
     */
    void setPerfectLinks(PerfectLinks* pl);

    /**
     * Propose a set of values for a specific slot
     * @param slot The slot number (1-indexed)
     * @param proposal The set of values to propose
     */
    void propose(uint32_t slot, const std::set<uint32_t>& proposal);

    /**
     * Callback from PerfectLinks when a message is delivered
     * Routes to appropriate handler based on message type
     */
    void onPerfectLinksDeliver(uint32_t sender_id, uint32_t seq_num, const std::vector<uint8_t>& payload);

    /**
     * Get total number of slots that have decided
     */
    uint32_t getDecidedCount() const;

    /**
     * Get total number of slots
     */
    uint32_t getNumSlots() const { return num_slots_; }

private:
    /**
     * Per-slot state for single-shot lattice agreement
     * Each slot has both proposer and acceptor state
     */
    struct SlotState {
        // Proposer state
        bool active{false};
        uint32_t ack_count{0};
        uint32_t nack_count{0};
        uint32_t active_proposal_number{0};
        std::set<uint32_t> proposed_value;
        
        // Acceptor state
        std::set<uint32_t> accepted_value;
        
        // Track which proposal numbers we've already responded to from each proposer to avoid duplicate responses
        std::unordered_map<uint32_t, uint32_t> last_responded_proposal;
        
        // Decision state
        bool decided{false};
        std::set<uint32_t> decision;
    };

    // Message encoding/decoding
    
    /**
     * Encode a PROPOSAL message
     * Format: [TYPE][SLOT][PROPOSER_ID][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
     */
    std::vector<uint8_t> encodeProposal(uint32_t slot, uint32_t proposal_number, const std::set<uint32_t>& value);
    
    /**
     * Encode an ACK message
     * Format: [TYPE][SLOT][PROPOSAL_NUM]
     */
    std::vector<uint8_t> encodeAck(uint32_t slot, uint32_t proposal_number);
    
    /**
     * Encode a NACK message
     * Format: [TYPE][SLOT][PROPOSAL_NUM][COUNT][VAL1][VAL2]...
     */
    std::vector<uint8_t> encodeNack(uint32_t slot, uint32_t proposal_number, const std::set<uint32_t>& value);
    
    /**
     * Decode any lattice agreement message
     * @return true if decoding successful
     */
    bool decodeMessage(const std::vector<uint8_t>& payload, MessageType& type_out, uint32_t& slot_out, uint32_t& proposer_id_out, uint32_t& proposal_number_out, std::set<uint32_t>& value_out);

    // Protocol handlers 
    
    /**
     * Handle incoming PROPOSAL message (acceptor role)
     */
    void handleProposal(uint32_t sender_id, uint32_t slot, uint32_t proposal_number, const std::set<uint32_t>& value);
    
    /**
     * Handle incoming ACK message (proposer role)
     */
    void handleAck(uint32_t slot, uint32_t proposal_number);
    
    /**
     * Handle incoming NACK message (proposer role)
     */
    void handleNack(uint32_t slot, uint32_t proposal_number, const std::set<uint32_t>& value);


    /**
     * Check progress and trigger decision or re-proposal if conditions are met
     * Called after receiving ACK or NACK
     */
    void checkProgress(uint32_t slot);

    /**
     * Log a decision for a slot
     */
    void logDecision(uint32_t slot, const std::set<uint32_t>& decision);

    // === Member variables ===
    
    uint8_t process_id_;
    std::vector<Parser::Host> hosts_;
    PerfectLinks* pl_{nullptr};
    Logger& logger_;
    uint32_t num_slots_;
    
    // Number of processes and quorum size
    uint32_t num_processes_;
    uint32_t f_;       // Max faulty: (n-1)/2
    uint32_t quorum_;  // f + 1 (majority of non-faulty)

    // Per-slot state
    std::unordered_map<uint32_t, SlotState> slots_;
    mutable std::mutex state_mutex_;
    
    // Track number of decided slots
    std::atomic<uint32_t> decided_count_{0};
};
