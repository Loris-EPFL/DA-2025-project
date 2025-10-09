#include "host_utils.hpp"
#include <stdexcept>
#include <fstream>
#include <iostream>
#include <mutex>
#include <memory>

namespace HostUtils {
    
    std::map<unsigned long, Parser::Host> createIdToPeerMap(const std::vector<Parser::Host>& hosts) {
        std::map<unsigned long, Parser::Host> idToPeer;
        
        for (const auto& host : hosts) {
            idToPeer[host.id] = host;
        }
        
        return idToPeer;
    }
    
    Parser::Host findLocalhost(const std::vector<Parser::Host>& hosts, unsigned long process_id) {
        for (const auto& host : hosts) {
            if (host.id == process_id) {
                return host;
            }
        }
        
        throw std::runtime_error("Localhost with ID " + std::to_string(process_id) + " not found in hosts list");
    }
    
    std::function<void(uint32_t, uint32_t)> createDeliveryCallback(const std::string& output_path) {
        // Return a no-op callback since logging is now handled internally by PerfectLinks
        return [](uint32_t sender_id, uint32_t sequence_number) noexcept {
            // No-op: logging is handled internally by PerfectLinks class
        };
    }
    
    bool isValidProcessId(const std::vector<Parser::Host>& hosts, unsigned long process_id) {
        for (const auto& host : hosts) {
            if (host.id == process_id) {
                return true;
            }
        }
        return false;
    }
}