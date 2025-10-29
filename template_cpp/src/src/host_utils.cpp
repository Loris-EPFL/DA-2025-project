#include "host_utils.hpp"
#include <stdexcept>
#include <fstream>
#include <iostream>
#include <mutex>
#include <memory>

namespace HostUtils {
    
    std::map<uint8_t, Parser::Host> createIdToPeerMap(const std::vector<Parser::Host>& hosts) {
        std::map<uint8_t, Parser::Host> idToPeer;
        
        for (const auto& host : hosts) {
            idToPeer[static_cast<uint8_t>(host.id)] = host;
        }
        
        return idToPeer;
    }
    
    Parser::Host findLocalhost(const std::vector<Parser::Host>& hosts, uint8_t process_id) {
        for (const auto& host : hosts) {
            if (host.id == process_id) {
                return host;
            }
        }
        
        throw std::runtime_error("Localhost with ID " + std::to_string(static_cast<unsigned int>(process_id)) + " not found in hosts list");
    }
    
    std::function<void(uint32_t, uint32_t)> createDeliveryCallback(const std::string& output_path) {
        // Create a shared output file stream that will be managed by the callback
        auto output_file = std::make_shared<std::ofstream>(output_path);
        auto output_mutex = std::make_shared<std::mutex>();
        
        if (!output_file->is_open()) {
            throw std::runtime_error("Failed to open output file: " + output_path);
        }
        
        return [=](uint32_t sender_id, uint32_t sequence_number) noexcept {
            try {
                std::lock_guard<std::mutex> lock(*output_mutex);
                *output_file << "d " << sender_id << " " << sequence_number << std::endl;
                output_file->flush();
            } catch (...) {
                // Silently ignore errors in noexcept lambda
            }
        };
    }
    
    bool isValidProcessId(const std::vector<Parser::Host>& hosts, uint8_t process_id) {
        for (const auto& host : hosts) {
            if (host.id == process_id) {
                return true;
            }
        }
        return false;
    }
}