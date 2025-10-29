#pragma once

#include "parser.hpp"
#include <map>
#include <functional>

namespace HostUtils {
    /**
     * @brief Creates an efficient ID-to-Host mapping from Parser hosts
     * @param hosts Vector of hosts from Parser::hosts()
     * @return Map for O(1) host lookups by ID
     */
    std::map<uint8_t, Parser::Host> createIdToPeerMap(const std::vector<Parser::Host>& hosts);
    
    /**
     * @brief Finds localhost from the hosts list
     * @param hosts Vector of hosts from Parser::hosts()
     * @param process_id The current process ID
     * @return The localhost Host object
     * @throws std::runtime_error if localhost not found
     */
    Parser::Host findLocalhost(const std::vector<Parser::Host>& hosts, uint8_t process_id);
    
    /**
     * @brief Creates a delivery callback function for Perfect Links
     * @param output_path Path to output file for logging
     * @return Function that handles message delivery and logging
     */
    std::function<void(uint32_t, uint32_t)> createDeliveryCallback(const std::string& output_path);
    
    /**
     * @brief Validates that a process ID exists in the hosts list
     * @param hosts Vector of hosts from Parser::hosts()
     * @param process_id The process ID to validate
     * @return true if process ID exists, false otherwise
     */
    bool isValidProcessId(const std::vector<Parser::Host>& hosts, uint8_t process_id);
}