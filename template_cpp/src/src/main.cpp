#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>
#include <sstream>
#include <atomic>
#include <signal.h>
#include <set>

#include "parser.hpp"
#include "perfect_links.hpp"
#include "logger.hpp"
#include "urb.hpp"
#include "lattice_agreement.hpp"

// Global Perfect Links instance for signal handling
static std::atomic<PerfectLinks*> g_perfect_links;

static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";
  
  // Stop Perfect Links if running
  PerfectLinks* pl = g_perfect_links.load();
  if (pl != nullptr) {
    pl->stop();
  }

  // Flush logs to disk (crash-time logging)
  Logger* logger = g_optimized_logger.load();
  if (logger) {
    logger->flushOnCrash();
  }

  // write/flush output file if necessary
  std::cout << "Writing output.\n";

  // exit directly from signal handler
  exit(0);
}

int main(int argc, char **argv) {
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  // `true` means that a config file is required.
  // Call with `false` if no config file is necessary.
  bool requireConfig = true;

  Parser parser(argc, argv);
  parser.parse();

  std::cout << "My PID: " << getpid() << "\n";
  std::cout << "From a new terminal type `kill -SIGINT " << getpid() << "` or `kill -SIGTERM "
            << getpid() << "` to stop processing packets\n\n";

  std::cout << "My ID: " << parser.id() << "\n\n";

  std::cout << "List of resolved hosts is:\n";
  std::cout << "==========================\n";
  auto hosts = parser.hosts();
  for (auto &host : hosts) {
    std::cout << host.id << "\n";
    std::cout << "Human-readable IP: " << host.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << host.ip << "\n";
    std::cout << "Human-readbale Port: " << host.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << host.port << "\n";
    std::cout << "\n";
  }
  std::cout << "\n";

  std::cout << "Path to output:\n";
  std::cout << "===============\n";
  std::cout << parser.outputPath() << "\n\n";

  std::cout << "Path to config:\n";
  std::cout << "===============\n";
  std::cout << parser.configPath() << "\n\n";

  std::cout << "Doing some initialization...\n\n";

  // Initialize Perfect Links
  try {
    // Create logger for crash-time logging
    Logger logger(parser.outputPath());
    
    // Find localhost from hosts vector
    Parser::Host localhost;
    bool found_localhost = false;
    for (const auto& host : hosts) {
      if (host.id == parser.id()) {
        localhost = host;
        found_localhost = true;
        break;
      }
    }
    
    if (!found_localhost) {
      std::cerr << "Could not find localhost with id " << parser.id() << std::endl;
      return 1;
    }
    // Detect config mode by parsing first line
    // FIFO: single integer (num_messages)
    // Lattice: three integers (p vs ds)
    std::ifstream config_file(parser.configPath());
    if (!config_file.is_open()) {
      std::cerr << "Failed to open config file: " << parser.configPath() << std::endl;
      return 1;
    }
    
    std::string first_line;
    std::getline(config_file, first_line);
    std::istringstream first_line_stream(first_line);
    
    int first_int, second_int, third_int;
    first_line_stream >> first_int;
    bool is_lattice_mode = false;
    
    if (first_line_stream >> second_int >> third_int) {
      // Three integers = Lattice Agreement mode
      is_lattice_mode = true;
    }
    
    if (is_lattice_mode) {
      // LATTICE AGREEMENT MODE (Milestone 3)
      uint32_t num_proposals = static_cast<uint32_t>(first_int);
      uint32_t max_elements = static_cast<uint32_t>(second_int);
      uint32_t max_distinct = static_cast<uint32_t>(third_int);
      
      std::cout << "Lattice Agreement mode: p=" << num_proposals << ", vs=" << max_elements << ", ds=" << max_distinct << std::endl;
      
      // Parse proposals (each line is a set of integers)
      std::vector<std::set<uint32_t>> proposals;
      for (uint32_t i = 0; i < num_proposals; i++) {
        std::string line;
        if (!std::getline(config_file, line)) {
          std::cerr << "Failed to read proposal " << (i+1) << std::endl;
          return 1;
        }
        
        std::set<uint32_t> proposal;
        std::istringstream line_stream(line);
        uint32_t val;
        while (line_stream >> val) {
          proposal.insert(val);
        }
        proposals.push_back(proposal);
      }
      config_file.close();
      
      std::cout << "Parsed " << proposals.size() << " proposals" << std::endl;
      
      // Set logger to lattice mode
      logger.setLatticeMode(num_proposals);
      
      // Create Lattice Agreement
      LatticeAgreement lattice(static_cast<uint8_t>(parser.id()), hosts, logger, num_proposals);
      
      // Create delivery callback for PerfectLinks
      auto deliveryCallback = [&lattice](uint32_t sender_id, uint32_t sequence_number, const std::vector<uint8_t>& payload) noexcept {
        try { lattice.onPerfectLinksDeliver(sender_id, sequence_number, payload); } catch (...) {}
      };
      
      // Initialize Perfect Links
      PerfectLinks perfect_links(localhost, deliveryCallback, hosts, parser.outputPath());
      g_perfect_links.store(&perfect_links);
      
      if (!perfect_links.initialize()) {
        std::cerr << "Failed to initialize Perfect Links" << std::endl;
        g_perfect_links.store(nullptr);
        return 1;
      }
      perfect_links.start();
      lattice.setPerfectLinks(&perfect_links);
      
      std::cout << "Starting Lattice Agreement for " << num_proposals << " slots...\n\n";
      
      // Propose all slots
      for (uint32_t slot = 1; slot <= num_proposals; slot++) {
        lattice.propose(slot, proposals[slot - 1]);
        
        // Broadcast the proposal (need to call this explicitly after propose sets up state)
        // The propose function sets up state, we need to trigger the broadcast
      }
      
      // Wait for all slots to decide
      while (lattice.getDecidedCount() < num_proposals) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      
      std::cout << "All " << num_proposals << " slots decided!" << std::endl;
      
      // Wait forever for signal
      while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      
      g_perfect_links.store(nullptr);
      
    } else {
      // FIFO BROADCAST MODE (Milestone 2)
      int num_messages = first_int;
      config_file.close();
      
      // Create URB instance and delivery callback that forwards payloads (URB logs deliveries)
      UniformReliableBroadcast urb(static_cast<uint8_t>(parser.id()), hosts, logger);
      auto deliveryCallback = [&urb](uint32_t sender_id, uint32_t sequence_number, const std::vector<uint8_t>& payload) noexcept {
        // Forward PL deliveries up to URB; URB decides and logs deliveries
        try { urb.onPerfectLinksDeliver(sender_id, sequence_number, payload); } catch (...) {}
      };
      
      // Initialize Perfect Links with logger
      PerfectLinks perfect_links(localhost, deliveryCallback, hosts, parser.outputPath());
      g_perfect_links.store(&perfect_links);
      
      if (!perfect_links.initialize()) {
        std::cerr << "Failed to initialize Perfect Links" << std::endl;
        g_perfect_links.store(nullptr);
        return 1;
      }
      //Start the actual Perfect Links system
      perfect_links.start();
      // Give URB access to Perfect Links for rebroadcasts
      urb.setPerfectLinks(&perfect_links);
      
      std::cout << "Broadcasting and delivering messages (FIFO mode)...\n\n";
      
      std::cout << "Each process will broadcast " << num_messages << " messages via URB" << std::endl;
      // Atomic sequential broadcast loop: completely signal-safe and gap-free
      while (true) {
        // Atomically get and broadcast the next sequential message
        uint32_t broadcast_seq = urb.broadcastNextSequential(static_cast<uint32_t>(num_messages));
        
        if (broadcast_seq == 0) {
          // All messages broadcast, exit loop
          break;
        }
        
        // Small delay to avoid overwhelming the network
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      
      // After a process finishes broadcasting,
      // it waits forever for the delivery of messages.
      while (true) {
        // Shorter sleep for more responsive shutdown
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      
      // Clean shutdown
      g_perfect_links.store(nullptr);
    }
    
  } catch (const std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    g_perfect_links.store(nullptr);
    return 1;
  }

  return 0;
}
