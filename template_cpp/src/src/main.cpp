#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>
#include <atomic>
#include <signal.h>

#include "parser.hpp"
#include "perfect_links.hpp"
#include "logger.hpp"
#include "urb.hpp"

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
    // Parse FIFO config: first line is number of messages (m)
    int num_messages = 0;
    {
      std::ifstream config_file(parser.configPath());
      if (!config_file.is_open()) {
        std::cerr << "Failed to open config file: " << parser.configPath() << std::endl;
        return 1;
      }
      if (!(config_file >> num_messages)) {
        std::cerr << "Invalid FIFO config format (expected single integer m)" << std::endl;
        return 1;
      }
      config_file.close();
    }

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
    // FIFO/URB mode: every process broadcasts 1..m and URB logs deliveries
    for (int i = 1; i <= num_messages; ++i) {
      logger.logBroadcast(static_cast<uint32_t>(i));
      urb.broadcast(static_cast<uint32_t>(i));
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
    
  } catch (const std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    g_perfect_links.store(nullptr);
    return 1;
  }

  return 0;
}
