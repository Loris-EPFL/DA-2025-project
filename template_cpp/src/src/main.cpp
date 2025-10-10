#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>
#include <atomic>

#include "parser.hpp"
#include "hello.h"
#include "perfect_links.hpp"
#include "host_utils.hpp"
#include <signal.h>

// Global Perfect Links instance for signal handling
static std::atomic<PerfectLinks*> g_perfect_links{nullptr};
static std::atomic<bool> g_shutdown_requested{false};

static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // Set shutdown flag
  g_shutdown_requested.store(true);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";
  
  // Stop Perfect Links if running
  PerfectLinks* pl = g_perfect_links.load();
  if (pl != nullptr) {
    pl->stop();
  }

  // write/flush output file if necessary
  std::cout << "Writing output.\n";

  // DO NOT exit directly from signal handler
  // Let main thread handle cleanup and exit gracefully
}

int main(int argc, char **argv) {
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  // `true` means that a config file is required.
  // Call with `false` if no config file is necessary.
  bool requireConfig = true;

  Parser parser(argc, argv);
  parser.parse();

  hello();
  std::cout << std::endl;

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
    // Use modern constructor with HostUtils helpers
    auto localhost = HostUtils::findLocalhost(hosts, parser.id());
    auto idToPeer = HostUtils::createIdToPeerMap(hosts);
    auto deliveryCallback = HostUtils::createDeliveryCallback(parser.outputPath());
    
    PerfectLinks perfect_links(localhost, deliveryCallback, idToPeer, parser.outputPath());
    g_perfect_links.store(&perfect_links);
    
    if (!perfect_links.initialize()) {
      std::cerr << "Failed to initialize Perfect Links" << std::endl;
      g_perfect_links.store(nullptr);
      return 1;
    }
    
    perfect_links.start();
    
    std::cout << "Broadcasting and delivering messages...\n\n";
    
    // Parse configuration file to get number of messages and destination
    std::ifstream config_file(parser.configPath());
    if (!config_file.is_open()) {
      std::cerr << "Failed to open config file: " << parser.configPath() << std::endl;
      return 1;
    }
    
    int num_messages, destination_id;
    config_file >> num_messages >> destination_id;
    config_file.close();
    
    std::cout << "Sending " << num_messages << " messages to process " << destination_id << std::endl;
    
    // Only sender processes log broadcast events
    // Receiver processes only log delivery events (handled by delivery callback)
    if (parser.id() != static_cast<unsigned long>(destination_id)) {
      // Open output file for logging broadcast events (sender only)
      std::ofstream output_file(parser.outputPath());
      if (!output_file.is_open()) {
        std::cerr << "Failed to open output file: " << parser.outputPath() << std::endl;
        return 1;
      }
      
      // Send messages and log broadcast events
      for (int i = 1; i <= num_messages; ++i) {
        // Log broadcast event
        output_file << "b " << i << std::endl;
        output_file.flush();
        
        perfect_links.send(static_cast<uint8_t>(destination_id), static_cast<uint32_t>(i));
        // Small delay to avoid overwhelming the network
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      
      output_file.close();
    } else {
      // Receiver process: only send messages, delivery logging handled by callback
      for (int i = 1; i <= num_messages; ++i) {
        perfect_links.send(static_cast<uint8_t>(destination_id), static_cast<uint32_t>(i));
        // Small delay to avoid overwhelming the network
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    
    // After a process finishes broadcasting,
    // it waits forever for the delivery of messages.
    while (!g_shutdown_requested.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
