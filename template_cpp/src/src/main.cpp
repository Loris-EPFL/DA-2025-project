#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>

#include "parser.hpp"
#include "hello.h"
#include "perfect_links.hpp"
#include "host_utils.hpp"
#include <signal.h>

// Global Perfect Links instance for signal handling
static PerfectLinks* g_perfect_links = nullptr;


static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";
  
  // Stop Perfect Links if running
  if (g_perfect_links != nullptr) {
    g_perfect_links->stop();
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

  try {
    // Create localhost and peer mappings
    auto localhost = HostUtils::findLocalhost(hosts, parser.id());
    auto idToPeer = HostUtils::createIdToPeerMap(hosts);
    auto deliveryCallback = HostUtils::createDeliveryCallback(parser.outputPath());

    PerfectLinks perfect_links(localhost, deliveryCallback, idToPeer, parser.outputPath());
    g_perfect_links = &perfect_links;

    if (!perfect_links.initialize()) {
      std::cerr << "Failed to initialize Perfect Links" << std::endl;
      return 1;
    }

    perfect_links.start();

    std::cout << "Broadcasting and delivering messages...\n\n";

    // Read configuration
    std::ifstream config_file(parser.configPath());
    if (!config_file.is_open()) {
      std::cerr << "Failed to open config file: " << parser.configPath() << std::endl;
      return 1;
    }

    int num_messages, destination_id;
    config_file >> num_messages >> destination_id;
    config_file.close();

    std::cout << "Sending " << num_messages << " messages to process " << destination_id << std::endl;

    // Send messages if this process is a sender
    if (parser.id() == 1 || parser.id() == 3) {
        for (uint32_t i = 1; i <= 10; ++i) {
            perfect_links.send(2, i);  // Send to process 2
        }
    }

    // After a process finishes broadcasting,
    // it waits forever for the delivery of messages.
    while (true) {
      std::this_thread::sleep_for(std::chrono::hours(1));
    }

  } catch (const std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
