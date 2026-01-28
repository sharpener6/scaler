#include <csignal>
#include <iostream>
#include <string>

#include <ftxui/component/screen_interactive.hpp>

#include "scaler_top.h"

// Static screen pointer for signal handler - Exit() is signal-safe in FTXUI
static ftxui::ScreenInteractive* G_SCREEN = nullptr;

static const int DEFAULT_TIMEOUT_SECONDS = 5;

void signalHandler(int) {
    // FTXUI's Exit() is signal-safe (writes to a self-pipe)
    if (G_SCREEN) {
        G_SCREEN->Exit();
    }
}

void printUsage(const char* programName) {
    std::cerr << "Usage: " << programName << " <monitor_address> [-t timeout]\n";
    std::cerr << "Example: " << programName << " tcp://127.0.0.1:2347\n";
    std::cerr << "\n";
    std::cerr << "Options:\n";
    std::cerr << "  -t timeout  Timeout in seconds (default: 5)\n";
    std::cerr << "\n";
    std::cerr << "Keyboard shortcuts:\n";
    std::cerr << "  g  Sort by group\n";
    std::cerr << "  n  Sort by worker name\n";
    std::cerr << "  C  Sort by agent CPU\n";
    std::cerr << "  M  Sort by agent RSS\n";
    std::cerr << "  c  Sort by task CPU\n";
    std::cerr << "  m  Sort by task RSS\n";
    std::cerr << "  F  Sort by OS free RSS\n";
    std::cerr << "  f  Sort by free slots\n";
    std::cerr << "  w  Sort by sent\n";
    std::cerr << "  d  Sort by queued\n";
    std::cerr << "  s  Sort by suspended\n";
    std::cerr << "  l  Sort by lag\n";
    std::cerr << "  q  Quit\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printUsage(argv[0]);
        return 1;
    }

    std::string address = argv[1];

    if (address == "-h" || address == "--help") {
        printUsage(argv[0]);
        return 0;
    }

    int timeout = DEFAULT_TIMEOUT_SECONDS;

    for (int i = 2; i < argc; ++i) {
        if (std::string(argv[i]) == "-t" && i + 1 < argc) {
            try {
                timeout = std::stoi(argv[++i]);
                if (timeout <= 0) {
                    std::cerr << "Error: Timeout must be positive\n";
                    return 1;
                }
            } catch (const std::exception&) {
                std::cerr << "Error: Invalid timeout value\n";
                return 1;
            }
        }
    }

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    try {
        scaler::top::ScalerTop app(address, timeout);
        app.run([](ftxui::ScreenInteractive* screen) { G_SCREEN = screen; });
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
