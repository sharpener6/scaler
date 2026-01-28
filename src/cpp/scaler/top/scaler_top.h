#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <ftxui/component/screen_interactive.hpp>

#include "network/ymq_subscriber.h"
#include "ui/top_screen.h"

namespace scaler::top {

// Callback type for screen setup (e.g., for signal handler registration)
using ScreenSetupCallback = std::function<void(ftxui::ScreenInteractive*)>;

class ScalerTop {
public:
    ScalerTop(const std::string& monitorAddress, int timeoutSeconds);
    ~ScalerTop();

    // Run the application (blocking)
    // Optional callback is called after screen creation, before the main loop
    void run(ScreenSetupCallback onScreenReady = nullptr);

    // Request shutdown
    void stop();

private:
    std::string _address;
    int _timeout;
    std::atomic<bool> _running{true};
    std::atomic<bool> _stopped{false};

    // YMQ network subscriber
    std::unique_ptr<YMQSubscriber> _subscriber;

    // UI
    std::unique_ptr<TopScreen> _screen;

    // Threading
    std::thread _networkThread;

    void networkLoop();
    void processMessage(const std::vector<uint8_t>& payload);
};

}  // namespace scaler::top
