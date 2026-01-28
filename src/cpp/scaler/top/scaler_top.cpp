#include "scaler_top.h"

#include <chrono>
#include <iostream>

#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>

#include "protocol/message_deserializer.h"

namespace scaler::top {

ScalerTop::ScalerTop(const std::string& monitorAddress, int timeoutSeconds)
    : _address(monitorAddress), _timeout(timeoutSeconds), _screen(std::make_unique<TopScreen>()) {}

ScalerTop::~ScalerTop() { stop(); }

void ScalerTop::stop() {
    // Ensure stop() is only executed once
    bool expected = false;
    if (!_stopped.compare_exchange_strong(expected, true)) {
        return;
    }

    _running.store(false);

    // Stop the subscriber to unblock any pending recv operations
    if (_subscriber) {
        _subscriber->requestStop();
    }

    // Wait for network thread to finish before touching _screen
    if (_networkThread.joinable()) {
        _networkThread.join();
    }

    // Now safe to request quit on screen (network thread is done)
    if (_screen) {
        _screen->requestQuit();
    }
}

void ScalerTop::run(ScreenSetupCallback onScreenReady) {
    // Create and connect YMQ subscriber
    _subscriber = std::make_unique<YMQSubscriber>(_address, _timeout * 1000);
    if (!_subscriber->connect()) {
        throw std::runtime_error("Failed to connect to " + _address);
    }

    // Start network thread
    _networkThread = std::thread([this] { networkLoop(); });

    // Run FTXUI screen
    auto screen = ftxui::ScreenInteractive::Fullscreen();
    _screen->setScreen(&screen);

    // Hide cursor (ANSI escape sequence)
    std::cout << "\033[?25l" << std::flush;

    // Call the setup callback (e.g., for signal handler setup)
    if (onScreenReady) {
        onScreenReady(&screen);
    }

    auto component = ftxui::CatchEvent(_screen->getComponent(), [this](ftxui::Event event) {
        if (event.is_character()) {
            return _screen->handleKey(event.character()[0]);
        }
        if (event == ftxui::Event::Escape || event == ftxui::Event::Character('q')) {
            _screen->requestQuit();
            return true;
        }
        return false;
    });

    screen.Loop(component);

    // Show cursor again (ANSI escape sequence)
    std::cout << "\033[?25h" << std::flush;

    // Clear the screen reference before cleanup (it's about to go out of scope)
    if (onScreenReady) {
        onScreenReady(nullptr);
    }

    // Clear the screen pointer in TopScreen before local screen goes out of scope
    _screen->setScreen(nullptr);

    // Clean up
    stop();
}

void ScalerTop::networkLoop() {
    while (_running.load()) {
        auto data = _subscriber->recv();

        if (data.empty()) {
            // Timeout or error, continue
            continue;
        }

        processMessage(data);
    }
}

void ScalerTop::processMessage(const std::vector<uint8_t>& payload) {
    auto state = parseStateScheduler(payload);
    if (state.has_value()) {
        _screen->updateState(std::move(state.value()));
    }
}

}  // namespace scaler::top
