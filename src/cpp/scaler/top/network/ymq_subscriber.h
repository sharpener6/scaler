#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"

namespace scaler::top {

class YMQSubscriber {
public:
    YMQSubscriber(const std::string& address, int timeoutMs = 5000);
    ~YMQSubscriber();

    // Non-copyable, non-movable
    YMQSubscriber(const YMQSubscriber&) = delete;
    YMQSubscriber& operator=(const YMQSubscriber&) = delete;
    YMQSubscriber(YMQSubscriber&&) = delete;
    YMQSubscriber& operator=(YMQSubscriber&&) = delete;

    // Connect to the address
    bool connect();

    // Receive a message (blocking with timeout)
    // Returns empty vector on timeout or error
    std::vector<uint8_t> recv();

    // Request stop (thread-safe)
    void requestStop();

    // Check if stopped
    bool isStopped() const { return _stopped.load(); }

private:
    std::string _address;
    int _timeoutMs;
    std::atomic<bool> _stopped{false};

    scaler::ymq::IOContext _ioContext;
    std::shared_ptr<scaler::ymq::IOSocket> _socket;
};

}  // namespace scaler::top
