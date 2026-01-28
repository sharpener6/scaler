#include "ymq_subscriber.h"

#include <iostream>

#include "scaler/ymq/simple_interface.h"

namespace scaler::top {

YMQSubscriber::YMQSubscriber(const std::string& address, int timeoutMs) : _address(address), _timeoutMs(timeoutMs), _ioContext(1) {}

YMQSubscriber::~YMQSubscriber() {
    requestStop();
}

bool YMQSubscriber::connect() {
    try {
        _socket = scaler::ymq::syncCreateSocket(_ioContext, scaler::ymq::IOSocketType::Multicast, "scaler_top_monitor");
        scaler::ymq::syncConnectSocket(_socket, _address);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to connect to " << _address << ": " << e.what() << "\n";
        return false;
    }
}

std::vector<uint8_t> YMQSubscriber::recv() {
    if (_stopped.load() || !_socket) {
        return {};
    }

    auto result = scaler::ymq::syncRecvMessage(_socket);
    if (!result.has_value()) {
        return {};
    }

    const auto& payload = result.value().payload;
    if (payload.is_null() || payload.size() == 0) {
        return {};
    }

    return std::vector<uint8_t>(payload.data(), payload.data() + payload.size());
}

void YMQSubscriber::requestStop() {
    _stopped.store(true);
    if (_socket) {
        _ioContext.requestIOSocketStop(_socket);
    }
}

}  // namespace scaler::top
