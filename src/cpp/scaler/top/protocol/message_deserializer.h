#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "scaler/top/state/scheduler_state.h"

namespace scaler::top {

// Parse a Message from payload and extract StateScheduler if present
std::optional<SchedulerState> parseStateScheduler(const std::vector<uint8_t>& payload);

}  // namespace scaler::top
