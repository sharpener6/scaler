#pragma once

#include <cstdint>
#include <string>

namespace scaler::top {

// Format bytes with appropriate unit suffix (B, K, M, G, T)
std::string formatBytes(uint64_t bytes);

// Format percentage from internal representation (e.g., 992 -> "99.2%")
std::string formatPercentage(uint16_t pct);

// Format integer with thousands separators (e.g., 1234567 -> "1,234,567")
std::string formatInteger(uint32_t num);

// Format microseconds to appropriate unit (us, ms, s)
std::string formatMicroseconds(uint64_t us);

// Format seconds (caps at "60+s")
std::string formatSeconds(uint8_t seconds);

// Truncate string with indicator (e.g., "longstring" -> "longst+" or "+string")
std::string truncate(const std::string& str, size_t maxLen, bool leftTruncate = false);

}  // namespace scaler::top
