#include "formatters.h"

#include <cmath>
#include <iomanip>
#include <sstream>

namespace scaler::top {

static const double STORAGE_SIZE_MODULUS = 1024.0;
static const int TIME_MODULUS = 1000;
static const int NUM_STORAGE_UNITS = 5;
static const int NUM_TIME_UNITS = 3;
static const int MAX_DISPLAY_SECONDS = 60;
static const int LAG_THRESHOLD_SECONDS = 5;

std::string formatBytes(uint64_t bytes) {
    double number = static_cast<double>(bytes);
    const char* units[] = {"B", "K", "M", "G", "T"};

    for (int i = 0; i < NUM_STORAGE_UNITS; ++i) {
        if (number < STORAGE_SIZE_MODULUS || i == NUM_STORAGE_UNITS - 1) {
            std::ostringstream oss;
            if (i <= 1) {
                // B or K: show as integer
                oss << static_cast<int>(number) << units[i];
            } else {
                // M, G, T: show with one decimal
                oss << std::fixed << std::setprecision(1) << number << units[i];
            }
            return oss.str();
        }
        number /= STORAGE_SIZE_MODULUS;
    }

    return "0B";
}

std::string formatPercentage(uint16_t pct) {
    double percentage = pct / 10.0;
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << percentage << "%";
    return oss.str();
}

std::string formatInteger(uint32_t num) {
    std::string numStr = std::to_string(num);
    std::string result;

    int count = 0;
    for (auto it = numStr.rbegin(); it != numStr.rend(); ++it) {
        if (count > 0 && count % 3 == 0) {
            result = ',' + result;
        }
        result = *it + result;
        ++count;
    }

    return result;
}

std::string formatMicroseconds(uint64_t us) {
    int64_t number = static_cast<int64_t>(us);
    const char* units[] = {"us", "ms", "s"};

    for (int i = 0; i < NUM_TIME_UNITS; ++i) {
        if (number < TIME_MODULUS || i == NUM_TIME_UNITS - 1) {
            std::ostringstream oss;
            if (i == 0) {
                // Still in microseconds, show as ms with decimal
                oss << std::fixed << std::setprecision(1) << (number / static_cast<double>(TIME_MODULUS)) << "ms";
            } else if (i == 2 && number > TIME_MODULUS) {
                // Seconds and too big
                oss << number << "+s";
            } else {
                oss << number << units[i];
            }
            return oss.str();
        }
        number /= TIME_MODULUS;
    }

    return "0ms";
}

std::string formatSeconds(uint8_t seconds) {
    if (seconds > MAX_DISPLAY_SECONDS) {
        return std::to_string(MAX_DISPLAY_SECONDS) + "+s";
    }
    return std::to_string(seconds) + "s";
}

std::string truncate(const std::string& str, size_t maxLen, bool leftTruncate) {
    if (maxLen == 0 || str.length() <= maxLen) {
        return str;
    }

    if (leftTruncate) {
        return "+" + str.substr(str.length() - maxLen);
    } else {
        return str.substr(0, maxLen) + "+";
    }
}

}  // namespace scaler::top
