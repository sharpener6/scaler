#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace scaler::top {

struct Resource {
    uint16_t cpu = 0;  // percentage * 10 (e.g., 99.2% = 992)
    uint64_t rss = 0;  // bytes

    bool operator==(const Resource&) const = default;
};

struct WorkerStatus {
    std::string workerId;
    std::string groupId;
    Resource agent;
    uint64_t rssFree = 0;
    uint32_t free = 0;
    uint32_t sent = 0;
    uint32_t queued = 0;
    uint8_t suspended = 0;
    uint64_t lagUs = 0;
    uint8_t lastS = 0;
    std::string itl;
    uint16_t totalCpu = 0;   // sum of processor CPUs
    uint64_t totalRss = 0;   // sum of processor RSS

    bool operator==(const WorkerStatus&) const = default;
};

// Task states matching common.capnp TaskState enum
enum class TaskState : uint8_t {
    Inactive = 0,
    Running = 1,
    Canceling = 2,
    BalanceCanceling = 3,
    Success = 4,
    Failed = 5,
    FailedWorkerDied = 6,
    Canceled = 7,
    CanceledNotFound = 8,
    BalanceCanceled = 9,
    WorkerDisconnecting = 10
};

std::string taskStateToString(TaskState state);

struct SchedulerState {
    // Scheduler info
    Resource scheduler;
    uint64_t rssFree = 0;

    // Binder stats
    std::map<std::string, uint32_t> binderReceived;
    std::map<std::string, uint32_t> binderSent;

    // Managers
    std::map<std::string, uint32_t> clientToNumTasks;
    uint32_t numObjects = 0;
    std::map<TaskState, uint32_t> taskStateToCount;

    // Workers
    std::vector<WorkerStatus> workers;
    std::map<std::string, std::vector<std::string>> workerGroups;

    bool operator==(const SchedulerState&) const = default;

    // Returns true if state changed
    bool updateFrom(const SchedulerState& newState);
};

// Sort options matching Python implementation
enum class SortBy : uint8_t {
    Group,
    Worker,
    AgentCpu,
    AgentRss,
    Cpu,
    Rss,
    RssFree,
    Free,
    Sent,
    Queued,
    Suspended,
    Lag
};

struct SortState {
    SortBy sortBy = SortBy::Cpu;
    SortBy sortByPrevious = SortBy::Cpu;
    bool descending = true;

    // Update sort state based on key press, returns true if changed
    bool handleKey(char key);
};

}  // namespace scaler::top
