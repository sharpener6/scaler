#include "scheduler_state.h"

namespace scaler::top {

std::string taskStateToString(TaskState state) {
    switch (state) {
        case TaskState::Inactive:
            return "inactive";
        case TaskState::Running:
            return "running";
        case TaskState::Canceling:
            return "canceling";
        case TaskState::BalanceCanceling:
            return "balanceCanceling";
        case TaskState::Success:
            return "success";
        case TaskState::Failed:
            return "failed";
        case TaskState::FailedWorkerDied:
            return "failedWorkerDied";
        case TaskState::Canceled:
            return "canceled";
        case TaskState::CanceledNotFound:
            return "canceledNotFound";
        case TaskState::BalanceCanceled:
            return "balanceCanceled";
        case TaskState::WorkerDisconnecting:
            return "workerDisconnecting";
        default:
            return "unknown";
    }
}

bool SchedulerState::updateFrom(const SchedulerState& newState) {
    if (*this == newState) {
        return false;
    }
    *this = newState;
    return true;
}

bool SortState::handleKey(char key) {
    SortBy newSort = sortBy;

    switch (key) {
        case 'g':
            newSort = SortBy::Group;
            break;
        case 'n':
            newSort = SortBy::Worker;
            break;
        case 'C':
            newSort = SortBy::AgentCpu;
            break;
        case 'M':
            newSort = SortBy::AgentRss;
            break;
        case 'c':
            newSort = SortBy::Cpu;
            break;
        case 'm':
            newSort = SortBy::Rss;
            break;
        case 'F':
            newSort = SortBy::RssFree;
            break;
        case 'f':
            newSort = SortBy::Free;
            break;
        case 'w':
            newSort = SortBy::Sent;
            break;
        case 'd':
            newSort = SortBy::Queued;
            break;
        case 's':
            newSort = SortBy::Suspended;
            break;
        case 'l':
            newSort = SortBy::Lag;
            break;
        default:
            return false;
    }

    sortByPrevious = sortBy;
    sortBy = newSort;

    if (sortBy != sortByPrevious) {
        descending = true;
    } else {
        descending = !descending;
    }

    return true;
}

}  // namespace scaler::top
