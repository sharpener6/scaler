#include "top_screen.h"

#include <algorithm>

#include "formatters.h"

namespace scaler::top {

static const int LAG_THRESHOLD_SECONDS = 5;

TopScreen::TopScreen() = default;

bool TopScreen::updateState(SchedulerState newState) {
    std::lock_guard<std::mutex> lock(_stateMutex);
    if (_state == newState) {
        return false;
    }
    _state = std::move(newState);

    // Trigger screen refresh
    auto* screen = _screen.load();
    if (screen) {
        screen->PostEvent(ftxui::Event::Custom);
    }
    return true;
}

bool TopScreen::handleKey(char key) {
    if (key == 'q' || key == 'Q') {
        requestQuit();
        return true;
    }

    std::lock_guard<std::mutex> lock(_stateMutex);
    bool handled = _sortState.handleKey(key);

    if (handled) {
        auto* screen = _screen.load();
        if (screen) {
            screen->PostEvent(ftxui::Event::Custom);
        }
    }

    return handled;
}

void TopScreen::setScreen(ftxui::ScreenInteractive* screen) { _screen.store(screen); }

void TopScreen::requestQuit() {
    _running.store(false);
    auto* screen = _screen.load();
    if (screen) {
        screen->Exit();
    }
}

ftxui::Component TopScreen::getComponent() {
    return ftxui::Renderer([this] { return render(); });
}

std::string TopScreen::formatHeader(const std::string& name, SortBy sortBy) {
    if (_sortState.sortBy == sortBy) {
        return "[" + name + "]";
    }
    return name;
}

std::vector<WorkerStatus> TopScreen::getSortedWorkers() {
    std::vector<WorkerStatus> workers = _state.workers;

    auto comparator = [this](const WorkerStatus& a, const WorkerStatus& b) {
        bool result = false;
        switch (_sortState.sortBy) {
            case SortBy::Group:
                result = a.groupId < b.groupId;
                break;
            case SortBy::Worker:
                result = a.workerId < b.workerId;
                break;
            case SortBy::AgentCpu:
                result = a.agent.cpu < b.agent.cpu;
                break;
            case SortBy::AgentRss:
                result = a.agent.rss < b.agent.rss;
                break;
            case SortBy::Cpu:
                result = a.totalCpu < b.totalCpu;
                break;
            case SortBy::Rss:
                result = a.totalRss < b.totalRss;
                break;
            case SortBy::RssFree:
                result = a.rssFree < b.rssFree;
                break;
            case SortBy::Free:
                result = a.free < b.free;
                break;
            case SortBy::Sent:
                result = a.sent < b.sent;
                break;
            case SortBy::Queued:
                result = a.queued < b.queued;
                break;
            case SortBy::Suspended:
                result = a.suspended < b.suspended;
                break;
            case SortBy::Lag:
                result = a.lagUs < b.lagUs;
                break;
        }
        return _sortState.descending ? !result : result;
    };

    std::sort(workers.begin(), workers.end(), comparator);
    return workers;
}

ftxui::Element TopScreen::renderSchedulerInfo() {
    std::lock_guard<std::mutex> lock(_stateMutex);

    return ftxui::vbox({
        ftxui::text("scheduler") | ftxui::bold,
        ftxui::hbox({ftxui::text("cpu: "), ftxui::text(formatPercentage(_state.scheduler.cpu))}),
        ftxui::hbox({ftxui::text("rss: "), ftxui::text(formatBytes(_state.scheduler.rss))}),
        ftxui::hbox({ftxui::text("rss_free: "), ftxui::text(formatBytes(_state.rssFree))}),
    });
}

ftxui::Element TopScreen::renderTaskManager() {
    std::lock_guard<std::mutex> lock(_stateMutex);

    ftxui::Elements rows;
    rows.push_back(ftxui::text("task_manager") | ftxui::bold);

    // Sort task states for consistent display
    std::vector<std::pair<TaskState, uint32_t>> sorted(_state.taskStateToCount.begin(), _state.taskStateToCount.end());
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        return taskStateToString(a.first) < taskStateToString(b.first);
    });

    for (const auto& [state, count] : sorted) {
        rows.push_back(ftxui::hbox({ftxui::text(taskStateToString(state) + ": "), ftxui::text(formatInteger(count))}));
    }

    return ftxui::vbox(std::move(rows));
}

ftxui::Element TopScreen::renderBinderStats() {
    std::lock_guard<std::mutex> lock(_stateMutex);

    ftxui::Elements sentRows;
    sentRows.push_back(ftxui::text("scheduler_sent") | ftxui::bold);
    for (const auto& [client, count] : _state.binderSent) {
        sentRows.push_back(ftxui::hbox({ftxui::text(truncate(client, 15) + ": "), ftxui::text(formatInteger(count))}));
    }

    ftxui::Elements recvRows;
    recvRows.push_back(ftxui::text("scheduler_received") | ftxui::bold);
    for (const auto& [client, count] : _state.binderReceived) {
        recvRows.push_back(ftxui::hbox({ftxui::text(truncate(client, 15) + ": "), ftxui::text(formatInteger(count))}));
    }

    return ftxui::hbox({
        ftxui::vbox(std::move(sentRows)),
        ftxui::separator(),
        ftxui::vbox(std::move(recvRows)),
    });
}

ftxui::Element TopScreen::renderClientManager() {
    std::lock_guard<std::mutex> lock(_stateMutex);

    ftxui::Elements rows;
    rows.push_back(ftxui::text("client_manager") | ftxui::bold);

    for (const auto& [client, count] : _state.clientToNumTasks) {
        rows.push_back(ftxui::hbox({ftxui::text(truncate(client, 18) + ": "), ftxui::text(std::to_string(count))}));
    }

    return ftxui::vbox(std::move(rows));
}

ftxui::Element TopScreen::renderWorkerTable() {
    std::lock_guard<std::mutex> lock(_stateMutex);

    if (_state.workers.empty()) {
        return ftxui::text("No workers");
    }

    auto workers = getSortedWorkers();

    // Header row
    ftxui::Elements headerCells = {
        ftxui::text(formatHeader("group", SortBy::Group)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
        ftxui::text(formatHeader("worker", SortBy::Worker)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 20),
        ftxui::text(formatHeader("agt_cpu", SortBy::AgentCpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
        ftxui::text(formatHeader("agt_rss", SortBy::AgentRss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
        ftxui::text(formatHeader("cpu", SortBy::Cpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
        ftxui::text(formatHeader("rss", SortBy::Rss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
        ftxui::text(formatHeader("os_rss_free", SortBy::RssFree)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 11),
        ftxui::text(formatHeader("free", SortBy::Free)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
        ftxui::text(formatHeader("sent", SortBy::Sent)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
        ftxui::text(formatHeader("queued", SortBy::Queued)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 7),
        ftxui::text(formatHeader("suspended", SortBy::Suspended)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
        ftxui::text(formatHeader("lag", SortBy::Lag)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 12),
        ftxui::text("ITL") | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
    };

    ftxui::Elements rows;
    rows.push_back(ftxui::hbox(std::move(headerCells)) | ftxui::bold);

    for (const auto& w : workers) {
        // Format lag with optional last seconds prefix
        std::string lagStr;
        if (w.lastS > LAG_THRESHOLD_SECONDS) {
            lagStr = "(" + formatSeconds(w.lastS) + ") ";
        }
        lagStr += formatMicroseconds(w.lagUs);

        ftxui::Elements cells = {
            ftxui::text(truncate(w.groupId, 10)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
            ftxui::text(truncate(w.workerId, 20)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 20),
            ftxui::text(formatPercentage(w.agent.cpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatBytes(w.agent.rss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatPercentage(w.totalCpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatBytes(w.totalRss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatBytes(w.rssFree)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 11),
            ftxui::text(std::to_string(w.free)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
            ftxui::text(std::to_string(w.sent)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
            ftxui::text(std::to_string(w.queued)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 7),
            ftxui::text(std::to_string(w.suspended)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
            ftxui::text(lagStr) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 12),
            ftxui::text(truncate(w.itl, 10)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
        };
        rows.push_back(ftxui::hbox(std::move(cells)));
    }

    return ftxui::vbox(std::move(rows));
}

ftxui::Element TopScreen::renderShortcuts() {
    return ftxui::text("Shortcuts: group[g] worker[n] agt_cpu[C] agt_rss[M] cpu[c] rss[m] rss_free[F] free[f] sent[w] "
                       "queued[d] suspended[s] lag[l] quit[q]") |
           ftxui::dim;
}

ftxui::Element TopScreen::render() {
    std::lock_guard<std::mutex> lock(_stateMutex);

    // Top section: scheduler info, object manager, task manager, binder stats, client manager
    auto topSection = ftxui::hbox({
        ftxui::vbox({
            ftxui::text("scheduler") | ftxui::bold,
            ftxui::hbox({ftxui::text("cpu: "), ftxui::text(formatPercentage(_state.scheduler.cpu))}),
            ftxui::hbox({ftxui::text("rss: "), ftxui::text(formatBytes(_state.scheduler.rss))}),
            ftxui::hbox({ftxui::text("rss_free: "), ftxui::text(formatBytes(_state.rssFree))}),
        }),
        ftxui::separator(),
        ftxui::vbox({
            ftxui::text("object_manager") | ftxui::bold,
            ftxui::hbox({ftxui::text("num_of_objs: "), ftxui::text(std::to_string(_state.numObjects))}),
        }),
        ftxui::separator(),
        [this]() {
            ftxui::Elements rows;
            rows.push_back(ftxui::text("task_manager") | ftxui::bold);
            std::vector<std::pair<TaskState, uint32_t>> sorted(_state.taskStateToCount.begin(),
                                                               _state.taskStateToCount.end());
            std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return taskStateToString(a.first) < taskStateToString(b.first);
            });
            for (const auto& [state, count] : sorted) {
                rows.push_back(
                    ftxui::hbox({ftxui::text(taskStateToString(state) + ": "), ftxui::text(formatInteger(count))}));
            }
            return ftxui::vbox(std::move(rows));
        }(),
        ftxui::separator(),
        [this]() {
            ftxui::Elements rows;
            rows.push_back(ftxui::text("scheduler_sent") | ftxui::bold);
            for (const auto& [client, count] : _state.binderSent) {
                rows.push_back(
                    ftxui::hbox({ftxui::text(truncate(client, 15) + ": "), ftxui::text(formatInteger(count))}));
            }
            return ftxui::vbox(std::move(rows));
        }(),
        ftxui::separator(),
        [this]() {
            ftxui::Elements rows;
            rows.push_back(ftxui::text("scheduler_received") | ftxui::bold);
            for (const auto& [client, count] : _state.binderReceived) {
                rows.push_back(
                    ftxui::hbox({ftxui::text(truncate(client, 15) + ": "), ftxui::text(formatInteger(count))}));
            }
            return ftxui::vbox(std::move(rows));
        }(),
        ftxui::separator(),
        [this]() {
            ftxui::Elements rows;
            rows.push_back(ftxui::text("client_manager") | ftxui::bold);
            for (const auto& [client, count] : _state.clientToNumTasks) {
                rows.push_back(
                    ftxui::hbox({ftxui::text(truncate(client, 18) + ": "), ftxui::text(std::to_string(count))}));
            }
            return ftxui::vbox(std::move(rows));
        }(),
    });

    // Shortcuts bar
    auto shortcuts =
        ftxui::text("Shortcuts: group[g] worker[n] agt_cpu[C] agt_rss[M] cpu[c] rss[m] rss_free[F] free[f] sent[w] "
                    "queued[d] suspended[s] lag[l] quit[q]") |
        ftxui::dim;

    // Worker count summary
    auto workerSummary =
        ftxui::text("Total " + std::to_string(_state.workerGroups.size()) + " worker group(s) with " +
                    std::to_string(_state.workers.size()) + " worker(s)");

    // Worker table
    ftxui::Elements workerRows;
    if (_state.workers.empty()) {
        workerRows.push_back(ftxui::text("No workers"));
    } else {
        auto workers = getSortedWorkers();

        // Header
        ftxui::Elements headerCells = {
            ftxui::text(formatHeader("group", SortBy::Group)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
            ftxui::text(" "),
            ftxui::text(formatHeader("worker", SortBy::Worker)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 20),
            ftxui::text(" "),
            ftxui::text(formatHeader("agt_cpu", SortBy::AgentCpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatHeader("agt_rss", SortBy::AgentRss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatHeader("cpu", SortBy::Cpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatHeader("rss", SortBy::Rss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
            ftxui::text(formatHeader("os_rss_free", SortBy::RssFree)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 11),
            ftxui::text(formatHeader("free", SortBy::Free)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
            ftxui::text(formatHeader("sent", SortBy::Sent)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
            ftxui::text(formatHeader("queued", SortBy::Queued)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 7),
            ftxui::text(formatHeader("suspended", SortBy::Suspended)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
            ftxui::text(formatHeader("lag", SortBy::Lag)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 12),
            ftxui::text("ITL") | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
        };
        workerRows.push_back(ftxui::hbox(std::move(headerCells)) | ftxui::bold);

        for (const auto& w : workers) {
            std::string lagStr;
            if (w.lastS > LAG_THRESHOLD_SECONDS) {
                lagStr = "(" + formatSeconds(w.lastS) + ") ";
            }
            lagStr += formatMicroseconds(w.lagUs);

            ftxui::Elements cells = {
                ftxui::text(truncate(w.groupId, 10)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
                ftxui::text(" "),
                ftxui::text(truncate(w.workerId, 20)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 20),
                ftxui::text(" "),
                ftxui::text(formatPercentage(w.agent.cpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
                ftxui::text(formatBytes(w.agent.rss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
                ftxui::text(formatPercentage(w.totalCpu)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
                ftxui::text(formatBytes(w.totalRss)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 8),
                ftxui::text(formatBytes(w.rssFree)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 11),
                ftxui::text(std::to_string(w.free)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
                ftxui::text(std::to_string(w.sent)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 6),
                ftxui::text(std::to_string(w.queued)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 7),
                ftxui::text(std::to_string(w.suspended)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
                ftxui::text(lagStr) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 12),
                ftxui::text(truncate(w.itl, 10)) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 10),
            };
            workerRows.push_back(ftxui::hbox(std::move(cells)));
        }
    }

    return ftxui::vbox({
               topSection,
               ftxui::separatorHeavy(),
               shortcuts,
               workerSummary,
               ftxui::vbox(std::move(workerRows)) | ftxui::flex,
           }) |
           ftxui::border;
}

}  // namespace scaler::top
