#pragma once

#include <atomic>
#include <functional>
#include <mutex>

#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>

#include "scaler/top/state/scheduler_state.h"

namespace scaler::top {

class TopScreen {
public:
    TopScreen();

    // Update state (thread-safe) - returns true if screen needs refresh
    bool updateState(SchedulerState newState);

    // Handle keyboard input - returns true if handled
    bool handleKey(char key);

    // Get the FTXUI component for rendering
    ftxui::Component getComponent();

    // Set the screen for triggering refreshes
    void setScreen(ftxui::ScreenInteractive* screen);

    // Check if running
    bool isRunning() const { return _running.load(); }

    // Request quit
    void requestQuit();

private:
    // Build UI elements
    ftxui::Element renderSchedulerInfo();
    ftxui::Element renderTaskManager();
    ftxui::Element renderBinderStats();
    ftxui::Element renderClientManager();
    ftxui::Element renderWorkerTable();
    ftxui::Element renderShortcuts();
    ftxui::Element render();

    // Get sorted workers based on current sort state
    std::vector<WorkerStatus> getSortedWorkers();

    // Format sort column header (with brackets if selected)
    std::string formatHeader(const std::string& name, SortBy sortBy);

    std::mutex _stateMutex;
    SchedulerState _state;
    SortState _sortState;
    std::atomic<bool> _running{true};

    // FTXUI screen reference for posting events (atomic for thread-safety)
    std::atomic<ftxui::ScreenInteractive*> _screen{nullptr};
};

}  // namespace scaler::top
