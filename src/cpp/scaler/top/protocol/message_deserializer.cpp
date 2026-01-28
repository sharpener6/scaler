#include "message_deserializer.h"

#include <capnp/serialize.h>

#include "message.capnp.h"
#include "status.capnp.h"

namespace scaler::top {

std::optional<SchedulerState> parseStateScheduler(const std::vector<uint8_t>& payload) {
    if (payload.empty()) {
        return std::nullopt;
    }

    try {
        auto words = kj::ArrayPtr<const capnp::word>(
            reinterpret_cast<const capnp::word*>(payload.data()), payload.size() / sizeof(capnp::word));
        capnp::FlatArrayMessageReader reader(words);
        auto msg = reader.getRoot<Message>();

        if (msg.which() != Message::STATE_SCHEDULER) {
            return std::nullopt;
        }

        auto ss = msg.getStateScheduler();
        SchedulerState state;

        // Parse scheduler resource
        auto sched = ss.getScheduler();
        state.scheduler.cpu = sched.getCpu();
        state.scheduler.rss = sched.getRss();
        state.rssFree = ss.getRssFree();

        // Parse binder status
        auto binder = ss.getBinder();
        for (auto pair : binder.getReceived()) {
            state.binderReceived[pair.getClient().cStr()] = pair.getNumber();
        }
        for (auto pair : binder.getSent()) {
            state.binderSent[pair.getClient().cStr()] = pair.getNumber();
        }

        // Parse client manager
        for (auto pair : ss.getClientManager().getClientToNumOfTask()) {
            auto clientData = pair.getClient();
            std::string clientId(reinterpret_cast<const char*>(clientData.begin()), clientData.size());
            state.clientToNumTasks[clientId] = pair.getNumTask();
        }

        // Parse object manager
        state.numObjects = ss.getObjectManager().getNumberOfObjects();

        // Parse task manager
        for (auto pair : ss.getTaskManager().getStateToCount()) {
            auto taskState = static_cast<TaskState>(pair.getState());
            state.taskStateToCount[taskState] = pair.getCount();
        }

        // Parse scaling manager (worker groups)
        for (auto pair : ss.getScalingManager().getWorkerGroups()) {
            auto groupIdData = pair.getWorkerGroupID();
            std::string groupId(reinterpret_cast<const char*>(groupIdData.begin()), groupIdData.size());

            std::vector<std::string> workerIds;
            for (auto workerIdData : pair.getWorkerIDs()) {
                workerIds.emplace_back(reinterpret_cast<const char*>(workerIdData.begin()), workerIdData.size());
            }
            state.workerGroups[groupId] = std::move(workerIds);
        }

        // Build worker group lookup map
        std::map<std::string, std::string> workerToGroup;
        for (const auto& [groupId, workerIds] : state.workerGroups) {
            for (const auto& workerId : workerIds) {
                workerToGroup[workerId] = groupId;
            }
        }

        // Parse workers
        for (auto ws : ss.getWorkerManager().getWorkers()) {
            WorkerStatus worker;

            auto workerIdData = ws.getWorkerId();
            worker.workerId = std::string(reinterpret_cast<const char*>(workerIdData.begin()), workerIdData.size());

            // Look up group from worker groups
            auto groupIt = workerToGroup.find(worker.workerId);
            if (groupIt != workerToGroup.end()) {
                worker.groupId = groupIt->second;
            }

            auto agent = ws.getAgent();
            worker.agent.cpu = agent.getCpu();
            worker.agent.rss = agent.getRss();

            worker.rssFree = ws.getRssFree();
            worker.free = ws.getFree();
            worker.sent = ws.getSent();
            worker.queued = ws.getQueued();
            worker.suspended = ws.getSuspended();
            worker.lagUs = ws.getLagUS();
            worker.lastS = ws.getLastS();
            worker.itl = ws.getItl().cStr();

            // Sum up processor stats
            uint32_t totalCpu = 0;
            uint64_t totalRss = 0;
            for (auto ps : ws.getProcessorStatuses()) {
                auto res = ps.getResource();
                totalCpu += res.getCpu();
                totalRss += res.getRss();
            }
            worker.totalCpu = totalCpu;
            worker.totalRss = totalRss;

            state.workers.push_back(std::move(worker));
        }

        return state;
    } catch (...) {
        return std::nullopt;
    }
}

}  // namespace scaler::top
