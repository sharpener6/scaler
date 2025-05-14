@0xaf44f44ea94a4675;

using CommonType = import "common.capnp";
using Status = import "status.capnp";

struct Task {
    taskId @0 :Data;
    source @1 :Data;
    metadata @2 :Data;
    funcObjectId @3 :Data;
    functionArgs @4 :List(Argument);

    struct Argument {
        type @0 :ArgumentType;
        data @1 :Data;

        enum ArgumentType {
            task @0;
            objectID @1;
        }
    }
}

struct TaskCancel {
    struct TaskCancelFlags {
        force @0 :Bool;
    }

    taskId @0 :Data;
    flags @1 :TaskCancelFlags;
}

struct TaskResult {
    taskId @0 :Data;
    resultType @1 :CommonType.TaskResultType;
    metadata @2 :Data;
    results @3 :List(Data);
}

struct TaskCancelConfirm {
    taskId @0 :Data;
    cancelConfirmType @1 :CommonType.TaskCancelConfirmType;

    union {
        noTask @2 :Void;
        task @3 :Task;
    }
}

struct GraphTask {
    taskId @0 :Data;
    source @1 :Data;
    targets @2 :List(Data);
    graph @3 :List(Task);
}

struct GraphTaskCancel {
    taskId @0 :Data;
}

struct ClientHeartbeat {
    resource @0 :Status.Resource;
    latencyUS @1 :UInt32;
}

struct ClientHeartbeatEcho {
}

struct WorkerHeartbeat {
    agent @0 :Status.Resource;
    rssFree @1 :UInt64;
    queueSize @2 :UInt32;
    queuedTasks @3 :UInt32;
    latencyUS @4 :UInt32;
    taskLock @5 :Bool;
    processors @6 :List(Status.ProcessorStatus);
}

struct WorkerHeartbeatEcho {
}

struct ObjectInstruction {
    instructionType @0 :ObjectInstructionType;
    objectUser @1 :Data;
    objectContent @2 :CommonType.ObjectContent;

    enum ObjectInstructionType {
        create @0;
        delete @1;
        clear @2;
    }
}

struct ObjectRequest {
    requestType @0 :ObjectRequestType;
    objectIds @1 :List(Data);

    enum ObjectRequestType {
        get @0;
    }
}

struct ObjectResponse {
    responseType @0 :ObjectResponseType;
    objectContent @1 :CommonType.ObjectContent;

    enum ObjectResponseType {
        content @0;
        objectNotExist @1;
    }
}

struct DisconnectRequest {
    worker @0 :Data;
}

struct DisconnectResponse {
    worker @0 :Data;
}

struct ClientDisconnect {
    disconnectType @0 :DisconnectType;

    enum DisconnectType {
        disconnect @0;
        shutdown @1;
    }
}

struct ClientShutdownResponse {
    accepted @0 :Bool;
}

struct StateClient {
}

struct StateObject {
}

struct StateBalanceAdvice {
    workerId @0 :Data;
    taskIds @1 :List(Data);
}

struct StateScheduler {
    binder @0 :Status.BinderStatus;
    scheduler @1 :Status.Resource;
    rssFree @2 :UInt64;
    clientManager @3 :Status.ClientManagerStatus;
    objectManager @4 :Status.ObjectManagerStatus;
    taskManager @5 :Status.TaskManagerStatus;
    workerManager @6 :Status.WorkerManagerStatus;
}

struct StateWorker {
    workerId @0 :Data;
    message @1 :Data;
}

struct StateTask {
    taskId @0 :Data;
    functionName @1 :Data;
    state @2 :CommonType.TaskState;
    worker @3 :Data;
    metadata @4 :Data;
}

struct StateGraphTask {
    enum NodeTaskType {
        normal @0;
        target @1;
    }

    graphTaskId @0 :Data;
    taskId @1 :Data;
    nodeTaskType @2 :NodeTaskType;
    parentTaskIds @3 :List(Data);
}

struct ProcessorInitialized {
}

struct InformationRequest {
    request @0 :Data;
}

struct InformationResponse {
    response @0 :Data;
}


struct Message {
    union {
        task @0 :Task;
        taskCancel @1 :TaskCancel;
        taskCancelConfirm @2 :TaskCancelConfirm;
        taskResult @3 :TaskResult;

        graphTask @4 :GraphTask;
        graphTaskCancel @5 :GraphTaskCancel;

        objectInstruction @6 :ObjectInstruction;
        objectRequest @7 :ObjectRequest;
        objectResponse @8 :ObjectResponse;

        clientHeartbeat @9 :ClientHeartbeat;
        clientHeartbeatEcho @10 :ClientHeartbeatEcho;

        workerHeartbeat @11 :WorkerHeartbeat;
        workerHeartbeatEcho @12 :WorkerHeartbeatEcho;

        disconnectRequest @13 :DisconnectRequest;
        disconnectResponse @14 :DisconnectResponse;

        stateClient @15 :StateClient;
        stateObject @16 :StateObject;
        stateBalanceAdvice @17 :StateBalanceAdvice;
        stateScheduler @18 :StateScheduler;
        stateWorker @19 :StateWorker;
        stateTask @20 :StateTask;
        stateGraphTask @21 :StateGraphTask;

        clientDisconnect @22 :ClientDisconnect;
        clientShutdownResponse @23 :ClientShutdownResponse;

        processorInitialized @24 :ProcessorInitialized;

        informationRequest @25 :InformationRequest;
        informationResponse @26 :InformationResponse;
    }
}
