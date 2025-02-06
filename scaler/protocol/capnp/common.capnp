@0xf57f79ac88fab620;

enum TaskResultStatus {
    success @0;     # if submit and task is done and get result
    failed @1;      # if submit and task is failed on worker
    workerDied @2;  # if submit and worker died (only happened when scheduler keep_task=False)
    noWorker @3;    # if submit and scheduler is full (not implemented yet)
}

enum TaskCancelConfirmStatus {
    canceled @0;        # if cancel success
    cancelFailed @1;    # if cancel failed, this might happened if the task is in process
    cancelNotFound @2;  # if cancel cannot find such task
}

# below are only used for monitoring channel, not sent to client
enum TaskStatus {
    inactive @0;
    running @1;
    canceling @2;
    finished @3;
    canceled @4;
}

struct ObjectContent {
    objectIds @0 :List(Data);
    objectTypes @1 :List(ObjectContentType);
    objectNames @2 :List(Data);
    objectBytes @3 :List(List(Data));

    enum ObjectContentType {
        serializer @0;
        object @1;
    }
}
