@0xf57f79ac88fab620;

enum TaskResultType {
    success @0;           # if submit and task is done and get result
    failed @1;            # if submit and task is failed on worker
    failedWorkerDied @2;  # if submit and worker died (only happened when scheduler keep_task=False)
}

enum TaskCancelConfirmType {
    canceled @0;               # if cancel success
    cancelFailed @1;           # if cancel failed, this might happened if the task is in process
    cancelNotFound @2;         # if cancel cannot find such task
}

enum TaskTransition {
    hasCapacity @0;
    taskResultSuccess @1;
    taskResultFailed @2;
    taskResultWorkerDied @3;
    taskCancel @4;
    taskCancelConfirmCanceled @5;
    taskCancelConfirmFailed @6;
    taskCancelConfirmNotFound @7;
    balanceTaskCancel @8;
    workerDisconnect @9;
    schedulerHasTask @10;
    schedulerHasNoTask @11;
}

enum TaskState {
    inactive @0;
    running @1;
    canceling @2;
    balanceCanceling @3;
    success @4;
    failed @5;
    failedWorkerDied @6;
    canceled @7;
    canceledNotFound @8;
    balanceCanceled @9;
    workerDisconnecting @10;
}

struct ObjectMetadata {
    objectIds @0 :List(Data);
    objectTypes @1 :List(ObjectContentType);
    objectNames @2 :List(Data);

    enum ObjectContentType {
        serializer @0;
        object @1;
    }
}

struct ObjectStorageAddress {
    host @0 :Text;
    port @1 :UInt16;
}
