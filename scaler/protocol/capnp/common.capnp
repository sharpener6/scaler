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
    task @0;
    hasCapacity @1;
    taskResultSuccess @2;
    taskResultFailed @3;
    taskResultWorkerDied @4;
    taskCancel @5;
    taskCancelConfirmCanceled @6;
    taskCancelConfirmFailed @7;
    taskCancelConfirmNotFound @8;
    balanceTaskCancel @9;
    workerDisconnect @10;
    schedulerHasTask @11;
    schedulerHasNoTask @12;
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
