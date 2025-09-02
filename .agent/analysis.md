# Analysis of Cancellation Changes

## Current Problem
Based on the user's description, the task cancellation mechanism has changed:

1. **Old behavior**: When `future.cancel()` was called, it would immediately mark the future as cancelled
2. **New behavior**: When `future.cancel()` is called, it sends a cancel request to scheduler and needs to wait for either:
   - A `TaskCancelConfirm` message confirming the cancellation
   - A `TaskResult` message if the task completed before cancellation (in this case, future should also be marked as cancelled)

## Current Issues in client/future.py

Looking at the current implementation:

### Lines 67-69: Set result logic
```python
def set_result_ready(self, object_id: ObjectID, task_state: TaskState, profile_result: Optional[ProfileResult] = None) -> None:
    if self._cancel_ready_event is not None:
        self.set_canceled()
        return
```
This logic correctly handles the case where a result arrives after cancel() was called.

### Lines 171-188: Cancel logic  
```python
def cancel(self) -> bool:
    with self._condition:
        if self.cancelled():
            return True
        if self.done():
            return False
        self._cancel_ready_event = threading.Event()
        cancel_flags = TaskCancel.TaskCancelFlags(force=True)
        if self._group_task_id is not None:
            self._connector_agent.send(TaskCancel.new_msg(self._group_task_id, flags=cancel_flags))
        else:
            self._connector_agent.send(TaskCancel.new_msg(self._task_id, flags=cancel_flags))
    return True
```

The current cancel() method returns True immediately after sending the cancel request, but doesn't wait for confirmation.

### Lines 154-159: Cancelled check
```python
def cancelled(self) -> bool:
    if self._cancel_ready_event is None:
        return super().cancelled()
    self._cancel_ready_event.wait()
    return super().cancelled()
```

This waits for the cancel event, but there's no mechanism to set this event when TaskCancelConfirm is received.

## Missing Components

1. **No handler for TaskCancelConfirm**: The future needs a way to be notified when TaskCancelConfirm is received
2. **No timeout handling**: If TaskCancelConfirm never arrives, the future will hang indefinitely
3. **set_canceled() is called in wrong place**: Line 96 in set_canceled() sets the cancel_ready_event, but this should happen when we receive TaskCancelConfirm, not when we decide to cancel

## Test Analysis

Looking at test_client.py:
- `test_noop_cancel()` (lines 93-105): Tests calling cancel() on futures but doesn't assert the behavior
- `test_clear()` (lines 301-305): Expects futures to be cancelled after client.clear()

Looking at test_graph.py:
- `test_cancel()` (lines 127-139): Tests cancelling a graph task and expects result
- `test_client_quit()` (lines 141-152): Expects futures to be cancelled when client disconnects

## Plan to Fix

1. Add a method to handle TaskCancelConfirm reception
2. Modify cancel() logic to properly wait for confirmation or result
3. Ensure set_result_ready() properly handles the cancel case
4. Make sure cancelled() returns the correct state
5. Handle timeouts appropriately