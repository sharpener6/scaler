# Fix Plan for ScalerFuture Cancellation

## Problem Analysis

The current issue is in the `cancel()` method logic:

1. **Current `cancel()` behavior (lines 171-188)**:
   - Sets `_cancel_ready_event = threading.Event()` 
   - Sends `TaskCancel` message to scheduler
   - Returns `True` immediately, but doesn't wait for confirmation

2. **Current `cancelled()` behavior (lines 154-159)**:
   - Waits on `_cancel_ready_event.wait()` if cancellation was requested
   - But `_cancel_ready_event` is never set, so it hangs forever

3. **Current `set_canceled()` behavior (lines 90-99)**:
   - Sets the cancel event, but this is called when TaskCancelConfirm arrives
   - Problem: Line 96 calls `self._cancel_ready_event.set()` but this event might be None

4. **Current `set_result_ready()` behavior (lines 67-69)**:
   - Correctly handles case where result arrives after cancel() was called
   - If `_cancel_ready_event` is not None, calls `set_canceled()` and returns

## Root Issue

The `cancel()` method should behave like the standard Python Future:
- Return `True` if cancellation was successful or already cancelled
- Return `False` if the task is already done and can't be cancelled
- But in Scaler, we need to wait for either `TaskCancelConfirm` or `TaskResult` after requesting cancellation

## Solution

1. **Modify `cancel()` method**:
   - Don't return immediately after sending cancel request
   - Wait for either `TaskCancelConfirm` or `TaskResult` 
   - Handle timeout case
   - Return appropriate boolean based on actual cancellation success

2. **Fix `set_canceled()` method**:
   - Properly set the `_cancel_ready_event` when confirmation arrives
   - Only set this when we actually get confirmation, not when we request it

3. **Add new method `set_cancel_confirmed()`**:
   - Called by `ClientFutureManager` when `TaskCancelConfirm` is received
   - This should set the `_cancel_ready_event` and mark as cancelled

4. **Modify `set_result_ready()` for cancel case**:
   - When result arrives after cancel request, treat it as successful cancellation
   - Set both the result state and the cancel confirmation

## Implementation Steps

1. Add `set_cancel_confirmed()` method
2. Modify `cancel()` to wait for confirmation with timeout
3. Fix `set_canceled()` to not set cancel_ready_event prematurely 
4. Update `set_result_ready()` to handle post-cancel results properly
5. Ensure `cancelled()` method works correctly with new logic