# ScalerFuture Task Cancel Fix Plan

## Problem Analysis
Based on the user's description, the issue is with the ScalerFuture cancellation semantics after recent changes to the task channel. The new protocol requires TaskCancelConfirm messages, changing the cancellation behavior from standard Python futures.

## Key Requirements
1. When `future.cancel()` is called, it should send cancel to scheduler
2. Future should wait for either:
   - TaskCancelConfirm message 
   - TaskResult message (if task completed before cancel)
3. If TaskResult arrives after cancel request, future should still be marked as cancelled
4. All unit test cases must pass

## Investigation Results
Looking at the current implementation in `scaler/client/future.py`, I can see:
- Lines 71-75: Race condition handling in `set_result_ready()`
- Lines 179-205: Cancel method with timeout waiting
- Lines 92-103: `set_cancel_confirmed()` method
- Future manager already handles TaskCancelConfirm properly

## Current Issues to Fix
Let me run specific tests to identify what's actually failing and needs to be fixed.