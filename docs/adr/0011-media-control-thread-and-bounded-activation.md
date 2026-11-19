# ADR 0011 — A control thread per camera for GStreamer, and a bounded activation

- **Status:** Accepted
- **Date:** 2026-11-19
- **Deciders:** Project owner, fix plan `FIX-PLAN.md` (branch `fix/activation-deadlock`)
- **Supersedes:** —
- **Superseded by:** —

## Context

A camera could stay in `activating` for ever, and with it the whole
streamer: the registry kept every camera's state behind one tokio
`RwLock`, and the write guard was held across synchronous GStreamer
calls. The key-frame request sent a serialized downstream event, which
waits on the encoder's state and stream locks, so a busy streaming
thread kept the guard (and the async worker) blocked. The orchestrator
awaited `attach_live` inline without a deadline, so its mailbox stopped
draining too.

The cause is a hypothesis (H1) reasoned from the code and from the
stress test in `tests/registry_stress.rs`; the CI result for that test on
the untouched code is in the evidence file kept with the plan.

## Decision

1. No tokio lock, and no registry-wide lock, is held across a
   synchronous GStreamer call or a wait on a std mutex.
2. Per camera, a `CameraSlot` held as an `Arc` in a std `RwLock` map.
   Lock order is `cameras` → `control` → `wiring` → `live_switch`.
3. Every blocking GStreamer call of a camera runs on that camera's own
   control thread (`GstWorker`): a bounded queue (16), a 5 s timeout per
   call, a refusal when full.
4. The key-frame request is a non-serialized upstream
   `GstForceKeyUnit` event on the encoder's src pad; the switch probe
   uses `try_lock`; the `media-configure` and `unprepared` callbacks
   snapshot and release.
5. The live leg shutdown is bounded at 10 s.
6. Activation runs as a spawned task tracked by the orchestrator, with a
   50 s deadline. At the deadline, on admin idle, on offline or on
   shutdown it is aborted (2 s grace) and the existing teardown returns the
   camera to Idle. The camera task keeps draining its mailbox; pulses
   during an activation are absorbed and extend the session.
   `StateTransition::ActivationAborted` covers the admin case.
7. The runtime has at least four workers, with named threads.

## Consequences

Positive: a wedged GStreamer element costs one camera one call, not the
process; every activation ends; the mailbox never blocks.

Negative / risks: a call that times out leaves its work running on the
control thread until it returns, and the next call queues behind it; a
thread per camera; more lock levels to respect.

## Alternatives considered

- A watchdog that restarts the process: hides the fault, loses every
  camera. Rejected.
- `spawn_blocking` per call: no ordering per camera, and it exhausts the
  blocking pool under a wedge.
- Keeping the registry lock and shortening the critical sections: the
  next blocking call put in one brings the fault back.
