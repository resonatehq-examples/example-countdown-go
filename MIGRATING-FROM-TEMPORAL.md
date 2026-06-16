# Coming from Temporal: Durable Sequential Loop with Timers

This guide maps the durable-loop-with-timer pattern in
[`temporalio/samples-go/sleep-for-days`](https://github.com/temporalio/samples-go/tree/main/sleep-for-days)
(and the related [`timer`](https://github.com/temporalio/samples-go/tree/main/timer)
and [`cron`](https://github.com/temporalio/samples-go/tree/main/cron) samples) to the
countdown example here. The goal is to help you port a durable sequential loop — tick,
sleep, repeat, survive crashes — from Temporal to Resonate.

## The pattern

Both systems let you write a loop that performs a step (emit a notification, send an
email) and then waits a fixed duration before the next iteration. The loop is
*durable*: if the worker process crashes mid-loop, restarting it resumes from the next
pending tick rather than starting over. In Temporal this is implemented with
`workflow.ExecuteActivity` + `workflow.NewTimer`/`NewSelector`; in Resonate it is
`ctx.RPC` + `ctx.Sleep`, both of which create server-side durable promises.

There is no exact "countdown" sample in `temporalio/samples-go`. The closest match is
`sleep-for-days` (a loop + completion signal), with `timer` showing the
`NewTimer`/`Selector` mechanism and `cron` showing a scheduled repeating activity. This
guide is honest about that gap: the mapping is structural, not one-to-one.

## Side by side

### Temporal (`samples-go/sleep-for-days`)

```go
func SleepForDaysWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	isComplete := false
	sigChan := workflow.GetSignalChannel(ctx, "complete")

	for !isComplete {
		workflow.ExecuteActivity(ctx, SendEmailActivity, "Sleeping for 30 days")
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(workflow.NewTimer(ctx, time.Hour*24*30), func(f workflow.Future) {})
		selector.AddReceive(sigChan, func(c workflow.ReceiveChannel, more bool) {
			isComplete = true
		})
		selector.Select(ctx)
	}

	return "done", nil
}
```

`sleep-for-days` runs indefinitely, racing a 30-day timer against a `"complete"`
signal each iteration. The `timer` sample (not shown in full) demonstrates the same
`workflow.NewTimer` + `NewSelector` pattern to race a long-running activity against a
deadline notification.

### Resonate (this example)

```go
func countdown(ctx *resonate.Context, args CountdownArgs) (CountdownResult, error) {
	sent := 0
	for i := args.Start; i > 0; i-- {
		f, err := ctx.RPC("notify", NotifyArgs{Count: i, URL: args.NotifyURL})
		if err != nil {
			return CountdownResult{}, err
		}
		var r NotifyResult
		if err := f.Await(&r); err != nil {
			return CountdownResult{}, fmt.Errorf("notify %d: %w", i, err)
		}
		sent++

		if i > 1 {
			s, err := ctx.Sleep(time.Duration(args.StepSeconds) * time.Second)
			if err != nil {
				return CountdownResult{}, err
			}
			if err := s.Await(nil); err != nil {
				return CountdownResult{}, fmt.Errorf("sleep before %d: %w", i-1, err)
			}
		}
	}
	return CountdownResult{Sent: sent}, nil
}
```

The `if i > 1` guard skips the trailing sleep after the final tick — the workflow does
not pause after the last notification.

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `workflow.ExecuteActivity(ctx, fn, args)` (fire-and-forget, as in `sleep-for-days`) or `.Get(ctx, &r)` (blocking, as in `cron`) | `ctx.RPC("name", args)` → `f.Await(&r)` | `sleep-for-days` drops the activity future and races the timer instead — it never calls `.Get()`; the blocking form comes from the `cron` sample. Resonate always returns a `*Future`; call `f.Await` to block. |
| `workflow.NewTimer(ctx, d)` + `selector.Select(ctx)` | `ctx.Sleep(d)` → `s.Await(nil)` | Both suspend the loop for duration `d`; both use `time.Duration` |
| `workflow.ActivityOptions{StartToCloseTimeout: ...}` | `resonate.RPCOpts{Timeout: ...}` (optional) | Resonate has no required timeout; omit for default |
| `workflow.NewSelector` + `AddFuture`/`AddReceive` | No direct equivalent | `sleep-for-days` races a timer vs. a signal; countdown is a fixed-length loop with no external signal — no selector needed |
| `workflow.GetSignalChannel(ctx, "complete")` | `ctx.Promise(...)` + external `Sender().PromiseSettle(...)` | Signal = push from outside; latent promise = pull (workflow awaits; caller resolves). See `example-human-in-the-loop-go`. |
| `w.RegisterWorkflow` / `w.RegisterActivity` | `resonate.Register(r, "name", fn)` | Single registration call; no workflow-vs-activity distinction |
| Task Queue | `Config.Network` / `Group` (via `httpnet.HTTPOptions`) | Routes work to the right worker process |
| Worker process + Starter process (separate binaries) | Single `main()` in this example | Resonate does not require a separate client binary; `cdFn.Run` and the worker loop share one process here |
| Workflow ID (stable, caller-supplied) | Promise ID (`id` arg to `cdFn.Run`) | Same concept: a stable business key that makes resubmission idempotent |

## Porting it, step by step

1. **Remove the worker/starter split.** In `sleep-for-days` there is a `worker/`
   package and a `starter/` package. In this example both roles live in `main()`.
   `resonate.Register(r, "name", fn)` registers the function and returns the typed
   handle `cdFn`; `cdFn.Run(ctx, id, args)` then invokes it. These are two separate
   calls. The worker receive loop starts inside `resonate.New` (via
   `installMessageHandler` + `network.Start`) — there is no explicit `worker.Run`
   call equivalent; by the time `New` returns the worker is already listening.

2. **Replace `workflow.WithActivityOptions` + `ExecuteActivity`.** Each
   `workflow.ExecuteActivity(ctx, fn, args)` becomes `ctx.RPC("name", args)`. You get
   back a `*Future`; call `f.Await(&result)` to block on it. Error handling is explicit
   rather than packed into the future.

3. **Replace `workflow.NewTimer` + `NewSelector`.** Replace the selector dance with a
   two-liner: `s, err := ctx.Sleep(d)` then `s.Await(nil)`. No selector, no closure,
   no `AddFuture`.

4. **Drop `ActivityOptions`.** Resonate has no mandatory timeout. If you need one, pass
   `resonate.RPCOpts{Timeout: d}` as a third arg to `ctx.RPC`. Omitting it is fine for
   local development.

5. **Keep the loop termination logic explicit.** `sleep-for-days` exits via a signal;
   countdown exits when `i` reaches zero. Port your exit condition directly — there is
   no signal-channel equivalent needed for a bounded loop.

6. **Handle both return values from `ctx.RPC` and `ctx.Sleep`.** Both return
   `(*Future, error)`; the error is non-nil if the SDK cannot record the promise. Check
   it before calling `Await`.

7. **Use a stable promise ID.** `main.go` builds `id` from `time.Now().UnixNano()`.
   For production use, supply a deterministic business key so that re-submitting after
   a crash reconnects to the running workflow rather than starting a new one.

## What's different (and why)

**Replay model.** Both Temporal and Resonate re-execute the function body from the top
on resume. In Temporal, the replay is driven by an event log on the server; calls whose
events are already in the log short-circuit and return the recorded result. One
consequence is that the event history grows with each loop iteration; for long-running
or unbounded loops Temporal recommends `ContinueAsNew` to reset the history and avoid
hitting size limits (the companion recursive-factorial guide covers this). In Resonate,
already-settled child promises short-circuit by promise ID (a durable-promise cache,
not an event log), so history size is not a concern for long loops. The observable
behavior on crash-resume is the same in both systems: the loop skips ticks that already
completed. Side effects written *outside* `ctx.RPC`/`ctx.Sleep` (e.g., a bare
`fmt.Println`) will re-run on every resume in both systems.

**No workflow/activity distinction.** Temporal distinguishes workflows (deterministic,
replayed via event log) from activities (free I/O, no replay). That boundary informs
`workflow.Context` vs `context.Context` in function signatures, separate registration
calls, and the `ActivityOptions` timeout requirement. In Resonate all functions share
`*resonate.Context`; durability comes from whether a call goes through
`ctx.RPC`/`ctx.Run` (recorded as a promise) or is called directly (not recorded).

**No Selector.** `sleep-for-days` uses `NewSelector` to race a timer against a signal.
Countdown has no external signal, so no selector is needed. If you need to race a sleep
against an external event, the Resonate equivalent is a latent promise (`ctx.Promise`)
resolved from outside, awaited in a goroutine or select — see
`example-human-in-the-loop-go` for the pattern.

**Server URL is hardcoded.** `main.go` passes `URL: "http://localhost:8001"` directly
in `resonate.New(Config{...})`. There is no automatic fallback to an in-process local
mode. Start `resonate dev` (or your own server) before running the binary.

**`resonate.Register` is multi-return.** Unlike Temporal's `w.RegisterWorkflow` (which
returns nothing), `resonate.Register(r, "name", fn)` returns
`(*RegisteredFunc, error)`. A bare call that discards both return values compiles but
is a logic error: you lose the handle needed to call `.Run`, and any registration
error is silently swallowed. Assigning only one of the two return values is the form
that produces a compile error in Go.

## Notes & coverage

- **No bounded countdown sample in `samples-go`.** `sleep-for-days` runs until a
  `"complete"` signal arrives; it is not a count-down. The structural mapping (loop +
  activity + timer per iteration) is valid, but the exit condition differs. `cron` is
  closer in spirit (periodic scheduled work) but uses a server-side schedule rather
  than an in-workflow loop. Both are cited as reference, not direct ports.

- **`ctx.Sleep` skips on last tick.** The `if i > 1` guard is intentional: the
  workflow exits immediately after the final notify without queuing a sleep promise.
  Temporal has no equivalent guard because `sleep-for-days` exits via signal rather
  than by counting.

- **Promise IDs for child calls are SDK-generated.** Unlike the top-level workflow ID
  (which you supply), child promise IDs for each `ctx.RPC` and `ctx.Sleep` call are
  generated worker-side by the SDK via an internal sequence counter. You do not manage
  them.

- **`resonate-sdk-go` is pre-release.** This example pins to a specific commit. API
  signatures may change before `v0.1.0`.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal sample (`sleep-for-days`): https://github.com/temporalio/samples-go/tree/main/sleep-for-days
- Temporal sample (`timer`): https://github.com/temporalio/samples-go/tree/main/timer
- Temporal sample (`cron`): https://github.com/temporalio/samples-go/tree/main/cron
- This example's README
