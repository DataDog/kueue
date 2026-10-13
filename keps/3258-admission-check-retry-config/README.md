# KEP-3258: Admission Check Retry Config

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story 1: Static Retry Delay Without Controller Changes](#story-1-static-retry-delay-without-controller-changes)
    - [Story 2: Exponential Backoff](#story-2-exponential-backoff)
    - [Story 3: Jittered Backoff to Avoid Thundering Herd](#story-3-jittered-backoff-to-avoid-thundering-herd)
    - [Story 4: Fail Open After Exhausting Retries on Preprovisioning](#story-4-fail-open-after-exhausting-retries-on-preprovisioning)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [API Changes](#api-changes)
  - [Mapping to <code>pkg/util/wait/backoff.go</code>](#mapping-to-pkgutilwaitbackoffgo)
  - [Implementation Logic](#implementation-logic)
    - [Reconciler Context](#reconciler-context)
    - [Retry Delay Computation](#retry-delay-computation)
  - [Feature Gate](#feature-gate)
  - [Documentation Requirements](#documentation-requirements)
  - [Test Plan](#test-plan)
    - [Unit Tests](#unit-tests)
    - [Integration tests](#integration-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Inline Retry Config on the Workload](#inline-retry-config-on-the-workload)
  - [Configuration API (Kueue Server Config)](#configuration-api-kueue-server-config)
  - [Compute Delay Inline in <code>GetMaxRetryTime</code>](#compute-delay-inline-in-getmaxretrytime)
<!-- /toc -->

## Summary

This KEP extends the `AdmissionCheck` CRD with a `retryConfig` field so that Kueue can **automatically** compute retry delays when an admission check enters the `Retry` state.
Right now every admission check controller has to implement its own delay logic (calculating `RequeueAfterSeconds` and tracking `RetryCount`).
With this field, controllers just set `State=Retry` and Kueue handles the rest, like static delays, exponential backoff, jitter.
The `retryConfig` also supports `maxRetries` and a `failurePolicy` to control what happens when retries are exhausted — either reject the workload (`FailClosed`) or auto-accept it (`FailOpen`).

## Motivation

[KEP-3258 (Delayed Admission Check Retries)](/keps/3258-delayed-admission-check-retries)
introduced `RequeueAfterSeconds` and `RetryCount` on `AdmissionCheckState`, giving controllers fine-grained control over retry timing.
The downside is that it pushes complexity onto every controller author:

- Every controller has to roll its own backoff logic.
- Cluster admins can't tune retry behaviour without modifying controller code.
- The same patterns (exponential backoff with jitter, static delays) get re-implemented from scratch in every controller.

The parent KEP's "Future outlook" section already called this out and sketched a solution.
This KEP makes it concrete.

### Goals

- Let cluster admins configure retry delay strategies declaratively on the `AdmissionCheck` CRD.
- Support `Static` (fixed delay) and `ExponentialBackoff` strategies to start.
- Automatically compute retry delays and populate `RequeueAfterSeconds` on the workload when a controller sets `State=Retry` without providing a delay.
  The existing `GetMaxRetryTime` / `NeedsRequeueAtUpdate` flow handles the rest unchanged.
- Respect controller precedence: if a controller explicitly sets `RequeueAfterSeconds`, Kueue doesn't override it.
- Allow cluster admins to cap the number of retries and define a failure policy (`FailClosed` = reject, `FailOpen` = auto-accept).

### Non-Goals

- Changing the semantics of `RequeueAfterSeconds` or `RetryCount` on the workload status.
- Per-workload retry overrides -> configuration lives on the `AdmissionCheck`.
- Deprecating or removing the ability for controllers to set `RequeueAfterSeconds` directly.
- Adding new admission check states beyond `Pending`, `Ready`, `Retry`, `Rejected`.

## Proposal

Add an optional `retryConfig` field to `AdmissionCheckSpec`.
When set, Kueue computes the delay for any AC that transitions to `Retry` without an explicit `RequeueAfterSeconds`, using the configured strategy and the current `RetryCount`.

### User Stories

#### Story 1: Static Retry Delay Without Controller Changes

As a cluster admin, I deploy a third-party admission check controller that doesn't implement backoff logic.
It just sets `State=Retry` when a resource is unavailable.
I want a 60-second delay between retries without touching the controller.

I create an `AdmissionCheck` with:

```yaml
spec:
  controllerName: example.com/resource-checker
  retryConfig:
    strategy: Static
    baseDelay: 60s
```

Kueue automatically computes a 60-second delay on every retry.

#### Story 2: Exponential Backoff

As a platform engineer, I want my GPU-provisioning admission check to use exponential backoff starting at 30 seconds, doubling each attempt, capped at 10 minutes.

```yaml
spec:
  controllerName: example.com/gpu-provisioner
  retryConfig:
    strategy: ExponentialBackoff
    baseDelay: 30s
    backoff:
      factor: 2.0
      maxDelay: 10m
```

#### Story 3: Jittered Backoff to Avoid Thundering Herd

As a platform engineer, I have hundreds of workloads hitting the same external API.
I want jitter on retries to spread the load.

```yaml
spec:
  controllerName: example.com/api-validator
  retryConfig:
    strategy: ExponentialBackoff
    baseDelay: 10s
    backoff:
      factor: 1.5
      jitter: 0.3
      maxDelay: 5m
```

#### Story 4: Fail Open After Exhausting Retries on Preprovisioning

As a cluster operator, I use a preprovisioning admission check that occasionally fails due to transient infrastructure issues.
If it fails 3 times, I'd rather let the workload proceed without preprovisioned resources than block it indefinitely.
I configure `maxRetries: 3` with `failurePolicy: FailOpen`.

```yaml
spec:
  controllerName: example.com/preprovisioner
  retryConfig:
    strategy: ExponentialBackoff
    backoff:
      baseDelay: 30s
      factor: 2.0
      maxDelay: 5m
    maxRetries: 3
    failurePolicy: FailOpen
```

### Notes/Constraints/Caveats

- Controller-set `RequeueAfterSeconds` always wins.
  The retry config only kicks in when `RequeueAfterSeconds` is nil at the time Kueue processes the `Retry` state.
- `RetryCount` is already incremented by Kueue (parent KEP).
  This KEP reads the count but doesn't change how it's managed.
- Jitter is non-deterministic.
  Actual delays may differ between retries at the same count.
- The retry config delay computation writes `RequeueAfterSeconds` to the workload status and returns early.
  The existing `GetMaxRetryTime` / `NeedsRequeueAtUpdate` flow picks it up on the next reconcile.
  This adds one extra reconcile cycle but avoids any changes to `GetMaxRetryTime` or `pkg/workload`.
- Both `maxRetries` and `failurePolicy` are optional. By default, Kueue retries forever (no cap).
- `maxRetries` uses the existing `RetryCount` on the AC state. When `RetryCount >= maxRetries`, the failure policy is applied instead of scheduling another retry.
- `FailOpen` sets `State=Ready` (the AC is considered passed). `FailClosed` sets `State=Rejected`.

### Risks and Mitigations

**Risk**: Large `maxDelay` values make workloads look stuck.\
**Mitigation**: Surface the computed next-retry time in the AC `message` field.
Expose a metric for retry delay durations.

**Risk**: Interaction with controllers that already set `RequeueAfterSeconds`.\
**Mitigation**: Controller-set values always win.
The retry config is a fallback, not an override.

**Risk**: `FailOpen` silently accepts workloads that should have been blocked.\
**Mitigation**: Emit a Kubernetes event and set the AC `message` to indicate the check was force-accepted due to retry exhaustion. Expose a metric for fail-open events.

## Design Details

### API Changes

Add the following to `AdmissionCheckSpec`:

```go
type AdmissionCheckSpec struct {
    // Existing fields...
    ControllerName string                             `json:"controllerName"`
    Parameters     *AdmissionCheckParametersReference `json:"parameters,omitempty"`

    // RetryConfig configures automatic retry delay computation when an
    // admission check controller sets State=Retry without providing
    // RequeueAfterSeconds. If nil, Kueue does not compute delays
    // automatically.
    // +optional
    RetryConfig *AdmissionCheckRetryConfig `json:"retryConfig,omitempty"`
}
```

New types:

```go
// AdmissionCheckRetryStrategy defines the algorithm used to compute retry delays.
// +kubebuilder:validation:Enum=Static;ExponentialBackoff
type AdmissionCheckRetryStrategy string

const (
    // AdmissionCheckRetryStrategyStatic applies a fixed delay on every retry.
    AdmissionCheckRetryStrategyStatic AdmissionCheckRetryStrategy = "Static"

    // AdmissionCheckRetryStrategyExponentialBackoff applies an exponentially
    // increasing delay, optionally bounded by maxDelay.
    AdmissionCheckRetryStrategyExponentialBackoff AdmissionCheckRetryStrategy = "ExponentialBackoff"
)

// AdmissionCheckFailurePolicy defines what happens when maxRetries is exhausted.
// +kubebuilder:validation:Enum=FailClosed;FailOpen
type AdmissionCheckFailurePolicy string

const (
    // AdmissionCheckFailurePolicyFailClosed rejects the workload when retries are exhausted.
    AdmissionCheckFailurePolicyFailClosed AdmissionCheckFailurePolicy = "FailClosed"

    // AdmissionCheckFailurePolicyFailOpen auto-accepts the admission check when retries are exhausted.
    AdmissionCheckFailurePolicyFailOpen AdmissionCheckFailurePolicy = "FailOpen"
)

// AdmissionCheckRetryConfig holds the retry delay configuration for an admission check.
// +kubebuilder:validation:XValidation:rule="self.strategy != 'Static' || has(self.static)",message="retryConfig.static is required when strategy=Static"
// +kubebuilder:validation:XValidation:rule="self.strategy == 'Static' || !has(self.static)",message="retryConfig.static is only allowed when strategy=Static"
// +kubebuilder:validation:XValidation:rule="self.strategy != 'ExponentialBackoff' || has(self.backoff)",message="retryConfig.backoff is required when strategy=ExponentialBackoff"
// +kubebuilder:validation:XValidation:rule="self.strategy == 'ExponentialBackoff' || !has(self.backoff)",message="retryConfig.backoff is only allowed when strategy=ExponentialBackoff"
// +kubebuilder:validation:XValidation:rule="(has(self.maxRetries) && has(self.failurePolicy)) || (!has(self.maxRetries) && !has(self.failurePolicy))",message="maxRetries and failurePolicy must both be set or both be unset"
type AdmissionCheckRetryConfig struct {
    // Strategy selects the delay algorithm.
    // +required
    // +kubebuilder:validation:Required
    Strategy AdmissionCheckRetryStrategy `json:"strategy"`

    // Static holds additional parameters for the Static strategy.
    // +optional
    Static *AdmissionCheckRetryStaticConfig `json:"static,omitempty"`

    // Backoff holds additional parameters for the ExponentialBackoff strategy.
    // +optional
    Backoff *AdmissionCheckBackoffConfig `json:"backoff,omitempty"`

    // MaxRetries caps the number of retry attempts. If unset, retries continue
    // indefinitely. When set, failurePolicy must also be set.
    // +optional
    // +kubebuilder:validation:Minimum=1
    MaxRetries *int32 `json:"maxRetries,omitempty"`

    // FailurePolicy defines what happens when maxRetries is exhausted.
    // FailClosed rejects the workload; FailOpen auto-accepts the admission check.
    // Required when maxRetries is set (and vice versa).
    // +optional
    FailurePolicy *AdmissionCheckFailurePolicy `json:"failurePolicy,omitempty"`
}

type AdmissionCheckRetryStaticConfig struct {
    // BaseDelay is the initial delay duration. This is the fixed delay.
    // +required
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:Format=duration
    BaseDelay metav1.Duration `json:"baseDelay"`
}

// AdmissionCheckBackoffConfig holds parameters for exponential backoff.
type AdmissionCheckBackoffConfig struct {
    // BaseDelay is the initial delay duration. This is the delay on the first retry.
    // +required
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:Format=duration
    BaseDelay metav1.Duration `json:"baseDelay"`

    // Factor is the multiplier applied on each retry. Defaults to 1.2.
    // +optional
    // +kubebuilder:default=1.2
    // +kubebuilder:validation:Minimum=1.0
    Factor *float64 `json:"factor,omitempty"`

    // Jitter adds randomness to the delay. A value of 0.1 means up to +/-10%
    // of the computed delay. Defaults to 0 (no jitter).
    // +optional
    // +kubebuilder:default=0
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=1.0
    Jitter *float64 `json:"jitter,omitempty"`

    // MaxDelay caps the computed delay. If unset, the delay grows unbounded
    // (limited only by int32 seconds in RequeueAfterSeconds).
    // +optional
    // +kubebuilder:validation:Format=duration
    MaxDelay *metav1.Duration `json:"maxDelay,omitempty"`
}
```

### Mapping to `pkg/util/wait/backoff.go`

The retry config maps directly to the existing `NewBackoff` helper:

| RetryConfig field  | `NewBackoff` parameter |
| ------------------ | ---------------------- |
| `baseDelay`        | `initial`              |
| `backoff.maxDelay` | `cap`                  |
| `backoff.factor`   | `factor`               |
| `backoff.jitter`   | `jitter`               |

For `Static`, Kueue simply uses `baseDelay` as-is (equivalent to `NewBackoff(baseDelay, baseDelay, 1.0, 0)`).

### Implementation Logic

One new step in the workload reconciler (`pkg/controller/core/workload_controller.go`).
No changes to `GetMaxRetryTime`, `NeedsRequeueAtUpdate`, or any other existing function.

#### Reconciler Context

The workload reconciler processes phases in this order (simplified):

1. `NeedsRequeueAtUpdate` → reads AC `RequeueAfterSeconds`, computes `requeueState.requeueAt` (line ~226)
2. Backoff waiting / requeue condition handling (line ~308)
3. `reconcileCheckBasedEviction` → detects `State=Retry` or `State=Rejected`, evicts the workload (line ~498)

#### Retry Delay Computation

Runs **before** `NeedsRequeueAtUpdate`.
For each AC with `State=Retry` and `RequeueAfterSeconds == nil`:

1. Fetch the `AdmissionCheck` CRD and inspect `spec.retryConfig`.
2. If `retryConfig` is nil -> skip (no automatic behaviour).
3. If `maxRetries` is set and `RetryCount >= maxRetries`:
   - `FailClosed`: set `State=Rejected`, add message "admission check rejected after N retries".
   - `FailOpen`: set `State=Ready`, add message "admission check auto-accepted after N retries (failOpen)".
   - Emit a Kubernetes event indicating retry exhaustion.
   - Skip delay computation, return early.
4. Compute the delay:
   - **Static**: `delay = baseDelay`
   - **ExponentialBackoff**: `delay = wait.NewBackoff(baseDelay, maxDelay, factor, jitter).WaitTime(retryCount)`
   - Convert to seconds (round up) and patch `RequeueAfterSeconds` on the AC state.
   - Return early.

Next reconcile, `GetMaxRetryTime` sees the now-populated `RequeueAfterSeconds` and feeds it into `NeedsRequeueAtUpdate` → `SetRequeueState` → `requeueState.requeueAt`.
No changes to `pkg/workload` needed.

This costs one extra reconcile cycle vs. computing the delay inline in `GetMaxRetryTime`, but keeps things simple:

- `GetMaxRetryTime` stays a pure function of `*kueue.Workload`, so no lister or client dependency.
- All retry config logic lives in one place in the controller.
- The workload controller already writes AC states (e.g., `syncAdmissionCheckConditions` sets `State=Pending`), so writing `RequeueAfterSeconds` follows the same pattern.

### Feature Gate

No feature gate needed. `retryConfig` is optional on `AdmissionCheckSpec`.
When absent, the delay computation is a no-op and behaviour is unchanged. Fully opt-in.

### Documentation Requirements

- API reference documentation for the new types.
- A usage guide showing static and exponential backoff examples.
- Migration notes for controller authors who currently compute delays themselves.

### Test Plan

#### Unit Tests

- Controller precedence: AC with `RequeueAfterSeconds` already set is not affected by the delay computation.
- No `retryConfig` on AC: verify delay computation is a no-op.
- `maxRetries` reached with `FailClosed`: AC transitions to `Rejected`.
- `maxRetries` reached with `FailOpen`: AC transitions to `Ready`.
- `maxRetries` not reached: normal retry delay behavior.

#### Integration tests

- End-to-end flow: AC sets `Retry` without delay, delay computation populates `RequeueAfterSeconds`, workload is requeued after the computed delay.
- Mixed scenario: one AC with `retryConfig`, another with controller-set delay to verify Kueue uses max of both computed delays.
- End-to-end fail-open: AC retries 3 times, then auto-accepts. Workload proceeds.
- End-to-end fail-closed: AC retries 3 times, then rejects. Workload is evicted.

### Graduation Criteria

TBD

## Implementation History

- 2026-03-02: Initial KEP proposal

## Drawbacks

- Another optional field on `AdmissionCheckSpec`. More API surface.
- Two sources of delay (controller-set vs. config-computed) could confuse users if not documented well.
- The delay computation costs one extra reconcile cycle (patch `RequeueAfterSeconds`, then the existing flow picks it up next reconcile).
  Small latency hit, but keeps things simple.

## Alternatives

### Inline Retry Config on the Workload

Put retry config fields directly on the workload instead of the `AdmissionCheck` CRD. Rejected because:

- Puts the configuration burden on workload authors instead of platform admins.
- Would require changes to every workload-creating integration (Job, RayJob, etc.).
- AC retry policy is a platform concern, not a workload concern.

### Configuration API (Kueue Server Config)

Global retry config in the Kueue `Configuration` API. Rejected because:

- Different ACs have fundamentally different retry characteristics.
- A single global policy can't accommodate per-check tuning.
- The `AdmissionCheck` CRD is the natural home for check-specific config.

### Compute Delay Inline in `GetMaxRetryTime`

Compute the delay inline in `GetMaxRetryTime` and feed it directly into `requeueState.requeueAt` in a single reconcile cycle, skipping the status write.
Rejected because:

- `GetMaxRetryTime` (`pkg/workload/admissionchecks.go`) only takes a `*kueue.Workload`.
  Computing delays from `retryConfig` means fetching `AdmissionCheck` CRDs.
  Either changing the function signature to accept a lister/client or threading a side-channel map from the controller.
  Both couple `pkg/workload` to API machinery it currently doesn't touch.
- Splits retry config logic across two packages (`pkg/controller/core` and `pkg/workload`), harder to follow.
- The one-cycle saving isn't worth the complexity.
  The workload controller already writes AC states (e.g., `syncAdmissionCheckConditions`), so writing `RequeueAfterSeconds` follows an established pattern.
