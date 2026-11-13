# Record first GPU admission for Altair RayJobs

This prototype records one timestamp before Kueue releases the first opted-in GPU worker from its admission scheduling gate. It does not change maximum duration, warnings, retries, or job termination.

## Enable the prototype in staging

Build a controller image from this branch. Existing release images do not contain the `RayJobFirstAdmissionTime` feature.

1. Enable `RayJobFirstAdmissionTime=true` in the staging controller's feature gates.
2. Confirm that the controller service account can get `ray.io` RayClusters and get and patch RayJobs in the test namespace. The aggregate Kueue ClusterRole already grants these permissions.
3. Add `dd.ray/record-first-admission: "true"` to both the test RayJob's metadata annotations and its GPU worker pod template annotations.
4. Keep the RayJob's existing positive `dd.ray/ray-job-max-uptime` annotation. Select a dedicated ephemeral RayJob, not a shared persistent lab.

The hook also checks the worker's positive `nvidia.com/gpu` request, `ray.io/node-type=worker`, admitted Workload, and same-namespace controller owner chain. Both owner UIDs must match the current RayCluster and RayJob.

With the feature disabled or the worker opt-in absent, the hook makes no Ray API calls. A parent without the opt-in is not modified.

## Verify the record

1. Hold the test worker in its queue. Check that the parent has no `dd.ray/first-admitted-at` annotation.
2. Allow admission. Check that the parent receives an RFC3339Nano timestamp before the worker's admission gate disappears.
3. Restart the controller and admit a replacement worker. Check that the original timestamp remains unchanged.
4. Retry the logical job through Altair. Check that copied RayJob annotations retain the timestamp.
5. In an isolated test only, deny RayJob patch permission for the opted-in path. Check that the worker remains gated and reconciliation reports an error. Restore permission and check that reconciliation proceeds.
6. Run an ordinary workload without opt-in alongside the test. Check that its admission behavior is unchanged.

These staging steps have not been run by this change. Unit tests cover call ordering, opt-out behavior, owner identity checks, API failures, conflict retry, reconstructed controller behavior, and concurrent grouped workers. Real API-server resource-version and leader-handoff behavior still require staging validation.

## Timestamp semantics and limits

`dd.ray/first-admitted-at` records the first successful durable admission-release claim using the controller clock. It is not copied from the Workload's latest `Admitted` condition. The annotation is written once and never moved forward by later workers or readmission.

The RayJob patch and pod gate patch are separate API requests. If the controller stops after recording the timestamp but before removing the gate, the timestamp remains conservatively earlier than actual pod scheduling. This ordering prevents a worker from starting without a durable record. Multiple controllers depend on ordinary Kubernetes resource-version conflict checks to preserve one winning timestamp.

Altair's existing retry path copies the annotation, so it can outlive the current RayCluster and its workers. This prototype does not add policy for deliberate user removal or editing of that metadata.

Owner references and opt-in annotations identify objects; they do not authenticate a namespace tenant. Limit the prototype to a controlled staging namespace. Do not use this metadata as authorization for destructive expiration until the platform establishes trusted metadata writers.

Disable the feature gate to stop new records. Existing annotations remain available for inspection. No cleanup or deletion is required.
