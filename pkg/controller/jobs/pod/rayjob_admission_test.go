/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pod

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
)

func TestRunRayJobFirstAdmission(t *testing.T) {
	now := time.Date(2026, 11, 13, 20, 0, 0, 123, time.UTC)
	earlier := now.Add(-time.Hour).Format(time.RFC3339Nano)
	cases := map[string]struct {
		change       func(*corev1.Pod, *unstructured.Unstructured, *unstructured.Unstructured, *kueue.Workload)
		gateOff      bool
		getFailure   bool
		patchFailure bool
		wantError    string
		wantStamp    string
		wantRayCalls bool
	}{
		"first admitted GPU worker records before ungating": {wantStamp: now.Format(time.RFC3339Nano), wantRayCalls: true},
		"feature disabled makes no Ray calls":               {gateOff: true},
		"worker has not opted in": {change: func(p *corev1.Pod, _, _ *unstructured.Unstructured, _ *kueue.Workload) {
			delete(p.Annotations, recordRayJobAdmissionAnnotation)
		}},
		"CPU worker makes no Ray calls": {change: func(p *corev1.Pod, _, _ *unstructured.Unstructured, _ *kueue.Workload) {
			delete(p.Spec.Containers[0].Resources.Requests, corev1.ResourceName("nvidia.com/gpu"))
		}},
		"head pod makes no Ray calls": {change: func(p *corev1.Pod, _, _ *unstructured.Unstructured, _ *kueue.Workload) {
			p.Labels["ray.io/node-type"] = "head"
		}},
		"parent has not opted in": {change: func(_ *corev1.Pod, _, j *unstructured.Unstructured, _ *kueue.Workload) {
			a := j.GetAnnotations()
			delete(a, recordRayJobAdmissionAnnotation)
			j.SetAnnotations(a)
		}, wantRayCalls: true},
		"queued worker remains gated": {change: func(_ *corev1.Pod, _, _ *unstructured.Unstructured, w *kueue.Workload) {
			w.Status.Conditions = nil
		}, wantError: "workload is not admitted"},
		"recreated RayCluster remains gated": {change: func(_ *corev1.Pod, c, _ *unstructured.Unstructured, _ *kueue.Workload) {
			c.SetUID("replacement-cluster")
		}, wantError: "was replaced", wantRayCalls: true},
		"recreated RayJob remains gated": {change: func(_ *corev1.Pod, _, j *unstructured.Unstructured, _ *kueue.Workload) {
			j.SetUID("replacement-job")
		}, wantError: "was replaced", wantRayCalls: true},
		"missing controller owner remains gated": {change: func(p *corev1.Pod, _, _ *unstructured.Unstructured, _ *kueue.Workload) {
			p.OwnerReferences = nil
		}, wantError: "no valid RayCluster"},
		"max uptime marker is required": {change: func(_ *corev1.Pod, _, j *unstructured.Unstructured, _ *kueue.Workload) {
			a := j.GetAnnotations()
			a[rayJobMaxUptimeAnnotation] = "0"
			j.SetAnnotations(a)
		}, wantError: "no positive max uptime", wantRayCalls: true},
		"invalid timestamp remains gated": {change: func(_ *corev1.Pod, _, j *unstructured.Unstructured, _ *kueue.Workload) {
			a := j.GetAnnotations()
			a[rayJobFirstAdmittedAnnotation] = "invalid"
			j.SetAnnotations(a)
		}, wantError: "invalid first-admitted-at", wantStamp: "invalid", wantRayCalls: true},
		"readmission preserves the previous timestamp": {change: func(_ *corev1.Pod, _, j *unstructured.Unstructured, _ *kueue.Workload) {
			a := j.GetAnnotations()
			a[rayJobFirstAdmittedAnnotation] = earlier
			j.SetAnnotations(a)
		}, wantStamp: earlier, wantRayCalls: true},
		"forbidden owner read remains gated":   {getFailure: true, wantError: "forbidden", wantRayCalls: true},
		"failed timestamp write remains gated": {patchFailure: true, wantError: "forbidden", wantRayCalls: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.RayJobFirstAdmissionTime, !tc.gateOff)
			ctx, _ := utiltesting.ContextWithLog(t)
			pod, cluster, job := rayAdmissionObjects()
			wl := utiltestingapi.MakeWorkload("wl", pod.Namespace).SimpleReserveQuota("cq", "gpu", now).AdmittedAt(true, now).Obj()
			if tc.change != nil {
				tc.change(pod, cluster, job, wl)
			}
			beforeAnnotations := job.GetAnnotations()
			rayCalls := false
			kClient := utiltesting.NewClientBuilder().WithObjects(pod.DeepCopy(), cluster.DeepCopy(), job.DeepCopy()).WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if obj.GetObjectKind().GroupVersionKind().Group == "ray.io" {
						rayCalls = true
						if tc.getFailure {
							return apierrors.NewForbidden(schema.GroupResource{Group: "ray.io", Resource: "rayclusters"}, key.Name, fmt.Errorf("test read failure"))
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if obj.GetObjectKind().GroupVersionKind().Kind == "RayJob" && tc.patchFailure {
						return apierrors.NewForbidden(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, obj.GetName(), fmt.Errorf("test write failure"))
					}
					if _, isPod := obj.(*corev1.Pod); isPod && tc.wantStamp != "" {
						stored := job.DeepCopy()
						if err := c.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
							return err
						}
						if got := stored.GetAnnotations()[rayJobFirstAdmittedAnnotation]; got != tc.wantStamp {
							t.Errorf("pod ungated before first admission persisted: got %q, want %q", got, tc.wantStamp)
						}
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			}).Build()
			p := FromObject(pod.DeepCopy())
			p.clock = testingclock.NewFakeClock(now)
			err := p.Run(ctx, kClient, wl, []podset.PodSetInfo{{Name: "main"}}, nil, "")
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("Run error = %v, want %q", err, tc.wantError)
			}
			if rayCalls != tc.wantRayCalls {
				t.Errorf("Ray API accessed = %v, want %v", rayCalls, tc.wantRayCalls)
			}
			stored := pod.DeepCopy()
			if err := kClient.Get(ctx, client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatal(err)
			}
			if got, want := isGated(stored), tc.wantError != ""; got != want {
				t.Errorf("pod gated = %v, want %v", got, want)
			}
			storedJob := job.DeepCopy()
			if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), storedJob); err != nil && !tc.getFailure {
				t.Fatal(err)
			}
			if !tc.getFailure {
				want := beforeAnnotations
				if tc.wantStamp != "" {
					want[rayJobFirstAdmittedAnnotation] = tc.wantStamp
				}
				if diff := cmp.Diff(want, storedJob.GetAnnotations()); diff != "" {
					t.Errorf("RayJob annotations mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestRunRayJobAdmissionConflictPreservesWinner(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayJobFirstAdmissionTime, true)
	ctx, _ := utiltesting.ContextWithLog(t)
	pod, cluster, job := rayAdmissionObjects()
	now := time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)
	winner := now.Add(-time.Second).Format(time.RFC3339Nano)
	wl := utiltestingapi.MakeWorkload("wl", pod.Namespace).SimpleReserveQuota("cq", "gpu", now).AdmittedAt(true, now).Obj()
	conflicted := false
	kClient := utiltesting.NewClientBuilder().WithObjects(pod, cluster, job).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == "RayJob" && !conflicted {
				conflicted = true
				stored := job.DeepCopy()
				if err := c.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
					return err
				}
				a := stored.GetAnnotations()
				a[rayJobFirstAdmittedAnnotation] = winner
				stored.SetAnnotations(a)
				if err := c.Update(ctx, stored); err != nil {
					return err
				}
				return apierrors.NewConflict(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, obj.GetName(), fmt.Errorf("another worker recorded admission"))
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	p := FromObject(pod.DeepCopy())
	p.clock = testingclock.NewFakeClock(now)
	if err := p.Run(ctx, kClient, wl, []podset.PodSetInfo{{Name: "main"}}, nil, ""); err != nil {
		t.Fatal(err)
	}
	if !conflicted {
		t.Fatal("test did not exercise conflicting admission writes")
	}
	stored := job.DeepCopy()
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
		t.Fatal(err)
	}
	if got := stored.GetAnnotations()[rayJobFirstAdmittedAnnotation]; got != winner {
		t.Errorf("timestamp = %q, want concurrent winner %q", got, winner)
	}

	replacement := pod.DeepCopy()
	replacement.Name = "replacement"
	replacement.ResourceVersion = ""
	if err := kClient.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	p = FromObject(replacement)
	p.clock = testingclock.NewFakeClock(now.Add(time.Hour))
	if err := p.Run(ctx, kClient, wl, []podset.PodSetInfo{{Name: "main"}}, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
		t.Fatal(err)
	}
	if got := stored.GetAnnotations()[rayJobFirstAdmittedAnnotation]; got != winner {
		t.Errorf("restart changed timestamp to %q, want %q", got, winner)
	}
}

func TestRunRayJobAdmissionRecoversAfterUngateFailure(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayJobFirstAdmissionTime, true)
	ctx, _ := utiltesting.ContextWithLog(t)
	pod, cluster, job := rayAdmissionObjects()
	now := time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)
	wl := utiltestingapi.MakeWorkload("wl", pod.Namespace).SimpleReserveQuota("cq", "gpu", now).AdmittedAt(true, now).Obj()
	failPodPatch := true
	kClient := utiltesting.NewClientBuilder().WithObjects(pod, cluster, job).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod && failPodPatch {
				return fmt.Errorf("test pod patch failure")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	p := FromObject(pod.DeepCopy())
	p.clock = testingclock.NewFakeClock(now)
	if err := p.Run(ctx, kClient, wl, []podset.PodSetInfo{{Name: "main"}}, nil, ""); err == nil {
		t.Fatal("expected failed pod gate patch")
	}
	storedPod := pod.DeepCopy()
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(pod), storedPod); err != nil {
		t.Fatal(err)
	}
	if !isGated(storedPod) {
		t.Fatal("worker ran despite failed gate patch")
	}
	storedJob := job.DeepCopy()
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), storedJob); err != nil {
		t.Fatal(err)
	}
	if got, want := storedJob.GetAnnotations()[rayJobFirstAdmittedAnnotation], now.Format(time.RFC3339Nano); got != want {
		t.Fatalf("durable claim = %q, want %q", got, want)
	}
	failPodPatch = false
	p = FromObject(storedPod)
	p.clock = testingclock.NewFakeClock(now.Add(time.Hour))
	if err := p.Run(ctx, kClient, wl, []podset.PodSetInfo{{Name: "main"}}, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), storedJob); err != nil {
		t.Fatal(err)
	}
	if got, want := storedJob.GetAnnotations()[rayJobFirstAdmittedAnnotation], now.Format(time.RFC3339Nano); got != want {
		t.Errorf("recovery reset durable claim to %q, want %q", got, want)
	}
}

func TestRunRayJobAdmissionPodGroup(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayJobFirstAdmissionTime, true)
	ctx, _ := utiltesting.ContextWithLog(t)
	pod, cluster, job := rayAdmissionObjects()
	pod.Annotations[podconstants.RoleHashAnnotation] = "gpu"
	second := pod.DeepCopy()
	second.Name = "worker-2"
	now := time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)
	wl := utiltestingapi.MakeWorkload("wl", pod.Namespace).SimpleReserveQuota("cq", "gpu", now).AdmittedAt(true, now).Obj()
	kClient := utiltesting.NewClientBuilder().WithObjects(pod, second, cluster, job).Build()
	p := NewPod(WithClock(testingclock.NewFakeClock(now)))
	p.isGroup = true
	p.list.Items = []corev1.Pod{*pod.DeepCopy(), *second.DeepCopy()}
	if err := p.Run(ctx, kClient, wl, []podset.PodSetInfo{{Name: "gpu"}}, nil, ""); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{pod.Name, second.Name} {
		stored := &corev1.Pod{}
		if err := kClient.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: name}, stored); err != nil {
			t.Fatal(err)
		}
		if isGated(stored) {
			t.Errorf("admitted worker %q remains gated", name)
		}
	}
	stored := job.DeepCopy()
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
		t.Fatal(err)
	}
	if got, want := stored.GetAnnotations()[rayJobFirstAdmittedAnnotation], now.Format(time.RFC3339Nano); got != want {
		t.Errorf("timestamp = %q, want %q", got, want)
	}
}

func rayAdmissionObjects() (*corev1.Pod, *unstructured.Unstructured, *unstructured.Unstructured) {
	pod := testingpod.MakePod("worker", "test-ns").Label("ray.io/node-type", "worker").
		Annotation(recordRayJobAdmissionAnnotation, "true").Request("nvidia.com/gpu", "1").
		KueueSchedulingGate().OwnerReferenceWithUID("cluster", schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayCluster"}, "cluster-uid").Obj()
	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayCluster"})
	cluster.SetName("cluster")
	cluster.SetNamespace(pod.Namespace)
	cluster.SetUID("cluster-uid")
	cluster.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: "RayJob", Name: "job", UID: "job-uid", Controller: ptr.To(true)}})
	job := &unstructured.Unstructured{}
	job.SetGroupVersionKind(schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayJob"})
	job.SetName("job")
	job.SetNamespace(pod.Namespace)
	job.SetUID("job-uid")
	job.SetAnnotations(map[string]string{recordRayJobAdmissionAnnotation: "true", rayJobMaxUptimeAnnotation: "604800", "unrelated": "preserved"})
	return pod, cluster, job
}
