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
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
	"sigs.k8s.io/kueue/pkg/workload"
)

const (
	recordRayJobAdmissionAnnotation = "dd.ray/record-first-admission"
	rayJobFirstAdmittedAnnotation   = "dd.ray/first-admitted-at"
	rayJobMaxUptimeAnnotation       = "dd.ray/ray-job-max-uptime"
)

func (p *Pod) recordRayJobFirstAdmission(ctx context.Context, c client.Client, pod *corev1.Pod, wl *kueue.Workload) error {
	// Opt-in assumes trusted namespace writers; Kubernetes owner references do not authorize the writer.
	if !features.Enabled(features.RayJobFirstAdmissionTime) || pod.Annotations[recordRayJobAdmissionAnnotation] != "true" || pod.Labels["ray.io/node-type"] != "worker" {
		return nil
	}
	hasGPU := false
	for _, container := range pod.Spec.Containers {
		if gpu := container.Resources.Requests[corev1.ResourceName("nvidia.com/gpu")]; gpu.Sign() > 0 {
			hasGPU = true
			break
		}
	}
	if !hasGPU {
		return nil
	}
	if wl == nil || !workload.IsAdmitted(wl) {
		return fmt.Errorf("cannot record RayJob admission for pod %s/%s: workload is not admitted", pod.Namespace, pod.Name)
	}

	cluster, err := rayControllerOwner(ctx, c, pod, "RayCluster")
	if err != nil {
		return err
	}
	job, err := rayControllerOwner(ctx, c, cluster, "RayJob")
	if err != nil {
		return err
	}
	jobUID := job.GetUID()

	// Strict resource-version patches let concurrent workers preserve one winner.
	return clientutil.Patch(ctx, c, job, func() (bool, error) {
		if job.GetUID() != jobUID || !job.GetDeletionTimestamp().IsZero() {
			return false, fmt.Errorf("RayJob %s/%s was replaced or is deleting", job.GetNamespace(), job.GetName())
		}
		annotations := job.GetAnnotations()
		if annotations[recordRayJobAdmissionAnnotation] != "true" {
			return false, nil
		}
		maxUptime, err := strconv.ParseInt(annotations[rayJobMaxUptimeAnnotation], 10, 64)
		if err != nil || maxUptime <= 0 {
			return false, fmt.Errorf("RayJob %s/%s has no positive max uptime", job.GetNamespace(), job.GetName())
		}
		if value, found := annotations[rayJobFirstAdmittedAnnotation]; found {
			admittedAt, err := time.Parse(time.RFC3339Nano, value)
			if err != nil || admittedAt.IsZero() {
				return false, fmt.Errorf("RayJob %s/%s has an invalid first-admitted-at timestamp", job.GetNamespace(), job.GetName())
			}
			return false, nil
		}
		annotations[rayJobFirstAdmittedAnnotation] = p.clock.Now().UTC().Format(time.RFC3339Nano)
		job.SetAnnotations(annotations)
		return true, nil
	}, clientutil.WithRetryOnConflict())
}

func rayControllerOwner(ctx context.Context, c client.Client, child client.Object, kind string) (*unstructured.Unstructured, error) {
	owner := metav1.GetControllerOf(child)
	if owner == nil || owner.APIVersion != "ray.io/v1" || owner.Kind != kind || owner.Name == "" || owner.UID == "" {
		return nil, fmt.Errorf("%s/%s has no valid %s controller owner", child.GetNamespace(), child.GetName(), kind)
	}
	parent := &unstructured.Unstructured{}
	parent.SetGroupVersionKind(schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: kind})
	if err := c.Get(ctx, client.ObjectKey{Namespace: child.GetNamespace(), Name: owner.Name}, parent); err != nil {
		return nil, fmt.Errorf("get %s owner of %s/%s: %w", kind, child.GetNamespace(), child.GetName(), err)
	}
	if parent.GetUID() != owner.UID || !parent.GetDeletionTimestamp().IsZero() {
		return nil, fmt.Errorf("%s owner of %s/%s was replaced or is deleting", kind, child.GetNamespace(), child.GetName())
	}
	return parent, nil
}
