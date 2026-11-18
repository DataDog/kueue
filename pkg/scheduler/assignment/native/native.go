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

package native

import (
	"context"
	"maps"

	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/assignment"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption"
	"sigs.k8s.io/kueue/pkg/workload"
)

// NewPlanner returns a native implementation of assignment.Planner
// that utilizes the internal Kueue topology-assignment and preemption logic.
// It is meant to emulate the real scheduler via internally defined heuristics.
func NewPlanner(
	wl *workload.Info,
	snapshot *schdcache.Snapshot,
	preemptor *preemption.Preemptor,
	assigner *flavorassigner.FlavorAssigner,
) assignment.Planner {
	return &nativePlanner{wl, snapshot, preemptor, assigner}
}

var _ assignment.Planner = (*nativePlanner)(nil)

type nativePlanner struct {
	wl        *workload.Info
	snapshot  *schdcache.Snapshot
	preemptor *preemption.Preemptor
	assigner  *flavorassigner.FlavorAssigner
}

// Plan builds an assignment plan using kueue-native algorithms.
// As of right now, this implementation is expected to never return an Error-Plan.
func (p *nativePlanner) Plan(ctx context.Context, asgn *flavorassigner.Assignment, _ ...assignment.PlannerOption) assignment.Plan {
	log := log.FromContext(ctx)
	cq := p.snapshot.ClusterQueue(p.wl.ClusterQueue)

	if asgn.RepresentativeMode() != flavorassigner.NoFit {
		p.assigner.AssignTopology(ctx, log, asgn)
	}

	arm := asgn.RepresentativeMode()
	if arm == flavorassigner.Fit && asgn.RequiresBorrowing() && features.Enabled(features.FlavorFungibility) &&
		cq.FlavorFungibility.Preference != nil && *cq.FlavorFungibility.Preference == kueue.PreemptionOverBorrowing {
		if preferred, ok := p.preferredInQueuePlan(ctx, asgn); ok {
			return preferred
		}
	}

	if arm == flavorassigner.Preempt {
		strategies := p.preemptor.GetPreemptionStrategyIterator(ctx, *p.wl, p.snapshot, *asgn)
		faPreemptionTargets := p.preemptor.GetTargetsWithStrategy(ctx, strategies)
		if len(faPreemptionTargets) > 0 {
			p.updateAssignmentForTAS(ctx, cq, asgn, faPreemptionTargets)
			resolveNoFit(asgn, cq)
			return assignment.Plan{Assignment: asgn, PreemptionTargets: faPreemptionTargets}
		}
	}

	p.updateAssignmentForTAS(ctx, cq, asgn, nil)
	resolveNoFit(asgn, cq)
	return assignment.Plan{Assignment: asgn, PreemptionTargets: nil}
}

func (p *nativePlanner) preferredInQueuePlan(ctx context.Context, initial *flavorassigner.Assignment) (assignment.Plan, bool) {
	log := log.FromContext(ctx)
	targets := p.preemptor.GetInQueueTargetsWithoutBorrowing(ctx, *p.wl, p.snapshot, *initial)
	if len(targets) == 0 {
		return assignment.Plan{}, false
	}
	workloads := make([]*workload.Info, len(targets))
	for i, target := range targets {
		workloads[i] = target.WorkloadInfo
	}
	restoreUsage := p.snapshot.SimulateWorkloadRemoval(workloads)
	defer restoreUsage()
	restorePods := p.snapshot.SimulatePodRemoval(ctx, log, workloads)
	defer restorePods()
	counts := make([]int32, len(p.wl.Obj.Spec.PodSets))
	for i, ps := range p.wl.Obj.Spec.PodSets {
		for _, assigned := range initial.PodSets {
			if assigned.Name == ps.Name {
				counts[i] = assigned.Count
				break
			}
		}
	}
	alternate := p.assigner.AssignFlavors(ctx, log, counts)
	if alternate.RepresentativeMode() != flavorassigner.Fit || alternate.RequiresBorrowing() ||
		!sameFlavors(initial, &alternate) {
		return assignment.Plan{}, false
	}
	p.assigner.AssignTopology(ctx, log, &alternate)
	if alternate.RepresentativeMode() != flavorassigner.Fit {
		return assignment.Plan{}, false
	}
	alternate.SetRepresentativeMode(flavorassigner.Preempt)
	*initial = alternate
	return assignment.Plan{Assignment: initial, PreemptionTargets: targets}, true
}

func sameFlavors(a, b *flavorassigner.Assignment) bool {
	if len(a.PodSets) != len(b.PodSets) {
		return false
	}
	byName := make(map[kueue.PodSetReference]flavorassigner.ResourceAssignment, len(a.PodSets))
	for _, ps := range a.PodSets {
		byName[ps.Name] = ps.Flavors
	}
	for _, ps := range b.PodSets {
		original, found := byName[ps.Name]
		if !found || !maps.EqualFunc(original, ps.Flavors, func(a, b *flavorassigner.FlavorAssignment) bool { return a.Name == b.Name }) {
			return false
		}
	}
	return true
}

func (p *nativePlanner) updateAssignmentForTAS(
	ctx context.Context,
	cq *schdcache.ClusterQueueSnapshot,
	asgn *flavorassigner.Assignment,
	targets []*preemption.Target,
) {
	log := log.FromContext(ctx)

	if features.Enabled(features.TopologyAwareScheduling) && asgn.RepresentativeMode() == flavorassigner.Preempt &&
		(workload.IsExplicitlyRequestingTAS(p.wl.Obj.Spec.PodSets...) || cq.IsTASOnly()) && !workload.HasTopologyAssignmentWithUnhealthyNode(p.wl.Obj) {
		tasRequests := asgn.WorkloadsTopologyRequests(log, p.wl, cq)
		var tasResult schdcache.TASAssignmentsResult
		log = log.WithValues("workload", klog.KRef(p.wl.Obj.Namespace, p.wl.Obj.Name))

		if len(targets) > 0 {
			var targetWorkloads []*workload.Info
			for _, target := range targets {
				targetWorkloads = append(targetWorkloads, target.WorkloadInfo)
			}
			revertUsage := p.snapshot.SimulateWorkloadRemoval(targetWorkloads)
			// Freeing the victims' quota is not enough. Until the simulator is told,
			// it still reports their Pods and their nodes still look occupied.
			revertPods := p.snapshot.SimulatePodRemoval(ctx, log, targetWorkloads)
			tasResult = cq.FindTopologyAssignmentsForWorkload(
				ctx,
				tasRequests,
				schdcache.WithWorkloadInfo(p.wl),
			)
			revertPods()
			revertUsage()
		} else {
			// In this scenario we don't have any preemption candidates, yet we need
			// to reserve the TAS resources to avoid the situation when a lower
			// priority workload further in the queue gets admitted and preempted
			// in the next scheduling cycle by the waiting workload. To obtain
			// a TAS assignment for reserving the resources we run the algorithm
			// assuming the cluster is empty.
			tasResult = cq.FindTopologyAssignmentsForWorkload(
				ctx,
				tasRequests,
				schdcache.WithSimulateEmpty(true),
				schdcache.WithWorkloadInfo(p.wl),
			)
		}
		asgn.UpdateForTASResult(log, cq, p.wl, tasResult)
	}
}

func resolveNoFit(asgn *flavorassigner.Assignment, cq *schdcache.ClusterQueueSnapshot) {
	if features.Enabled(features.UnadmittedWorkloadsObservability) {
		asgn.ResolveNoFitReason(cq)
	}
}
