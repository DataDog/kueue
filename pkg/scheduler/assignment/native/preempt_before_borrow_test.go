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
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestPreferOwnQueuePreemptionBeforeBorrowing(t *testing.T) {
	const (
		gpu       = corev1.ResourceName("nvidia.com/gpu")
		flavor    = "gpu-test"
		projectA  = kueue.ClusterQueueReference("project-a")
		projectB  = kueue.ClusterQueueReference("project-b")
		tierLabel = "test.aipgp/priority"
		ruleName  = "high-preempts-own-low"
	)
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	low := utiltestingapi.MakeWorkload("background-low", string(projectA)).
		UID(types.UID("background-low")).Priority(100).Label(tierLabel, "low").
		Request(gpu, "4").
		SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj()
	medium := utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
		UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
		Request(gpu, "4").
		SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj()
	incoming := utiltestingapi.MakeWorkload("incoming", string(projectA)).UID("incoming").
		Priority(300).Label(tierLabel, "high").Request(gpu, "4").Obj()
	multiplePodSets := utiltestingapi.MakeWorkload("incoming", string(projectA)).UID("incoming").
		Priority(300).Label(tierLabel, "high").PodSets(
		*utiltestingapi.MakePodSet("first", 1).SetMinimumCount(1).Request(gpu, "2").Request(corev1.ResourceCPU, "2").Obj(),
		*utiltestingapi.MakePodSet("second", 1).SetMinimumCount(1).Request(gpu, "2").Request(corev1.ResourceCPU, "2").Obj(),
	)
	cases := map[string]struct {
		selectiveRule      bool
		disableFungibility bool
		preference         *kueue.FlavorFungibilityPreference
		withinQueue        kueue.PreemptionPolicy
		ruleScope          kueuealpha.PreemptionConfigPreemptionQueueScope
		admitted           []*kueue.Workload
		incoming           *kueue.Workload
		counts             []int32
		wantFit            bool
		wantBorrowing      bool
		wantVictims        []string
	}{
		"current policy keeps borrowing": {
			wantFit: true, wantBorrowing: true,
		},
		"selective rule without preference keeps borrowing": {
			selectiveRule: true, wantFit: true, wantBorrowing: true,
		},
		"explicit borrowing preference keeps borrowing": {
			selectiveRule: true, preference: new(kueue.BorrowingOverPreemption), wantFit: true, wantBorrowing: true,
		},
		"prefer own low-priority preemption over spare cohort capacity": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true,
			wantVictims: []string{"project-a/background-low"},
		},
		"disabled flavor fungibility keeps borrowing": {
			selectiveRule: true, disableFungibility: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true, wantBorrowing: true,
		},
		"preference does not grant victim permission": {
			preference: new(kueue.PreemptionOverBorrowing), wantFit: true, wantBorrowing: true,
		},
		"medium preemptor cannot use the high-only rule": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true, wantBorrowing: true,
			incoming: utiltestingapi.MakeWorkload("incoming", string(projectA)).UID("incoming").
				Priority(200).Label(tierLabel, "medium").Request(gpu, "4").Obj(),
		},
		"high cannot preempt medium": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true, wantBorrowing: true,
			admitted: []*kueue.Workload{utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
				UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
				Request(gpu, "8").
				SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj()},
		},
		"insufficient low capacity preserves the borrowing plan": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true, wantBorrowing: true,
			admitted: []*kueue.Workload{
				utiltestingapi.MakeWorkload("background-low", string(projectA)).
					UID(types.UID("background-low")).Priority(100).Label(tierLabel, "low").
					Request(gpu, "2").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
				utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
					UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
					Request(gpu, "6").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
			},
		},
		"preferred attempt intersects an any-queue rule with own queue": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing),
			ruleScope: kueuealpha.AnyClusterQueue, wantFit: true, wantBorrowing: true,
			admitted: []*kueue.Workload{
				utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
					UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
					Request(gpu, "8").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
				utiltestingapi.MakeWorkload("other-low", string(projectB)).
					UID(types.UID("other-low")).Priority(100).Label(tierLabel, "low").
					Request(gpu, "4").
					SimpleReserveQuota(projectB, flavor, now).AdmittedAt(true, now).Obj(),
			},
		},
		"unused own quota does not cause preemption": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true,
			admitted: []*kueue.Workload{low},
		},
		"GPU victims cannot eliminate CPU borrowing": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true, wantBorrowing: true,
			admitted: []*kueue.Workload{low, utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
				UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
				Request(gpu, "4").
				Request(corev1.ResourceCPU, "8").
				SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj()},
			incoming: utiltestingapi.MakeWorkload("incoming", string(projectA)).UID("incoming").
				Priority(300).Label(tierLabel, "high").Request(gpu, "4").Request(corev1.ResourceCPU, "4").Obj(),
		},
		"all resources fit after permitted victims": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true,
			wantVictims: []string{"project-a/background-low"},
			admitted: []*kueue.Workload{
				utiltestingapi.MakeWorkload("background-low", string(projectA)).
					UID(types.UID("background-low")).Priority(100).Label(tierLabel, "low").
					Request(gpu, "4").
					Request(corev1.ResourceCPU, "4").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
				utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
					UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
					Request(gpu, "4").
					Request(corev1.ResourceCPU, "4").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
			},
			incoming: utiltestingapi.MakeWorkload("incoming", string(projectA)).UID("incoming").
				Priority(300).Label(tierLabel, "high").Request(gpu, "4").Request(corev1.ResourceCPU, "4").Obj(),
		},
		"multiple PodSets preserve borrowing when CPU cannot fit locally": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true, wantBorrowing: true,
			admitted: []*kueue.Workload{low, utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
				UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
				Request(gpu, "4").
				Request(corev1.ResourceCPU, "8").
				SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj()},
			incoming: multiplePodSets.Clone().Obj(),
		},
		"multiple PodSets preempt one sufficient low workload": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true,
			wantVictims: []string{"project-a/background-low"}, incoming: multiplePodSets.Clone().Obj(),
			admitted: []*kueue.Workload{
				utiltestingapi.MakeWorkload("background-low", string(projectA)).
					UID(types.UID("background-low")).Priority(100).Label(tierLabel, "low").
					Request(gpu, "4").
					Request(corev1.ResourceCPU, "4").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
				utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
					UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
					Request(gpu, "4").
					Request(corev1.ResourceCPU, "4").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
			},
		},
		"partial PodSet counts are not expanded during alternate assignment": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true,
			wantVictims: []string{"project-a/background-low"}, incoming: multiplePodSets.Clone().PodSets(
				*utiltestingapi.MakePodSet("first", 2).SetMinimumCount(1).Request(gpu, "2").Request(corev1.ResourceCPU, "2").Obj(),
				*utiltestingapi.MakePodSet("second", 2).SetMinimumCount(1).Request(gpu, "2").Request(corev1.ResourceCPU, "2").Obj(),
			).Obj(), counts: []int32{1, 1},
			admitted: []*kueue.Workload{
				utiltestingapi.MakeWorkload("background-low", string(projectA)).
					UID(types.UID("background-low")).Priority(100).Label(tierLabel, "low").
					Request(gpu, "4").
					Request(corev1.ResourceCPU, "4").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
				utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
					UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
					Request(gpu, "4").
					Request(corev1.ResourceCPU, "4").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
			},
		},
		"classic in-queue permissions also support the preference": {
			preference: new(kueue.PreemptionOverBorrowing), withinQueue: kueue.PreemptionPolicyLowerPriority,
			wantFit: true, wantVictims: []string{"project-a/background-low"},
		},
		"without spare capacity the existing selective rule still preempts": {
			selectiveRule: true, wantFit: true, wantVictims: []string{"project-a/background-low"},
			admitted: []*kueue.Workload{low, medium, utiltestingapi.MakeWorkload("other-work", string(projectB)).
				UID(types.UID("other-work")).Priority(200).Label(tierLabel, "medium").
				Request(gpu, "8").
				SimpleReserveQuota(projectB, flavor, now).AdmittedAt(true, now).Obj()},
		},
		"protected work is not preempted when borrowing is unavailable": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing),
			admitted: []*kueue.Workload{
				utiltestingapi.MakeWorkload("protected-medium", string(projectA)).
					UID(types.UID("protected-medium")).Priority(200).Label(tierLabel, "medium").
					Request(gpu, "8").
					SimpleReserveQuota(projectA, flavor, now).AdmittedAt(true, now).Obj(),
				utiltestingapi.MakeWorkload("other-work", string(projectB)).
					UID(types.UID("other-work")).Priority(200).Label(tierLabel, "medium").
					Request(gpu, "8").
					SimpleReserveQuota(projectB, flavor, now).AdmittedAt(true, now).Obj(),
			},
		},
		"existing owner reclaim rights remain unchanged": {
			selectiveRule: true, preference: new(kueue.PreemptionOverBorrowing), wantFit: true,
			wantVictims: []string{"project-b/other-work"},
			admitted: []*kueue.Workload{medium, utiltestingapi.MakeWorkload("other-work", string(projectB)).
				UID(types.UID("other-work")).Priority(200).Label(tierLabel, "medium").
				Request(gpu, "12").
				SimpleReserveQuota(projectB, flavor, now).AdmittedAt(true, now).Obj()},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.FlavorFungibility, !tc.disableFungibility)
			features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, false)
			features.SetFeatureGateDuringTest(t, features.ConfigurablePreemptions, tc.selectiveRule)
			ctx, log := utiltesting.ContextWithLog(t)
			scope := tc.ruleScope
			if scope == "" {
				scope = kueuealpha.WithinClusterQueue
			}
			rule := utiltestingalpha.MakePreemptionConfig(ruleName).
				RuleWithPreemptorSelector(ruleName, kueuealpha.Always,
					&metav1.LabelSelector{MatchLabels: map[string]string{tierLabel: "high"}},
					utiltestingalpha.MakeCandidateSelector(scope).
						LabelSelector(&metav1.LabelSelector{MatchLabels: map[string]string{tierLabel: "low"}}).Obj(),
				).Obj()
			running := tc.admitted
			if running == nil {
				running = []*kueue.Workload{low, medium}
			}
			admittedList := make([]kueue.Workload, len(running))
			for i, w := range running {
				admittedList[i] = *w.DeepCopy()
			}
			cl := utiltesting.NewClientBuilder().WithObjects(rule).
				WithIndex(&corev1.LimitRange{}, indexer.LimitRangeHasContainerOrPodType, indexer.IndexLimitRangeHasContainerOrPodType).
				WithLists(&kueue.WorkloadList{Items: admittedList}).Build()
			cache := schdcache.New(cl)
			cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor(flavor).Obj())
			if err := cache.AddOrUpdateCohort(utiltestingapi.MakeCohort("shared-gpus").Obj()); err != nil {
				t.Fatal(err)
			}
			for _, project := range []kueue.ClusterQueueReference{projectA, projectB} {
				withinQueue := tc.withinQueue
				if withinQueue == "" {
					withinQueue = kueue.PreemptionPolicyNever
				}
				cq := utiltestingapi.MakeClusterQueue(string(project)).Cohort("shared-gpus").
					Preemption(kueue.ClusterQueuePreemption{
						WithinClusterQueue: withinQueue, ReclaimWithinCohort: kueue.PreemptionPolicyAny,
						BorrowWithinCohort: &kueue.BorrowWithinCohort{Policy: kueue.BorrowWithinCohortPolicyNever},
					}).ResourceGroup(*utiltestingapi.MakeFlavorQuotas(flavor).Resource(gpu, "8").Resource(corev1.ResourceCPU, "8").Obj())
				if project == projectA {
					if tc.selectiveRule {
						cq.Annotation(kueuealpha.PreemptionConfigNameAnnotation, ruleName)
					}
					if tc.preference != nil {
						cq.FlavorFungibility(kueue.FlavorFungibility{
							WhenCanBorrow: kueue.TryNextFlavor, WhenCanPreempt: kueue.TryNextFlavor, Preference: tc.preference,
						})
					}
				}
				if err := cache.AddClusterQueue(ctx, cq.Obj()); err != nil {
					t.Fatal(err)
				}
			}
			for i := range admittedList {
				if !cache.AddOrUpdateWorkload(ctx, log, &admittedList[i]) {
					t.Fatalf("adding admitted workload %s", admittedList[i].Name)
				}
			}
			snapshot, err := cache.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			input := tc.incoming
			if input == nil {
				input = incoming
			}
			wl := workload.NewInfo(log, input.DeepCopy())
			wl.ClusterQueue = projectA
			preemptor := preemption.New(cl, workload.Ordering{}, &utiltesting.EventRecorder{}, nil,
				false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)
			assigner := flavorassigner.New(wl, snapshot.ClusterQueue(projectA), snapshot.ResourceFlavors,
				false, preemption.NewOracle(preemptor, snapshot), nil, configapi.QuotaCheckBlockUndeclared,
				resources.NewResourceFormatter(), 0)
			before := preferenceSnapshotState(snapshot)
			for attempt := range 2 {
				initial := assigner.AssignFlavors(ctx, log, tc.counts)
				original := initial.ToAPI(log)
				plan := NewPlanner(wl, snapshot, preemptor, assigner).Plan(ctx, &initial)
				victims := []string{}
				for _, target := range plan.PreemptionTargets {
					victims = append(victims, string(workload.Key(target.WorkloadInfo.Obj)))
				}
				slices.Sort(victims)
				wantVictims := tc.wantVictims
				if wantVictims == nil {
					wantVictims = []string{}
				}
				if plan.CanFit() != tc.wantFit {
					t.Errorf("can fit: want %v, got %v", tc.wantFit, plan.CanFit())
				}
				if tc.wantFit && plan.Assignment.RequiresBorrowing() != tc.wantBorrowing {
					t.Errorf("borrowing: want %v, got %v", tc.wantBorrowing, plan.Assignment.RequiresBorrowing())
				}
				if diff := cmp.Diff(wantVictims, victims); diff != "" {
					t.Errorf("victims differ (-want +got): %s", diff)
				}
				if len(victims) == 0 {
					if diff := cmp.Diff(original, plan.Assignment.ToAPI(log)); diff != "" {
						t.Errorf("fallback assignment changed (-want +got): %s", diff)
					}
				}
				for _, ps := range plan.Assignment.PodSets {
					for _, initialPS := range original {
						if ps.Name == initialPS.Name && ps.Count != *initialPS.Count {
							t.Errorf("PodSet %s count: want %d, got %d", ps.Name, *initialPS.Count, ps.Count)
						}
					}
				}
				if diff := cmp.Diff(before, preferenceSnapshotState(snapshot)); diff != "" {
					t.Errorf("planning changed snapshot (-before +after): %s", diff)
				}
				if attempt == 0 {
					encoded, err := json.Marshal(map[string]any{"case": name, "assignment_mode": plan.Assignment.RepresentativeMode().String(),
						"can_fit": plan.CanFit(), "requires_borrowing": plan.Assignment.RequiresBorrowing(), "selected_victims": victims})
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("AIPGP456_OBSERVATION %s", encoded)
				}
			}
		})
	}
}

func preferenceSnapshotState(snapshot *schdcache.Snapshot) map[string]map[string]string {
	state := make(map[string]map[string]string)
	for name, cq := range snapshot.ClusterQueues() {
		values := make(map[string]string)
		for key := range cq.Workloads {
			values["workload:"+string(key)] = "present"
		}
		for fr, quantity := range cq.ResourceNode.Usage {
			values["usage:"+string(fr.Flavor)+":"+string(fr.Resource)] = quantity.String()
		}
		state["queue:"+string(name)] = values
	}
	for name, cohort := range snapshot.Cohorts() {
		values := make(map[string]string)
		for fr, quantity := range cohort.ResourceNode.Usage {
			values[string(fr.Flavor)+":"+string(fr.Resource)] = quantity.String()
		}
		state["cohort:"+string(name)] = values
	}
	return state
}
