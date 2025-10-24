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

package delayedadmission

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	config "sigs.k8s.io/kueue/apis/config/v1beta1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta1"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/test/util"
)

var _ = ginkgo.Describe("SchedulerWithDelayedAdmissionChecks", func() {
	var (
		// Values referenced by tests:
		defaultFlavor *kueue.ResourceFlavor
		ns            *corev1.Namespace
		clusterQ      *kueue.ClusterQueue
		localQ        *kueue.LocalQueue
		delayedCheck  *kueue.AdmissionCheck
		realClock     = clock.RealClock{}
	)

	ginkgo.JustBeforeEach(func() {
		fwk.StartManager(ctx, cfg, managerAndSchedulerSetup(&config.Configuration{}))

		defaultFlavor = utiltestingapi.MakeResourceFlavor("default").Obj()
		util.MustCreate(ctx, k8sClient, defaultFlavor)

		ns = util.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "podsready-")

		delayedCheck = utiltestingapi.MakeAdmissionCheck("delayed-check").ControllerName("ctrl").Obj()
		util.MustCreate(ctx, k8sClient, delayedCheck)
		util.SetAdmissionCheckActive(ctx, k8sClient, delayedCheck, metav1.ConditionTrue)

		clusterQ = utiltestingapi.MakeClusterQueue("dev-cq").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "5").Obj()).
			AdmissionChecks(kueue.AdmissionCheckReference(delayedCheck.Name)).
			Obj()
		util.MustCreate(ctx, k8sClient, clusterQ)

		localQ = utiltestingapi.MakeLocalQueue("dev-queue", ns.Name).ClusterQueue(clusterQ.Name).Obj()
		util.MustCreate(ctx, k8sClient, localQ)

		util.ExpectClusterQueuesToBeActive(ctx, k8sClient, clusterQ)
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(util.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		util.ExpectObjectToBeDeleted(ctx, k8sClient, clusterQ, true)
		util.ExpectObjectToBeDeleted(ctx, k8sClient, defaultFlavor, true)
		util.ExpectObjectToBeDeleted(ctx, k8sClient, delayedCheck, true)
		fwk.StopManager(ctx)
	})

	ginkgo.Context("Long PodsReady timeout", func() {
		ginkgo.BeforeEach(func() {
		})

		ginkgo.It("should do something", func() {
			wl := utiltestingapi.MakeWorkload("delayed-ac-retry", ns.Name).
				RequestAndLimit(corev1.ResourceCPU, "1").
				Queue(kueue.LocalQueueName(localQ.Name)).
				Obj()

			ginkgo.By("Creating the job", func() {
				util.MustCreate(ctx, k8sClient, wl)
			})
			wlLookupKey := client.ObjectKeyFromObject(wl)
			createdWorkload := &kueue.Workload{}

			ginkgo.By("Verifying workload is created but not admitted yet", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					g.Expect(workload.HasQuotaReservation(createdWorkload)).To(gomega.BeTrue())
					g.Expect(workload.IsAdmitted(createdWorkload)).To(gomega.BeFalse())
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Marking AC as Retry to trigger an immediate retry", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					workload.SetAdmissionCheckState(&createdWorkload.Status.AdmissionChecks, kueue.AdmissionCheckState{
						Name:       kueue.AdmissionCheckReference(delayedCheck.Name),
						State:      kueue.CheckStateRetry,
						Message:    "Retrying admission check",
						RetryCount: ptr.To(int32(20)), // This should be overwritten by kueue
					}, realClock)
					g.Expect(k8sClient.Status().Update(ctx, createdWorkload)).Should(gomega.Succeed())
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Finish eviction", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					util.FinishEvictionForWorkloads(ctx, k8sClient, createdWorkload)
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Verifying retry counter is incremented after transition to Pending", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())

					ac := admissioncheck.FindAdmissionCheck(createdWorkload.Status.AdmissionChecks, kueue.AdmissionCheckReference(delayedCheck.Name))
					g.Expect(ac).ToNot(gomega.BeNil())
					g.Expect(ac.State).To(gomega.Equal(kueue.CheckStatePending))
					g.Expect(ac.LastTransitionTime).ToNot(gomega.BeNil())
					g.Expect(ac.RetryCount).ToNot(gomega.BeNil())
					g.Expect(*ac.RetryCount).To(gomega.Equal(int32(1)))

					g.Expect(createdWorkload.Status.RequeueState).To(gomega.BeNil())
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Wait for job to have quota again", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					g.Expect(workload.HasQuotaReservation(createdWorkload)).To(gomega.BeTrue())
					g.Expect(workload.IsAdmitted(createdWorkload)).To(gomega.BeFalse())
				}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Marking AC as Retry to trigger a delayed retry", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					workload.SetAdmissionCheckState(&createdWorkload.Status.AdmissionChecks, kueue.AdmissionCheckState{
						Name:                kueue.AdmissionCheckReference(delayedCheck.Name),
						State:               kueue.CheckStateRetry,
						Message:             "Retrying admission check",
						RetryCount:          ptr.To(int32(20)), // This should be overwritten by kueue
						RequeueAfterSeconds: ptr.To(int32(5)),
					}, realClock)
					g.Expect(k8sClient.Status().Update(ctx, createdWorkload)).Should(gomega.Succeed())
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Finish eviction", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					util.FinishEvictionForWorkloads(ctx, k8sClient, createdWorkload)
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Verifying retry counter is incremented after transition to Pending", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())

					ac := admissioncheck.FindAdmissionCheck(createdWorkload.Status.AdmissionChecks, kueue.AdmissionCheckReference(delayedCheck.Name))
					g.Expect(ac).ToNot(gomega.BeNil())
					g.Expect(ac.State).To(gomega.Equal(kueue.CheckStatePending))
					g.Expect(ac.LastTransitionTime).To(gomega.Not(gomega.BeNil()))
					g.Expect(ac.RetryCount).ToNot(gomega.BeNil())
					g.Expect(*ac.RetryCount).To(gomega.Equal(int32(2)))
					g.Expect(ac.RequeueAfterSeconds).ToNot(gomega.BeNil())

					g.Expect(createdWorkload.Status.RequeueState).ToNot(gomega.BeNil())
					g.Expect(createdWorkload.Status.RequeueState.RequeueAt).ToNot(gomega.BeNil())
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Wait for job to have quota again", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					g.Expect(workload.HasQuotaReservation(createdWorkload)).To(gomega.BeTrue())
					g.Expect(workload.IsAdmitted(createdWorkload)).To(gomega.BeFalse())
				}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Marking AC as Ready", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
					wlPatch := workload.BaseSSAWorkload(createdWorkload, true)
					workload.SetAdmissionCheckState(&wlPatch.Status.AdmissionChecks, kueue.AdmissionCheckState{
						Name:  kueue.AdmissionCheckReference(delayedCheck.Name),
						State: kueue.CheckStateReady,
					}, realClock)
					g.Expect(k8sClient.Status().Patch(ctx, wlPatch, client.Apply, client.FieldOwner(kueue.MultiKueueControllerName), client.ForceOwnership)).Should(gomega.Succeed())
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})

			ginkgo.By("Verifying workload is admitted with retry counter", func() {
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())

					ac := admissioncheck.FindAdmissionCheck(createdWorkload.Status.AdmissionChecks, kueue.AdmissionCheckReference(delayedCheck.Name))
					g.Expect(ac.State).To(gomega.Equal(kueue.CheckStateReady))
					g.Expect(ac.LastTransitionTime).To(gomega.Not(gomega.BeNil()))
					g.Expect(ac.RetryCount).ToNot(gomega.BeNil())
					g.Expect(*ac.RetryCount).To(gomega.Equal(int32(2)))

					g.Expect(workload.HasQuotaReservation(createdWorkload)).To(gomega.BeTrue())
					g.Expect(workload.IsAdmitted(createdWorkload)).To(gomega.BeTrue())
				}, util.Timeout, util.Interval).Should(gomega.Succeed())
			})
		})
	})
})
