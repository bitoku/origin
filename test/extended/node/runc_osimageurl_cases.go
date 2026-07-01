package node

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	g "github.com/onsi/ginkgo/v2"
	o "github.com/onsi/gomega"
	ote "github.com/openshift-eng/openshift-tests-extension/pkg/ginkgo"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"

	configv1 "github.com/openshift/api/config/v1"
	machineconfigv1 "github.com/openshift/api/machineconfiguration/v1"
	machineconfigclient "github.com/openshift/client-go/machineconfiguration/clientset/versioned"
	exutil "github.com/openshift/origin/test/extended/util"
)

const (
	osImageURLGuardPool = "runc-osimageurl-guard"

	osImageURLGuardCRCName          = "99-runc-osimageurl-guard-runc"
	osImageURLRHEL9MCName           = "99-runc-osimageurl-guard-rhel9"
	osImageURLCRCDefaultRuntimePath = "/etc/crio/crio.conf.d/01-ctrcfg-defaultRuntime"

	osImageURLMachineConfigCO = "machine-config"

	osImageURLDegradedPoolReason  = "DegradedPool"
	osImageURLDegradedPoolMessage = "One or more machine config pools are degraded"
)

var osImageURLRHELMajorFromOSImage = regexp.MustCompile(`Linux\s+([0-9]+)`)

// When a pool uses runc and the OSImageURL points to a RHEL 10 image (detected
// via stream class inspection), MCO must block the rollout by setting
// MachineConfigPool RenderDegraded. MCO then sets ClusterOperator
// Upgradeable=False (DegradedPool), which CVO aggregates on ClusterVersion.
//
// This test exercises the OSImageURL code path (validateNoRuncFromOSImageURL)
// rather than the OSImageStream path tested by the sibling test case.
var _ = g.Describe("[Suite:openshift/disruptive-longrunning][sig-node][Serial][Disruptive] runc RHCOS 10 upgrade guard", func() {
	defer g.GinkgoRecover()

	var (
		oc       = exutil.NewCLI("runc-osimageurl-guard")
		mcClient *machineconfigclient.Clientset
		nodeName string
	)

	g.BeforeEach(func(ctx context.Context) {
		var err error
		mcClient, err = machineconfigclient.NewForConfig(oc.AdminConfig())
		o.Expect(err).NotTo(o.HaveOccurred())

		isMicroShift, err := exutil.IsMicroShiftCluster(oc.AdminKubeClient())
		o.Expect(err).NotTo(o.HaveOccurred())
		if isMicroShift {
			g.Skip("Skipping on MicroShift cluster: runc cannot be configured")
		}

		controlPlaneTopology, err := exutil.GetControlPlaneTopology(oc)
		o.Expect(err).NotTo(o.HaveOccurred())
		if *controlPlaneTopology == configv1.ExternalTopologyMode {
			g.Skip("Skipping on external control plane (Hypershift) cluster")
		}
		if *controlPlaneTopology == configv1.SingleReplicaTopologyMode {
			g.Skip("Skipping on single-replica topology: requires a pure worker node")
		}

		requireClusterOnRHEL10(ctx, oc)
	})

	g.It("blocks runc when RHEL 10 is detected via OSImageURL stream class inspection", ote.Informing(), func(ctx context.Context) {
		g.By("Labeling one worker into the custom pool")
		var err error
		nodeName, err = osImageURLLabelFirstPureWorker(ctx, oc, osImageURLGuardPool)
		o.Expect(err).NotTo(o.HaveOccurred(), "need a worker node for the custom pool")

		g.By("Creating custom MachineConfigPool without osImageStream")
		o.Expect(createOSImageURLGuardMCP(ctx, mcClient)).To(o.Succeed())

		g.By("Getting RHEL 9 OS image URL from OSImageStream")
		rhel9ImageURL, rhel9Found, err := getRHEL9OSImageURL(ctx, mcClient)
		o.Expect(err).NotTo(o.HaveOccurred())
		if !rhel9Found {
			framework.Logf("No RHEL 9 stream in OSImageStream; skipping RHEL 9 positive-control step")
		}

		if rhel9Found {
			g.By("Positive control: creating MachineConfig with RHEL 9 OSImageURL for the custom pool")
			o.Expect(createOSImageURLRHEL9MC(ctx, mcClient, osImageURLGuardPool, rhel9ImageURL)).To(o.Succeed())

			g.By("Creating ContainerRuntimeConfig for runc (should be allowed on RHEL 9)")
			o.Expect(createOSImageURLGuardCRC(ctx, mcClient)).To(o.Succeed())

			g.By("Waiting for pool to render with runc on RHEL 9 (no guard expected)")
			o.Expect(waitForMCP(ctx, mcClient, osImageURLGuardPool, 30*time.Minute, WaitMCPWithMachineCount(1), WaitMCPAllowDegraded())).To(o.Succeed(),
				"pool did not stabilize with runc + RHEL 9 OSImageURL")

			g.By("Verifying pool is NOT render-degraded (runc allowed on RHEL 9)")
			o.Expect(osImageURLVerifyNotRenderDegraded(ctx, mcClient, osImageURLGuardPool)).To(o.Succeed())

			g.By("Removing RHEL 9 OSImageURL override to expose RHEL 10 default")
			o.Expect(deleteOSImageURLRHEL9MC(ctx, mcClient)).To(o.Succeed())

			g.By("Waiting for render guard to fire after falling back to RHEL 10 OSImageURL")
			o.Expect(osImageURLWaitForMCPRenderDegraded(ctx, mcClient, osImageURLGuardPool, 10*time.Minute)).To(o.Succeed())
		} else {
			g.By("Waiting for pool baseline rollout on RHEL 10 with crun")
			o.Expect(waitForMCP(ctx, mcClient, osImageURLGuardPool, 30*time.Minute, WaitMCPWithMachineCount(1))).To(o.Succeed(),
				"node did not join custom MCP; ensure cluster is on RHEL 10 and MCO supports OSImageURL stream class inspection (PR 6238)")

			g.By("Verifying baseline: node is on RHEL 10 with crun")
			var rhelMajor string
			rhelMajor, err = osImageURLNodeRHELMajorVersion(ctx, oc, nodeName)
			o.Expect(err).NotTo(o.HaveOccurred())
			o.Expect(rhelMajor).To(o.Equal("10"), "pool should be on RHCOS 10 for the OSImageURL guard test")
			crun, crunErr := osImageURLUsesCrunRuntime(oc, nodeName)
			o.Expect(crunErr).NotTo(o.HaveOccurred())
			o.Expect(crun).To(o.BeTrue(), "node should use crun as default runtime before adding runc config")

			g.By("Creating ContainerRuntimeConfig that sets default runtime to runc for the custom pool")
			o.Expect(createOSImageURLGuardCRC(ctx, mcClient)).To(o.Succeed())

			g.By("Waiting for render guard to fire via OSImageURL stream class inspection")
			o.Expect(osImageURLWaitForMCPRenderDegraded(ctx, mcClient, osImageURLGuardPool, 10*time.Minute)).To(o.Succeed())
		}

		g.By("Verifying cluster upgrade is blocked via CO and CVO Upgradeable=False")
		o.Expect(osImageURLWaitForUpgradeBlocked(ctx, oc)).To(o.Succeed())

		g.By("Verifying node remains ready, not rolling out, on RHEL 10 with crun after guard blocks")
		o.Expect(osImageURLVerifyNodeReadyAndNotRollingOut(ctx, oc, nodeName)).To(o.Succeed())
		rhelMajor, err := osImageURLNodeRHELMajorVersion(ctx, oc, nodeName)
		o.Expect(err).NotTo(o.HaveOccurred())
		o.Expect(rhelMajor).To(o.Equal("10"), "node should remain on RHCOS 10 after guard blocks rollout")
		crun, crunErr := osImageURLUsesCrunRuntime(oc, nodeName)
		o.Expect(crunErr).NotTo(o.HaveOccurred())
		o.Expect(crun).To(o.BeTrue(), "node should keep crun as default runtime after guard blocks rollout")

		g.By("Recovering pool by deleting ContainerRuntimeConfig")
		o.Expect(osImageURLDeleteContainerRuntimeConfig(ctx, mcClient, osImageURLGuardCRCName)).To(o.Succeed())
		o.Expect(waitForMCP(ctx, mcClient, osImageURLGuardPool, 30*time.Minute, WaitMCPAllowDegraded())).To(o.Succeed())

		g.By("Verifying node remains ready on RHEL 10 with crun after recovery")
		o.Expect(osImageURLVerifyNodeReadyAndNotRollingOut(ctx, oc, nodeName)).To(o.Succeed())
		rhelMajor, err = osImageURLNodeRHELMajorVersion(ctx, oc, nodeName)
		o.Expect(err).NotTo(o.HaveOccurred())
		o.Expect(rhelMajor).To(o.Equal("10"), "node should remain on RHCOS 10 after recovery")
		crun, crunErr = osImageURLUsesCrunRuntime(oc, nodeName)
		o.Expect(crunErr).NotTo(o.HaveOccurred())
		o.Expect(crun).To(o.BeTrue(), "node should keep crun as default runtime after recovery")
	})

	g.AfterEach(func(ctx context.Context) {
		if nodeName != "" {
			roleLabel := osImageURLPoolNodeRoleLabel(osImageURLGuardPool)
			if err := osImageURLRemoveNodeLabel(ctx, oc, nodeName, roleLabel); err != nil {
				framework.Logf("cleanup: failed to remove node label %s from %s: %v", roleLabel, nodeName, err)
			}
		}
		if err := deleteOSImageURLRHEL9MC(ctx, mcClient); err != nil {
			framework.Logf("cleanup: failed to delete RHEL 9 MachineConfig %s: %v", osImageURLRHEL9MCName, err)
		}
		if err := osImageURLDeleteContainerRuntimeConfig(ctx, mcClient, osImageURLGuardCRCName); err != nil {
			framework.Logf("cleanup: failed to delete ContainerRuntimeConfig %s: %v", osImageURLGuardCRCName, err)
		}
		if nodeName != "" {
			if err := waitForMCP(ctx, mcClient, osImageURLGuardPool, 10*time.Minute, WaitMCPWithMachineCount(0), WaitMCPAllowDegraded()); err != nil {
				framework.Logf("cleanup: failed waiting for MCP %s machine count 0: %v", osImageURLGuardPool, err)
			}
			if err := osImageURLWaitForNodeWorkerConfigRollback(ctx, oc, nodeName, osImageURLGuardPool, 15*time.Minute); err != nil {
				framework.Logf("cleanup: failed waiting for node %s worker config rollback: %v", nodeName, err)
			}
		}
		if err := osImageURLDeleteMachineConfigPool(ctx, mcClient, osImageURLGuardPool); err != nil {
			framework.Logf("cleanup: failed to delete MachineConfigPool %s: %v", osImageURLGuardPool, err)
		}
		if nodeName != "" {
			if err := waitForMCP(ctx, mcClient, "worker", 30*time.Minute); err != nil {
				framework.Logf("cleanup: failed waiting for worker MCP to become ready: %v", err)
			}
		}
	})
})

func requireClusterOnRHEL10(ctx context.Context, oc *exutil.CLI) {
	workers, err := getPureWorkerNodesFromCluster(ctx, oc)
	o.Expect(err).NotTo(o.HaveOccurred(), "need pure worker nodes to check RHEL version")
	o.Expect(workers).NotTo(o.BeEmpty(), "need at least one pure worker node")

	major, err := osImageURLNodeRHELMajorVersion(ctx, oc, workers[0].Name)
	if err != nil || major != "10" {
		g.Skip(fmt.Sprintf("Skipping: cluster is not on RHEL 10 (worker %s OSImage major=%q err=%v); OSImageURL guard only fires on RHEL 10", workers[0].Name, major, err))
	}
	framework.Logf("Cluster is on RHEL 10 (worker %s), OSImageURL guard test can proceed", workers[0].Name)
}

func osImageURLPoolNodeRoleLabel(poolName string) string {
	return fmt.Sprintf("node-role.kubernetes.io/%s", poolName)
}

func createOSImageURLGuardMCP(ctx context.Context, mcClient *machineconfigclient.Clientset) error {
	mcp := &machineconfigv1.MachineConfigPool{
		ObjectMeta: metav1.ObjectMeta{
			Name: osImageURLGuardPool,
			Labels: map[string]string{
				fmt.Sprintf("pools.operator.machineconfiguration.openshift.io/%s", osImageURLGuardPool): "",
			},
		},
		Spec: machineconfigv1.MachineConfigPoolSpec{
			MachineConfigSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      machineconfigv1.MachineConfigRoleLabelKey,
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{"worker", osImageURLGuardPool},
				}},
			},
			NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					osImageURLPoolNodeRoleLabel(osImageURLGuardPool): "",
				},
			},
		},
	}
	_, err := mcClient.MachineconfigurationV1().MachineConfigPools().Create(ctx, mcp, metav1.CreateOptions{})
	return err
}

func createOSImageURLGuardCRC(ctx context.Context, mcClient *machineconfigclient.Clientset) error {
	crc := &machineconfigv1.ContainerRuntimeConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: osImageURLGuardCRCName,
		},
		Spec: machineconfigv1.ContainerRuntimeConfigSpec{
			MachineConfigPoolSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					fmt.Sprintf("pools.operator.machineconfiguration.openshift.io/%s", osImageURLGuardPool): "",
				},
			},
			ContainerRuntimeConfig: &machineconfigv1.ContainerRuntimeConfiguration{
				DefaultRuntime: machineconfigv1.ContainerRuntimeDefaultRuntimeRunc,
			},
		},
	}
	_, err := mcClient.MachineconfigurationV1().ContainerRuntimeConfigs().Create(ctx, crc, metav1.CreateOptions{})
	return err
}

func osImageURLLabelFirstPureWorker(ctx context.Context, oc *exutil.CLI, poolName string) (string, error) {
	workers, err := getPureWorkerNodesFromCluster(ctx, oc)
	if err != nil {
		return "", err
	}

	var node *corev1.Node
	for i := range workers {
		for _, c := range workers[i].Status.Conditions {
			if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
				node = &workers[i]
				break
			}
		}
		if node != nil {
			break
		}
	}
	if node == nil {
		return "", fmt.Errorf("no Ready pure worker node found")
	}
	label := osImageURLPoolNodeRoleLabel(poolName)
	patchData := []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:""}}}`, label))
	_, err = oc.AdminKubeClient().CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return "", err
	}
	framework.Logf("Labeled node %s with %s", node.Name, label)
	return node.Name, nil
}

func osImageURLRemoveNodeLabel(ctx context.Context, oc *exutil.CLI, nodeName, label string) error {
	patchData := []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:null}}}`, label))
	_, err := oc.AdminKubeClient().CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patchData, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func osImageURLWaitForMCPRenderDegraded(ctx context.Context, mcClient *machineconfigclient.Clientset, poolName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 10*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		mcp, err := mcClient.MachineconfigurationV1().MachineConfigPools().Get(ctx, poolName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		renderDegraded := false
		var renderMessage string
		for _, c := range mcp.Status.Conditions {
			if c.Type == machineconfigv1.MachineConfigPoolRenderDegraded && c.Status == corev1.ConditionTrue {
				renderDegraded = true
				renderMessage = c.Message
			}
		}

		if renderDegraded &&
			strings.Contains(renderMessage, "runc") &&
			strings.Contains(renderMessage, "stream class") {
			framework.Logf("MCP %s render degraded as expected via OSImageURL inspection: %s", poolName, renderMessage)
			return true, nil
		}

		framework.Logf("MCP %s waiting for runc+stream-class guard: renderDegraded=%v message=%q",
			poolName, renderDegraded, renderMessage)
		return false, nil
	})
}

func osImageURLWaitForUpgradeBlocked(ctx context.Context, oc *exutil.CLI) error {
	return wait.PollUntilContextTimeout(ctx, 10*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		co, err := oc.AdminConfigClient().ConfigV1().ClusterOperators().Get(ctx, osImageURLMachineConfigCO, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if degraded := osImageURLConditionStatus(co.Status.Conditions, configv1.OperatorDegraded); degraded == configv1.ConditionTrue {
			return false, fmt.Errorf("ClusterOperator %s Degraded=True; expected Upgradeable=False only while isolated pool guard is active", osImageURLMachineConfigCO)
		}
		if available := osImageURLConditionStatus(co.Status.Conditions, configv1.OperatorAvailable); available != configv1.ConditionTrue {
			return false, fmt.Errorf("ClusterOperator %s Available=%s, expected True", osImageURLMachineConfigCO, available)
		}

		upgradeable := osImageURLFindCondition(co.Status.Conditions, configv1.OperatorUpgradeable)
		if upgradeable == nil || upgradeable.Status != configv1.ConditionFalse {
			status := configv1.ConditionUnknown
			if upgradeable != nil {
				status = upgradeable.Status
			}
			framework.Logf("waiting for ClusterOperator %s Upgradeable=False, current status=%s", osImageURLMachineConfigCO, status)
			return false, nil
		}
		if upgradeable.Reason != osImageURLDegradedPoolReason {
			framework.Logf("waiting for ClusterOperator %s Upgradeable reason=%s, current reason=%q", osImageURLMachineConfigCO, osImageURLDegradedPoolReason, upgradeable.Reason)
			return false, nil
		}
		if !strings.Contains(upgradeable.Message, osImageURLDegradedPoolMessage) {
			framework.Logf("waiting for ClusterOperator %s Upgradeable message to contain %q, current message=%q", osImageURLMachineConfigCO, osImageURLDegradedPoolMessage, upgradeable.Message)
			return false, nil
		}

		cv, err := oc.AdminConfigClient().ConfigV1().ClusterVersions().Get(ctx, "version", metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if available := osImageURLConditionStatus(cv.Status.Conditions, configv1.OperatorAvailable); available != configv1.ConditionTrue {
			return false, fmt.Errorf("ClusterVersion Available=%s, expected True", available)
		}
		if progressing := osImageURLConditionStatus(cv.Status.Conditions, configv1.OperatorProgressing); progressing == configv1.ConditionTrue {
			return false, fmt.Errorf("ClusterVersion Progressing=True while isolated pool guard is active")
		}
		if degraded := osImageURLConditionStatus(cv.Status.Conditions, configv1.OperatorDegraded); degraded == configv1.ConditionTrue {
			return false, fmt.Errorf("ClusterVersion Degraded=True while isolated pool guard is active")
		}

		cvUpgradeable := osImageURLFindCondition(cv.Status.Conditions, configv1.OperatorUpgradeable)
		if cvUpgradeable == nil || cvUpgradeable.Status != configv1.ConditionFalse {
			status := configv1.ConditionUnknown
			if cvUpgradeable != nil {
				status = cvUpgradeable.Status
			}
			framework.Logf("waiting for ClusterVersion Upgradeable=False, current status=%s", status)
			return false, nil
		}

		if exutil.IsNoUpgradeFeatureSet(oc) || len(cv.Spec.Overrides) > 0 ||
			strings.Contains(cvUpgradeable.Message, "cluster version overrides") {
			framework.Logf("ClusterOperator %s reports Upgradeable=False (reason %s); ClusterVersion Upgradeable=False without machine-config in message (feature set, CV overrides, or stale override reason present)",
				osImageURLMachineConfigCO, osImageURLDegradedPoolReason)
			return true, nil
		}
		if !strings.Contains(cvUpgradeable.Message, osImageURLMachineConfigCO) {
			framework.Logf("waiting for ClusterVersion Upgradeable message to mention %s, current message=%q", osImageURLMachineConfigCO, cvUpgradeable.Message)
			return false, nil
		}

		framework.Logf("ClusterOperator %s and ClusterVersion report Upgradeable=False (reason %s) with isolated MCP guard active",
			osImageURLMachineConfigCO, osImageURLDegradedPoolReason)
		return true, nil
	})
}

func osImageURLWaitForNodeWorkerConfigRollback(ctx context.Context, oc *exutil.CLI, nodeName, poolName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 10*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		node, err := oc.AdminKubeClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		currentConfig := node.Annotations["machineconfiguration.openshift.io/currentConfig"]
		desiredConfig := node.Annotations["machineconfiguration.openshift.io/desiredConfig"]
		rolledBack := currentConfig != "" &&
			!strings.Contains(currentConfig, poolName) &&
			currentConfig == desiredConfig
		if !rolledBack {
			framework.Logf("Node %s waiting for worker rollback: current=%q desired=%q",
				nodeName, currentConfig, desiredConfig)
		}
		return rolledBack, nil
	})
}

func osImageURLVerifyNodeReadyAndNotRollingOut(ctx context.Context, oc *exutil.CLI, nodeName string) error {
	node, err := oc.AdminKubeClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	ready := false
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			ready = true
			break
		}
	}
	if !ready {
		return fmt.Errorf("node %s is not Ready", nodeName)
	}

	currentConfig := node.Annotations["machineconfiguration.openshift.io/currentConfig"]
	desiredConfig := node.Annotations["machineconfiguration.openshift.io/desiredConfig"]
	if currentConfig == "" || desiredConfig == "" {
		return fmt.Errorf("node %s missing MCO config annotations (current=%q desired=%q)", nodeName, currentConfig, desiredConfig)
	}
	if currentConfig != desiredConfig {
		return fmt.Errorf("node %s is rolling out MCO config (current=%q desired=%q)", nodeName, currentConfig, desiredConfig)
	}

	framework.Logf("Node %s is Ready and not rolling out MCO config (%s)", nodeName, currentConfig)
	return nil
}

func osImageURLFindCondition(conditions []configv1.ClusterOperatorStatusCondition, condType configv1.ClusterStatusConditionType) *configv1.ClusterOperatorStatusCondition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func osImageURLConditionStatus(conditions []configv1.ClusterOperatorStatusCondition, condType configv1.ClusterStatusConditionType) configv1.ConditionStatus {
	for _, c := range conditions {
		if c.Type == condType {
			return c.Status
		}
	}
	return configv1.ConditionUnknown
}

func osImageURLNodeRHELMajorVersion(ctx context.Context, oc *exutil.CLI, nodeName string) (string, error) {
	node, err := oc.AdminKubeClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	osImage := node.Status.NodeInfo.OSImage
	switch {
	case strings.Contains(osImage, "CoreOS 10."):
		return "10", nil
	case strings.Contains(osImage, "CoreOS 9."):
		return "9", nil
	}

	if matches := osImageURLRHELMajorFromOSImage.FindStringSubmatch(osImage); len(matches) >= 2 {
		return matches[1], nil
	}
	return "", fmt.Errorf("could not parse RHEL major version from OSImage %q on node %s", osImage, nodeName)
}

func osImageURLUsesCrunRuntime(oc *exutil.CLI, nodeName string) (bool, error) {
	out, err := ExecOnNodeWithChroot(oc, nodeName, "grep", "default_runtime", osImageURLCRCDefaultRuntimePath)
	if err != nil {
		return false, fmt.Errorf("failed to check default runtime on node %s: %w", nodeName, err)
	}
	return strings.Contains(out, "crun"), nil
}

func osImageURLDeleteContainerRuntimeConfig(ctx context.Context, mcClient *machineconfigclient.Clientset, name string) error {
	err := mcClient.MachineconfigurationV1().ContainerRuntimeConfigs().Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func osImageURLDeleteMachineConfigPool(ctx context.Context, mcClient *machineconfigclient.Clientset, name string) error {
	err := mcClient.MachineconfigurationV1().MachineConfigPools().Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func getRHEL9OSImageURL(ctx context.Context, mcClient *machineconfigclient.Clientset) (string, bool, error) {
	ois, err := mcClient.MachineconfigurationV1().OSImageStreams().Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	for _, stream := range ois.Status.AvailableStreams {
		if strings.Contains(stream.Name, "9") {
			return string(stream.OSImage), true, nil
		}
	}
	return "", false, nil
}

func createOSImageURLRHEL9MC(ctx context.Context, mcClient *machineconfigclient.Clientset, poolName, osImageURL string) error {
	mc := &machineconfigv1.MachineConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: osImageURLRHEL9MCName,
			Labels: map[string]string{
				machineconfigv1.MachineConfigRoleLabelKey: poolName,
			},
		},
		Spec: machineconfigv1.MachineConfigSpec{
			OSImageURL: osImageURL,
		},
	}
	_, err := mcClient.MachineconfigurationV1().MachineConfigs().Create(ctx, mc, metav1.CreateOptions{})
	return err
}

func deleteOSImageURLRHEL9MC(ctx context.Context, mcClient *machineconfigclient.Clientset) error {
	err := mcClient.MachineconfigurationV1().MachineConfigs().Delete(ctx, osImageURLRHEL9MCName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func osImageURLVerifyNotRenderDegraded(ctx context.Context, mcClient *machineconfigclient.Clientset, poolName string) error {
	mcp, err := mcClient.MachineconfigurationV1().MachineConfigPools().Get(ctx, poolName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	for _, c := range mcp.Status.Conditions {
		if c.Type == machineconfigv1.MachineConfigPoolRenderDegraded && c.Status == corev1.ConditionTrue {
			return fmt.Errorf("MachineConfigPool %s is unexpectedly RenderDegraded: %s", poolName, c.Message)
		}
	}
	return nil
}

