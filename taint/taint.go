package taint

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	taintutils "k8s.io/kubernetes/pkg/util/taints"
)

const AgentNotReadyNodeTaintKeySuffix = "/agent-not-ready"

// RemoveTaintInBackground is a goroutine that retries removeNotReadyTaint with exponential backoff
func RemoveTaintInBackground(ctx context.Context, k8sClient kubernetes.Interface, nodeName, driverName string, backoff wait.Backoff) {
	logger := klog.FromContext(ctx)
	backoffErr := wait.ExponentialBackoff(backoff, func() (bool, error) {
		err := removeNotReadyTaint(ctx, k8sClient, nodeName, driverName)
		if err != nil {
			logger.Error(err, "Unexpected failure when attempting to remove node taint(s)")
			return false, nil
		}
		return true, nil
	})

	if backoffErr != nil {
		logger.Error(backoffErr, "Retries exhausted, giving up attempting to remove node taint(s)")
	}
}

// removeNotReadyTaint removes the taint driverName/agent-not-ready from the local node
// This taint can be optionally applied by users to prevent startup race conditions such as
// https://github.com/kubernetes/kubernetes/issues/95911
func removeNotReadyTaint(ctx context.Context, clientset kubernetes.Interface, nodeName, driverName string) error {
	logger := klog.FromContext(ctx)
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if err := checkAllocatable(ctx, clientset, nodeName, driverName); err != nil {
		return err
	}

	taintKeyToRemove := driverName + AgentNotReadyNodeTaintKeySuffix

	logger.V(2).Info("removing taint", "key", taintKeyToRemove, "node", nodeName)

	// We cannot use controller.RemoveTaintOffNode as it matches against effect as well
	newTaints, _ := taintutils.DeleteTaintsByKey(node.Spec.Taints, taintKeyToRemove)
	newNode := node.DeepCopy()
	newNode.Spec.Taints = newTaints
	err = controller.PatchNodeTaints(ctx, clientset, nodeName, node, newNode)

	if err != nil {
		return err
	}
	logger.V(2).Info("removed taint successfully", "key", taintKeyToRemove, "node", nodeName)
	return nil
}

func checkAllocatable(ctx context.Context, clientset kubernetes.Interface, nodeName, driverName string) error {
	logger := klog.FromContext(ctx)
	csiNode, err := clientset.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("isAllocatableSet: failed to get CSINode for %s: %w", nodeName, err)
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == driverName {
			if driver.Allocatable != nil && driver.Allocatable.Count != nil {
				logger.V(2).Info("CSINode Allocatable value is set for driver", "node", nodeName, "count", *driver.Allocatable.Count)
				return nil
			}
			return fmt.Errorf("isAllocatableSet: allocatable value not set for driver on node %s", nodeName)
		}
	}

	return fmt.Errorf("isAllocatableSet: driver not found on node %s", nodeName)
}
