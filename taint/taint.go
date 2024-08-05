package taint

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const AgentNotReadyNodeTaintKeySuffix = "/agent-not-ready"

// JSONPatch is a struct for JSON patch operations
type JSONPatch struct {
	OP    string      `json:"op,omitempty"`
	Path  string      `json:"path,omitempty"`
	Value interface{} `json:"value"`
}

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
	var taintsToKeep []corev1.Taint
	for _, taint := range node.Spec.Taints {
		logger.V(5).Info("checking taint", "key", taint.Key, "value", taint.Value, "effect", taint.Effect)
		if taint.Key != taintKeyToRemove {
			taintsToKeep = append(taintsToKeep, taint)
		} else {
			logger.V(2).Info("queued taint for removal", "key", taint.Key, "effect", taint.Effect)
		}
	}

	if len(taintsToKeep) == len(node.Spec.Taints) {
		logger.V(2).Info("No taints to remove on node, skipping taint removal")
		return nil
	}

	patchRemoveTaints := []JSONPatch{
		{
			OP:    "test",
			Path:  "/spec/taints",
			Value: node.Spec.Taints,
		},
		{
			OP:    "replace",
			Path:  "/spec/taints",
			Value: taintsToKeep,
		},
	}

	patch, err := json.Marshal(patchRemoveTaints)
	if err != nil {
		return err
	}

	_, err = clientset.CoreV1().Nodes().Patch(ctx, nodeName, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
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
