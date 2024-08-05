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
func RemoveTaintInBackground(k8sClient kubernetes.Interface, nodeName, driverName string, backoff wait.Backoff) {
	backoffErr := wait.ExponentialBackoff(backoff, func() (bool, error) {
		err := removeNotReadyTaint(k8sClient, nodeName, driverName)
		if err != nil {
			klog.ErrorS(err, "Unexpected failure when attempting to remove node taint(s)")
			return false, nil
		}
		return true, nil
	})

	if backoffErr != nil {
		klog.ErrorS(backoffErr, "Retries exhausted, giving up attempting to remove node taint(s)")
	}
}

// removeNotReadyTaint removes the taint driverName/agent-not-ready from the local node
// This taint can be optionally applied by users to prevent startup race conditions such as
// https://github.com/kubernetes/kubernetes/issues/95911
func removeNotReadyTaint(clientset kubernetes.Interface, nodeName, driverName string) error {
	ctx := context.Background()
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if err := checkAllocatable(ctx, clientset, nodeName, driverName); err != nil {
		return err
	}

	taintKeyToRemove := driverName + AgentNotReadyNodeTaintKeySuffix
	klog.V(2).Infof("removing taint with key %s from local node %s", taintKeyToRemove, nodeName)
	var taintsToKeep []corev1.Taint
	for _, taint := range node.Spec.Taints {
		klog.V(5).Infof("checking taint key %s, value %s, effect %s", taint.Key, taint.Value, taint.Effect)
		if taint.Key != taintKeyToRemove {
			taintsToKeep = append(taintsToKeep, taint)
		} else {
			klog.V(2).Infof("queued taint for removal with key %s, effect %s", taint.Key, taint.Effect)
		}
	}

	if len(taintsToKeep) == len(node.Spec.Taints) {
		klog.V(2).Infof("No taints to remove on node, skipping taint removal")
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
	klog.V(2).Infof("removed taint with key %s from local node %s successfully", taintKeyToRemove, nodeName)
	return nil
}

func checkAllocatable(ctx context.Context, clientset kubernetes.Interface, nodeName, driverName string) error {
	csiNode, err := clientset.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("isAllocatableSet: failed to get CSINode for %s: %w", nodeName, err)
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == driverName {
			if driver.Allocatable != nil && driver.Allocatable.Count != nil {
				klog.V(2).Infof("CSINode Allocatable value is set for driver on node %s, count %d", nodeName, *driver.Allocatable.Count)
				return nil
			}
			return fmt.Errorf("isAllocatableSet: allocatable value not set for driver on node %s", nodeName)
		}
	}

	return fmt.Errorf("isAllocatableSet: driver not found on node %s", nodeName)
}
