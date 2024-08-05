package taint

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
	"time"
)

func TestRemoveTaint(t *testing.T) {
	client := fake.NewSimpleClientset()

	driverName := "fake.csi.driver.io"
	nodeName := "node"
	initialTaints := []v1.Taint{{Key: nodeName}}
	startupTaint := v1.Taint{
		Key: fmt.Sprintf("%s%s", driverName, AgentNotReadyNodeTaintKeySuffix),
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Spec: v1.NodeSpec{
			Taints: append(initialTaints, startupTaint),
		},
	}
	taintRemovalBackoff := wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   2,
		Steps:    10, // Max delay = 0.5 * 2^9 = ~4 minutes
	}

	_, err := client.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
	require.NoError(t, err)
	count := int32(1)
	_, err = client.StorageV1().CSINodes().Create(context.Background(), &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{
				{
					Name:        driverName,
					Allocatable: &storagev1.VolumeNodeResources{Count: &count},
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	w, err := client.CoreV1().Nodes().Watch(context.TODO(), metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", nodeName).String(),
	})
	require.NoError(t, err)

	RemoveTaintInBackground(context.TODO(), client, nodeName, driverName, taintRemovalBackoff)

	for event := range w.ResultChan() {
		n, ok := event.Object.(*v1.Node)
		if !ok {
			t.Fatalf("unexpected type")
		}
		if event.Type == watch.Modified {
			assert.Equal(t, initialTaints, n.Spec.Taints)
			break
		}
	}
}
