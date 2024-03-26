package k8snodedecorator

import (
	"context"
	"fmt"
	"maps"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func SetLabel(node *corev1.Node, key, newValue string) (changed bool) {
	changed = false
	oldValue, ok := node.Labels[key]

	if !ok || oldValue != newValue {
		changed = true
		node.Labels[key] = newValue
	}

	return changed
}

func UpdateNodeLabels(
	clientset *kubernetes.Clientset,
	nodeName string,
	instanceData *InstanceData,
) error {
	if instanceData == nil {
		return fmt.Errorf("instance data received from Linode metadata service is nil")
	}

	node, err := GetCurrentNode(clientset, nodeName)
	if err != nil {
		return fmt.Errorf("failed to get the node: %w", err)
	}

	klog.Infof("Updating node labels with Linode instance data: %v", instanceData)
	labelsUpdated := false

	handleUpdated := func(updated bool) {
		if updated {
			labelsUpdated = updated
		}
	}

	handleUpdated(SetLabel(node, "decorator.linode.com/label", instanceData.label))
	handleUpdated(SetLabel(node, "decorator.linode.com/instance-id", instanceData.id))
	handleUpdated(SetLabel(node, "decorator.linode.com/region", instanceData.region))
	handleUpdated(SetLabel(node, "decorator.linode.com/instance-type", instanceData.instanceType))
	handleUpdated(SetLabel(node, "decorator.linode.com/host", instanceData.hostUUID))

	oldTags := make(map[string]string)
	maps.Copy(oldTags, node.Labels)

	newTags := ParseTags(instanceData.tags)

	for key := range oldTags {
		if !strings.HasPrefix(key, TagLabelPrefix) {
			continue
		}
		if _, ok := newTags[key]; !ok {
			delete(node.Labels, key)
			labelsUpdated = true
			continue
		}
	}

	for key, value := range newTags {
		handleUpdated(SetLabel(node, key, value))
	}

	if !labelsUpdated {
		return nil
	}

	_, err = clientset.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update labels: %s", err.Error())
		return err
	}

	klog.Infof("Successfully updated the labels of the node")

	return nil
}
