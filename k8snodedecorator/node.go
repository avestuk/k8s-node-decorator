package k8snodedecorator

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func GetCurrentNode(clientset *kubernetes.Clientset, nodeName string) (*corev1.Node, error) {
	return clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
}

// GetNodeID attempts to get the LinodeID from the Kubernetes node.Spec.ProviderID
func GetNodeID(clientset *kubernetes.Clientset, nodeName string) (int, error) {
	klog.Infof("getting node: %s", nodeName)
	node, err := GetCurrentNode(clientset, nodeName)
	if err != nil {
		return 0, fmt.Errorf("failed to get current Kubernetes node, cannot get Provider ID and therefore cannot proceed, got err: %w", err)
	}

	if node.Spec.ProviderID == "" {
		return 0, fmt.Errorf("kubernetes node ProviderID is empty and therefore cannot proceed")
	}

	linodeID, err := parseProviderID(node.Spec.ProviderID)
	if err != nil {
		return 0, err
	}

	return linodeID, nil
}

// TODO(av): Get consensus on whether to export the parseProviderID fn
// Taken from https://github.com/linode/linode-cloud-controller-manager/blob/main/cloud/linode/common.go
const providerIDPrefix = "linode://"

type invalidProviderIDError struct {
	value string
}

func (e invalidProviderIDError) Error() string {
	return fmt.Sprintf("invalid provider ID %q", e.value)
}

func isLinodeProviderID(providerID string) bool {
	return strings.HasPrefix(providerID, providerIDPrefix)
}

func parseProviderID(providerID string) (int, error) {
	if !isLinodeProviderID(providerID) {
		return 0, invalidProviderIDError{providerID}
	}
	id, err := strconv.Atoi(strings.TrimPrefix(providerID, providerIDPrefix))
	if err != nil {
		return 0, invalidProviderIDError{providerID}
	}
	return id, nil
}
