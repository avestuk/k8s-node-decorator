// Copyright 2024 Akamai Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	metadata "github.com/linode/go-metadata"
	decorator "github.com/linode/k8s-node-decorator/k8snodedecorator"
	"github.com/linode/linode-cloud-controller-manager/pkg/linodeid"
	"github.com/linode/linodego"
)

const k8sNodeDecorator = "k8s-node-decorator"

var (
	version  string
	nodeName string
)

func init() {
	_ = flag.Set("logtostderr", "true")
}

func GetCurrentNode(clientset *kubernetes.Clientset) (*corev1.Node, error) {
	return clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
}

func SetLabel(node *corev1.Node, key, newValue string) (changed bool) {
	changed = false
	oldValue, ok := node.Labels[key]

	if !ok || oldValue != newValue {
		changed = true
		node.Labels[key] = newValue
	}

	return changed
}

// instanceWatcherData holds all the common fields that different metadata
// clients can set as labels on a node
type instanceWatcherData struct {
	label        string
	id           string
	region       string
	instanceType string
	hostUUID     string
	tags         []string
}

func UpdateNodeLabels(
	clientset *kubernetes.Clientset,
	instanceData *instanceWatcherData,
) error {
	if instanceData == nil {
		return fmt.Errorf("instance data received from Linode metadata service is nil")
	}

	node, err := GetCurrentNode(clientset)
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

	oldTags := make(map[string]string, len(node.Labels))
	maps.Copy(oldTags, node.Labels)

	newTags := decorator.ParseTags(instanceData.tags)

	for key := range oldTags {
		if !strings.HasPrefix(key, decorator.TagLabelPrefix) {
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

	_, err = clientset.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{
		FieldManager: k8sNodeDecorator,
	})

	if err != nil {
		klog.Errorf("Failed to update labels: %s", err.Error())
		return err
	}

	klog.Infof("Successfully updated the labels of the node")

	return nil
}

func GetClientset() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return clientset, nil
}

// watcher is the interface that various metadata clients must implement
type watcher interface {
	GetInstance(context.Context) (*instanceWatcherData, error)
	Watch() (<-chan *instanceWatcherData, <-chan error)
}

func StartDecorator(instanceWatcher watcher, clientset *kubernetes.Clientset) {
	instanceData, err := instanceWatcher.GetInstance(context.TODO())
	if err != nil {
		klog.Fatalf("Failed to get the initial instance data: %s", err.Error())
	}

	err = UpdateNodeLabels(clientset, instanceData)
	if err != nil {
		klog.Error(err)
	}

	updates, errors := instanceWatcher.Watch()

	for {
		select {
		case data := <-updates:
			err = UpdateNodeLabels(clientset, data)
			if err != nil {
				klog.Fatal(err)
			}
		case err := <-errors:
			klog.Errorf("Got error from instance watcher: %s", err)
		}
	}
}

type metadataWatcher struct {
	client   metadata.Client
	interval time.Duration
	Updates  chan *instanceWatcherData
}

func (mw *metadataWatcher) GetInstance(ctx context.Context) (*instanceWatcherData, error) {
	data, err := mw.client.GetInstance(ctx)
	if err != nil {
		return nil, err
	}

	return &instanceWatcherData{
		label:        data.Label,
		id:           strconv.Itoa(data.ID),
		region:       data.Region,
		instanceType: data.Type,
		hostUUID:     data.HostUUID,
		tags:         data.Tags,
	}, nil
}

func (mw *metadataWatcher) Watch() (<-chan *instanceWatcherData, <-chan error) {
	instanceWatcher := mw.client.NewInstanceWatcher(
		metadata.WatcherWithInterval(mw.interval),
	)

	go instanceWatcher.Start(context.TODO())

	go func() {
		for {
			data := <-instanceWatcher.Updates
			mw.Updates <- &instanceWatcherData{
				label:        data.Label,
				id:           strconv.Itoa(data.ID),
				region:       data.Region,
				instanceType: data.Type,
				hostUUID:     data.HostUUID,
				tags:         data.Tags,
			}
		}
	}()

	return mw.Updates, instanceWatcher.Errors
}

type restWatcher struct {
	client   linodego.Client
	interval time.Duration
	linodeID int
	Updates  chan *instanceWatcherData
	Errors   chan error
}

func (rw *restWatcher) GetInstance(context.Context) (*instanceWatcherData, error) {
	data, err := rw.client.GetInstance(context.TODO(), rw.linodeID)
	if err != nil {
		return nil, err
	}

	return &instanceWatcherData{
		label:        data.Label,
		id:           strconv.Itoa(data.ID),
		region:       data.Region,
		instanceType: data.Type,
		hostUUID:     data.HostUUID,
		tags:         data.Tags,
	}, nil
}

func (rw *restWatcher) Watch() (<-chan *instanceWatcherData, <-chan error) {
	go func() {
		ticker := time.NewTicker(rw.interval)

		for {
			<-ticker.C
			data, err := rw.GetInstance(context.TODO())
			if err != nil {
				rw.Errors <- err
			}

			rw.Updates <- data

		}
	}()

	return rw.Updates, rw.Errors
}

// getNodeID attempts to get the LinodeID from the Kubernetes node.Spec.ProviderID
func getNodeID(clientset *kubernetes.Clientset) (int, error) {
	node, err := GetCurrentNode(clientset)
	if err != nil {
		return 0, fmt.Errorf("failed to get current Kubernetes node, cannot get Provider ID and therefore cannot proceed, got err: %s", err)
	}

	if node.Spec.ProviderID == "" {
		return 0, fmt.Errorf("kubernetes node ProviderID is empty and therefore cannot proceed")
	}

	linodeID, err := linodeid.ParseProviderID(node.Spec.ProviderID)
	if err != nil {
		return 0, err
	}

	return linodeID, nil
}

func main() {
	var (
		interval   time.Duration
		useRESTAPI bool
	)
	flag.DurationVar(
		&interval, "poll-interval", 5*time.Minute,
		"The time interval to poll and update node information",
	)
	flag.BoolVar(
		&useRESTAPI,
		"use-rest",
		false,
		"Whether to use the Linode REST API instead of the metadata service",
	)
	flag.Parse()

	nodeName = os.Getenv("NODE_NAME")
	if nodeName == "" {
		klog.Fatal("Environment variable NODE_NAME is not set")
	}

	klog.Infof("Starting Linode Kubernetes Node Decorator: version %s", version)
	klog.Infof("The poll interval is set to %v.", interval)

	clientset, err := GetClientset()
	if err != nil {
		klog.Fatal(err)
	}

	_, err = GetCurrentNode(clientset)
	if err != nil {
		klog.Fatal(err)
	}

	var metadataClient watcher
	if !useRESTAPI {
		klog.Info("using metadata service")
		client, err := metadata.NewClient(
			context.Background(),
			metadata.ClientWithManagedToken(),
		)
		if err != nil {
			klog.Fatal(err)
		}
		metadataClient = &metadataWatcher{
			client:   *client,
			interval: interval,
			Updates:  make(chan *instanceWatcherData),
		}

	} else {
		klog.Info("using rest-api")
		client, err := linodego.NewClientFromEnv(nil)
		if err != nil {
			klog.Fatal(err)
		}

		linodeID, err := getNodeID(clientset)
		if err != nil {
			klog.Fatal(err)
		}

		metadataClient = &restWatcher{
			client:   *client,
			interval: interval,
			linodeID: linodeID,
			Updates:  make(chan *instanceWatcherData),
			Errors:   make(chan error),
		}
	}

	StartDecorator(metadataClient, clientset)
}
