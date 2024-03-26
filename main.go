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
	"os"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	metadata "github.com/linode/go-metadata"
	decorator "github.com/linode/k8s-node-decorator/k8snodedecorator"
	"github.com/linode/linodego"
)

var version string

func init() {
	_ = flag.Set("logtostderr", "true")
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

func main() {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		klog.Fatal("Environment variable NODE_NAME is not set")
	}

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

	klog.Infof("Starting Linode Kubernetes Node Decorator: version %s", version)
	klog.Infof("The poll interval is set to %v.", interval)

	clientset, err := GetClientset()
	if err != nil {
		klog.Fatal(err)
	}

	var watcher decorator.Watcher
	if !useRESTAPI {
		klog.Info("using metadata service")
		client, err := metadata.NewClient(
			context.Background(),
			metadata.ClientWithManagedToken(),
		)
		if err != nil {
			klog.Fatal(err)
		}
		watcher = &decorator.MetadataWatcher{
			Client:   *client,
			Interval: interval,
			Updates:  make(chan *decorator.InstanceData),
		}

	} else {
		klog.Info("using rest-api")
		client, err := linodego.NewClientFromEnv(nil)
		if err != nil {
			klog.Fatal(err)
		}

		linodeID, err := decorator.GetNodeID(clientset, nodeName)
		if err != nil {
			klog.Fatal(err)
		}

		watcher = &decorator.RestWatcher{
			Client:   *client,
			Interval: interval,
			LinodeID: linodeID,
			Updates:  make(chan *decorator.InstanceData),
			Errors:   make(chan error),
		}
	}
	decorator.StartDecorator(watcher, clientset, nodeName)
}
