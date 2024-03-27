package k8snodedecorator

import (
	"context"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func StartDecorator(watcher Watcher, clientset *kubernetes.Clientset, nodeName string) {
	klog.LogToStderr(true)

	instanceData, err := watcher.getInstance(context.TODO())
	if err != nil {
		klog.Fatalf("Failed to get the initial instance data: %s", err.Error())
	}

	if err = UpdateNodeLabels(clientset, nodeName, instanceData); err != nil {
		klog.Error(err)
	}

	updates, errs := watcher.watch()

	for {
		select {
		case data := <-updates:
			if err = UpdateNodeLabels(clientset, nodeName, data); err != nil {
				klog.Error(err)
			}
		case err := <-errs:
			// TODO
			klog.Error(err)
		}
	}
}
