package k8snodedecorator

import (
	"context"
	"strconv"
	"time"

	"github.com/linode/go-metadata"
	"github.com/linode/linodego"
)

// InstanceData holds all the common fields that different metadata
// clients can set as labels on a node
type InstanceData struct {
	label        string
	id           string
	region       string
	instanceType string
	hostUUID     string
	tags         []string
}

// Watcher is the interface that various metadata clients must implement
type Watcher interface {
	GetInstance(context.Context) (*InstanceData, error)
	Watch() (<-chan *InstanceData, <-chan error)
}

type MetadataWatcher struct {
	Client   metadata.Client
	Interval time.Duration
	Updates  chan *InstanceData
}

func (mw *MetadataWatcher) GetInstance(ctx context.Context) (*InstanceData, error) {
	data, err := mw.Client.GetInstance(ctx)
	if err != nil {
		return nil, err
	}

	return &InstanceData{
		label:        data.Label,
		id:           strconv.Itoa(data.ID),
		region:       data.Region,
		instanceType: data.Type,
		hostUUID:     data.HostUUID,
		tags:         data.Tags,
	}, nil
}

func (mw *MetadataWatcher) Watch() (<-chan *InstanceData, <-chan error) {
	instanceWatcher := mw.Client.NewInstanceWatcher(
		metadata.WatcherWithInterval(mw.Interval),
	)

	go instanceWatcher.Start(context.TODO())

	go func() {
		for {
			data := <-instanceWatcher.Updates
			mw.Updates <- &InstanceData{
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

type RestWatcher struct {
	Client   linodego.Client
	Interval time.Duration
	LinodeID int
	Updates  chan *InstanceData
	Errors   chan error
}

func (rw *RestWatcher) GetInstance(context.Context) (*InstanceData, error) {
	data, err := rw.Client.GetInstance(context.TODO(), rw.LinodeID)
	if err != nil {
		return nil, err
	}

	return &InstanceData{
		label:        data.Label,
		id:           strconv.Itoa(data.ID),
		region:       data.Region,
		instanceType: data.Type,
		hostUUID:     data.HostUUID,
		tags:         data.Tags,
	}, nil
}

func (rw *RestWatcher) Watch() (<-chan *InstanceData, <-chan error) {
	go func() {
		ticker := time.NewTicker(rw.Interval)

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
