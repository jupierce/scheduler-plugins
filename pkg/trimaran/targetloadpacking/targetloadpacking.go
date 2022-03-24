/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
targetloadpacking package provides K8s scheduler plugin for best-fit variant of bin packing based on CPU utilization around a target load
It contains plugin for Score extension point.
*/

package targetloadpacking

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paypal/load-watcher/pkg/watcher"
	loadwatcherapi "github.com/paypal/load-watcher/pkg/watcher/api"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	pluginConfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/config/v1beta2"
	"sigs.k8s.io/scheduler-plugins/pkg/trimaran"
)

const (
	// Time interval in seconds for each metrics agent ingestion.
	metricsAgentReportingIntervalSeconds = 60
	metricsUpdateIntervalSeconds         = 30
	LoadWatcherServiceClientName         = "load-watcher"
	Name                                 = "TargetLoadPacking"
)

var (
	requestsMilliCores           = v1beta2.DefaultRequestsMilliCores
	hostTargetUtilizationPercent = v1beta2.DefaultTargetUtilizationPercent
	requestsMultiplier           float64
)

type TargetLoadPacking struct {
	handle       framework.Handle
	client       loadwatcherapi.Client
	metrics      watcher.WatcherMetrics
	eventHandler *trimaran.PodAssignEventHandler
	// For safe access to metrics
	mu sync.RWMutex
}

func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}
	hostTargetUtilizationPercent = args.TargetUtilization
	requestsMilliCores = args.DefaultRequests.Cpu().MilliValue()
	requestsMultiplier, _ = strconv.ParseFloat(args.DefaultRequestsMultiplier, 64)

	podAssignEventHandler := trimaran.New()

	var client loadwatcherapi.Client
	if args.WatcherAddress != "" {
		client, err = loadwatcherapi.NewServiceClient(args.WatcherAddress)
	} else {
		opts := watcher.MetricsProviderOpts{string(args.MetricProvider.Type), args.MetricProvider.Address, args.MetricProvider.Token}
		client, err = loadwatcherapi.NewLibraryClient(opts)
	}
	if err != nil {
		return nil, err
	}

	pl := &TargetLoadPacking{
		handle:       handle,
		client:       client,
		eventHandler: podAssignEventHandler,
	}

	pl.handle.SharedInformerFactory().Core().V1().Pods().Informer().AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return isAssigned(t)
				case cache.DeletedFinalStateUnknown:
					if pod, ok := t.Obj.(*v1.Pod); ok {
						return isAssigned(pod)
					}
					klog.Errorf("unable to convert object %T to *v1.Pod in %T", obj, pl)
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, pl))
					return false
				default:
					klog.Errorf("unable to handle object in %T: %T", pl, obj)
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", pl, obj))
					return false
				}
			},
			Handler: podAssignEventHandler,
		},
		time.Minute * 2,
	)

	// populate metrics before returning
	err = pl.updateMetrics()
	if err != nil {
		klog.ErrorS(err, "Unable to populate metrics initially")
	}
	go func() {
		metricsUpdaterTicker := time.NewTicker(time.Second * metricsUpdateIntervalSeconds)
		for range metricsUpdaterTicker.C {
			err = pl.updateMetrics()
			if err != nil {
				klog.ErrorS(err, "Unable to update metrics")
			}
		}
	}()

	return pl, nil
}

func (pl *TargetLoadPacking) updateMetrics() error {
	metrics, err := pl.client.GetLatestWatcherMetrics()
	if err != nil {
		return err
	}

	pl.mu.Lock()
	pl.metrics = *metrics
	pl.mu.Unlock()

	return nil
}

func (pl *TargetLoadPacking) Name() string {
	return Name
}

func getArgs(obj runtime.Object) (*pluginConfig.TargetLoadPackingArgs, error) {
	args, ok := obj.(*pluginConfig.TargetLoadPackingArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type TargetLoadPackingArgs, got %T", obj)
	}
	if args.WatcherAddress == "" {
		metricProviderType := string(args.MetricProvider.Type)
		validMetricProviderType := metricProviderType == string(pluginConfig.KubernetesMetricsServer) ||
			metricProviderType == string(pluginConfig.Prometheus) ||
			metricProviderType == string(pluginConfig.SignalFx)
		if !validMetricProviderType {
			return nil, fmt.Errorf("invalid MetricProvider.Type, got %T", args.MetricProvider.Type)
		}
	}
	_, err := strconv.ParseFloat(args.DefaultRequestsMultiplier, 64)
	if err != nil {
		return nil, errors.New("unable to parse DefaultRequestsMultiplier: " + err.Error())
	}
	return args, nil
}

func (pl *TargetLoadPacking) Score(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return framework.MinNodeScore, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	// copy value lest updateMetrics() updates it and to avoid locking for rest of the function
	pl.mu.RLock()
	metrics := pl.metrics
	pl.mu.RUnlock()

	// This happens if metrics were never populated since scheduler started
	if metrics.Data.NodeMetricsMap == nil {
		klog.ErrorS(nil, "Metrics not available from watcher, assigning 0 score to node", "nodeName", nodeName)
		return framework.MinNodeScore, nil
	}
	// This means the node is new (no metrics yet) or metrics are unavailable due to 404 or 500
	if _, ok := metrics.Data.NodeMetricsMap[nodeName]; !ok {
		klog.InfoS("Unable to find metrics for node", "nodeName", nodeName)
		// Avoid the node by scoring minimum
		return framework.MinNodeScore, nil
		// TODO(aqadeer): If this happens for a long time, fall back to allocation based packing. This could mean maintaining failure state across cycles if scheduler doesn't provide this state
	}

	incomingPodName := pod.ObjectMeta.Namespace + "/" + pod.Name
	incomingPodCPUMillis := CalculatePodCPURequests(pod)
	if pod.Spec.Overhead != nil {
		incomingPodCPUMillis += pod.Spec.Overhead.Cpu().MilliValue()
	}
	klog.V(6).InfoS("Predicted CPU usage for incoming pod", "podName", incomingPodName, "millis", incomingPodCPUMillis)

	var nodeCPUUtilPercent float64
	var cpuMetricFound bool
	for _, metric := range metrics.Data.NodeMetricsMap[nodeName].Metrics {
		if metric.Type == watcher.CPU {
			if metric.Operator == watcher.Average || metric.Operator == watcher.Latest {
				nodeCPUUtilPercent = metric.Value
				cpuMetricFound = true
			}
		}
	}

	if !cpuMetricFound {
		klog.ErrorS(nil, "CPU metric not found in node metrics", "nodeName", nodeName, "nodeMetrics", metrics.Data.NodeMetricsMap[nodeName].Metrics)
		return framework.MinNodeScore, nil
	}
	nodeCPUCapMillis := float64(nodeInfo.Node().Status.Capacity.Cpu().MilliValue())
	nodeCPUUtilMillis := (nodeCPUUtilPercent / 100) * nodeCPUCapMillis

	klog.V(6).InfoS("Calculated CPU utilization and capacity", "nodeName", nodeName, "cpuUtilPercent", nodeCPUUtilPercent, "cpuUtilMillis", nodeCPUUtilMillis, "cpuCapMillis", nodeCPUCapMillis)

	// The number of CPU millis claimed by pod.spec.containers[].resources limits or requests (starting with a base of the incoming pod's need.
	var scheduledResourcesCPUMillis int64 = 0
	pl.eventHandler.RLock()
	podCount := len(pl.eventHandler.ScheduledPodsCache[nodeName])
	for _, info := range pl.eventHandler.ScheduledPodsCache[nodeName] {
		podName := info.Pod.ObjectMeta.Namespace + "/" + info.Pod.Name
		podCPURequest := CalculatePodCPURequests(info.Pod)
		scheduledResourcesCPUMillis += podCPURequest
		klog.V(6).InfoS("Found CPU requests for incoming pod", nodeName, nodeName, "podName", podName, "podCPURequest", podCPURequest)
	}
	pl.eventHandler.RUnlock()

	klog.V(6).InfoS("Existing resource claims for node", "nodeName", nodeName, "scheduledResourcesCPUMillis", scheduledResourcesCPUMillis, "podCount", podCount)

	score := framework.MinNodeScore
	if nodeCPUCapMillis != 0 && scheduledResourcesCPUMillis != 0 {
		predictedCPUUsage := 100 * (nodeCPUUtilMillis + float64(incomingPodCPUMillis)) / nodeCPUCapMillis
		scheduledResourceReservations := 100 * float64(scheduledResourcesCPUMillis + incomingPodCPUMillis) / nodeCPUCapMillis
		if podCount > 130 {
			klog.V(6).InfoS("Node will NOT be prioritized because pod count is high", "nodeName", nodeName, "podCount", podCount)
		} else if scheduledResourceReservations > 95 {
			klog.V(6).InfoS("Node will NOT be prioritized because scheduled resources are high", "nodeName", nodeName, "scheduledResourceReservations", scheduledResourceReservations)
		} else if predictedCPUUsage > float64(hostTargetUtilizationPercent) {
			klog.V(6).InfoS("Node will NOT be prioritized because predicted CPU utilization is high", "nodeName", nodeName, "predictedCPUUsage", predictedCPUUsage)
		} else {
			klog.V(6).InfoS("Node WILL be prioritized because scheduled resources and measured CPU utilization are within targets", "nodeName", nodeName, "scheduledResourceReservations", scheduledResourceReservations, "predictedCPUUsage", predictedCPUUsage, )
			score = framework.MaxNodeScore
		}
	}

	klog.V(6).InfoS("Score for host", "nodeName", nodeName, "score", score)
	return score, framework.NewStatus(framework.Success, "")
}

func (pl *TargetLoadPacking) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *TargetLoadPacking) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {

	scoresCopy := make(framework.NodeScoreList, len(scores))
	copy(scoresCopy, scores)
	sort.Slice(scoresCopy, func(i, j int) bool {
		return strings.Compare(scoresCopy[i].Name, scoresCopy[j].Name) < 0
	})

	klog.V(6).InfoS("Incoming nodes in order of preferred name", "scoresCopy", scoresCopy)

	scoresMap := make(map[string]*framework.NodeScore)
	for i := range scores {
		scoresMap[scores[i].Name] = &scores[i]
	}

	targetNodeFound := false
	for _, nodeScore := range scoresCopy {
		if targetNodeFound {
			klog.V(6).InfoS("Already found target node; setting this node's score to 0", "name", nodeScore.Name)
			scoresMap[nodeScore.Name].Score = framework.MinNodeScore
		} else {
			if nodeScore.Score == framework.MaxNodeScore {
				klog.V(6).InfoS("Found first underutilized node; other nodes scores will be eliminated", "targetNode", nodeScore.Name, "score", nodeScore.Score)
				targetNodeFound = true
			} else {
				klog.V(6).InfoS("Found overutilized node while searching for target", "name", nodeScore.Name, "score", nodeScore.Score)
			}
		}
	}

	/*
	// Find highest and lowest scores.
	boostIndex := 0
	boostName := scores[0].Name
	for i, nodeScore := range scores {
		klog.V(6).InfoS("Analyzing node", "name", nodeScore.Name, "score", nodeScore.Score)
		if strings.Compare(boostName, nodeScore.Name) > 0 {
			boostIndex = i
			boostName = nodeScore.Name
		}
	}

	if scores[boostIndex].Score == framework.MaxNodeScore {
		klog.V(6).InfoS("Boost node has capacity; minimizing other nodes", "boostName", boostName)
		for i, _ := range scores {
			if i != boostIndex {
				scores[i].Score = framework.MinNodeScore
			}
		}
	} else {
		klog.V(6).InfoS("Boost node is on target; letting other scores stand", "boostName", boostName, "count", len(scores))
	}
	*/

	for _, nodeScore := range scores {
		klog.V(6).InfoS("Scored node", "name", nodeScore.Name, "score", nodeScore.Score)
	}

	// REMOVE  REMOVE  REMOVE
	// REMOVE  REMOVE  REMOVE
	// REMOVE  REMOVE  REMOVE
	/*
	for i, nodeScore := range scores {
		scores[i].Score = 0
		klog.V(6).InfoS("DEBUG CLEARING ALL SCORES", "name", nodeScore.Name, "score", nodeScore.Score)
	}
	*/

	return nil
}

// Checks and returns true if the pod is assigned to a node
func isAssigned(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func CalculatePodCPURequests(pod *v1.Pod) int64 {
	var incomingPodCPUMillis int64
	incomingPodName := pod.ObjectMeta.Namespace + "/" + pod.Name
	for _, container := range pod.Spec.Containers {
		incomingPodCPUMillis += CalculateContainerCPURequests(&container)
	}

	// In cases where init containers claim limits that exceed pod resources, the init resources/limits take over as the
	// effective requested resources for the pod: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#resources
	var incomingPodInitCPUMillis int64
	for _, container := range pod.Spec.InitContainers {
		incomingPodInitCPUMillis += CalculateContainerCPURequests(&container)
	}

	finalRequestMillis := incomingPodCPUMillis
	if incomingPodInitCPUMillis > incomingPodCPUMillis {
		finalRequestMillis = incomingPodInitCPUMillis
	}
	klog.V(6).InfoS("Incoming pod resource requests calculated", "podName", incomingPodName, "mainRequests", incomingPodCPUMillis, "initRequests", incomingPodInitCPUMillis, "finalRequestMillis", finalRequestMillis )
	return finalRequestMillis
}

// Predict utilization for a container based on its requests/limits
func CalculateContainerCPURequests(container *v1.Container) int64 {
	if _, ok := container.Resources.Limits[v1.ResourceCPU]; ok {
		return container.Resources.Limits.Cpu().MilliValue()
	} else if _, ok := container.Resources.Requests[v1.ResourceCPU]; ok {
		return int64(math.Round(float64(container.Resources.Requests.Cpu().MilliValue()) * requestsMultiplier))
	} else {
		return requestsMilliCores
	}
}
