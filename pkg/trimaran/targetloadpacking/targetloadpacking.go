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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/kubernetes/pkg/controller"
	"math"
	"os"
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
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	pluginConfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/config/v1beta2"
	"sigs.k8s.io/scheduler-plugins/pkg/trimaran"
)

const (
	// Time interval in seconds for each metrics agent ingestion.
	metricsAgentReportingIntervalSeconds = 60
	metricsUpdateIntervalSeconds         = 20
	LoadWatcherServiceClientName         = "load-watcher"
	Name               = "TargetLoadPacking"
    SchedulerTaintName = "ci-scheduler.openshift.io/overloaded"
)

var (
	requestsMilliCores           = v1beta2.DefaultRequestsMilliCores
	hostTargetUtilizationPercent = v1beta2.DefaultTargetUtilizationPercent
	requestsMultiplier float64
	SchedulerTaint     = v1.Taint{
		Key:    SchedulerTaintName,
		Effect: v1.TaintEffectNoSchedule,
	}
)

type TargetLoadPacking struct {
	handle       framework.Handle
	client       loadwatcherapi.Client
	metrics      watcher.WatcherMetrics
	eventHandler     *trimaran.PodAssignEventHandler
	// For safe access to metrics
	mu sync.RWMutex

	metricsClient metricsv.Interface
	k8sClient    clientset.Interface
}

func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.Warning("HERE1")
	kubeConfigPath, kubeConfigPresent := os.LookupEnv("KUBECONFIG")

	var config *rest.Config
	var err error
	kubeConfig := ""
	if kubeConfigPresent {
		kubeConfig = kubeConfigPath
	}

	klog.Warning("HERE2")
	config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		klog.Errorf("Error initializing metrics client config: %v", err)
		return nil, err
	}

	klog.Warning("HERE3")
	metricsClient, err := metricsv.NewForConfig(config)
	if err != nil {
		klog.Errorf("Error initializing metrics client: %v", err)
		return nil, err
	}

	klog.Warning("HERE4")
	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}
	hostTargetUtilizationPercent = args.TargetUtilization
	requestsMilliCores = args.DefaultRequests.Cpu().MilliValue()
	requestsMultiplier, _ = strconv.ParseFloat(args.DefaultRequestsMultiplier, 64)

	klog.Warning("HERE5")
	podAssignEventHandler := trimaran.New()
	klog.Warning("HERE6")
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

	klog.Warning("HERE7")
	pl := &TargetLoadPacking{
		handle:       handle,
		client:       client,
		eventHandler: podAssignEventHandler,
		k8sClient:    handle.ClientSet(),
		metricsClient: metricsClient,
	}
	klog.Warning("HERE8")

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

	klog.Warning("HERE9")
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

	klog.Warning("HERE10")
	go func() {
		ctx := context.TODO()
		hostTargetUtilizationPercent = args.TargetUtilization
		uncordonCheck := time.NewTicker(time.Minute * 1)
		for range uncordonCheck.C {

			// If the scheduler is restarted, we want to find all tainted nodes and not rely
			// on any internal state.
			nodeList, err := pl.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil  {
				klog.Errorf("Unable to retrieve node list for cluster during untainting check: %v", err)
				continue
			}

			taintedNodeNames := make([]string, 0)
			for _, node := range nodeList.Items {
				if taints.TaintExists(node.Spec.Taints, &SchedulerTaint) {
					taintedNodeNames = append(taintedNodeNames, node.Name)
				}
			}

			for _, nodeName := range taintedNodeNames {
				untaintTarget := float64(hostTargetUtilizationPercent) * 0.8
				cpuUtilPercent, memoryUtilPercent, err := pl.getWatcherUtilizations(ctx, nodeName)

				if err == nil {
					klog.V(6).InfoS("During tainting check node utilization", "nodeName", nodeName, "cpu", cpuUtilPercent, "memory", memoryUtilPercent, "cpuTarget%", untaintTarget)
					if cpuUtilPercent < untaintTarget {
						pl.setNodeTaint(ctx, nodeName, false)
					}
				} else {
					klog.Errorf("Unable to retrieve node %v metrics for tainting check: %v", nodeName, err)
				}
			}

		}
	}()

	klog.Warning("HERE11")
	return pl, nil
}

func (pl *TargetLoadPacking) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	nodeName := nodeInfo.Node().Name
	cpuUtilPercent, memoryUtilPercent, err := pl.getWatcherUtilizations(ctx, nodeName)
	if err != nil {
		pl.setNodeTaint(ctx, nodeName, true)
		klog.Errorf("Unable to check node %v metrics: %v", nodeName, err)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Unable to check node %v metrics: %v", nodeName, err))
	}
	if cpuUtilPercent >= float64(hostTargetUtilizationPercent) || memoryUtilPercent >= 60 {
		klog.Errorf("Unable to check node %v metrics: %v", nodeName, err)
		pl.setNodeTaint(ctx, nodeName, true)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Node %v is heavily utilized (cpu=%v, memory=%v); tainting", nodeName, cpuUtilPercent, memoryUtilPercent))
	}
	klog.V(6).InfoS("Filter() call found node utilization within tolerances", "nodeName", nodeName, "cpu", cpuUtilPercent, "memory", memoryUtilPercent)
	return nil
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
	klog.Warning("HERE-SCORE")
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return framework.MinNodeScore, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	incomingPodName := pod.ObjectMeta.Namespace + "/" + pod.Name
	incomingPodCPUMillis := CalculatePodCPURequests(pod)
	if pod.Spec.Overhead != nil {
		incomingPodCPUMillis += pod.Spec.Overhead.Cpu().MilliValue()
	}

	klog.V(6).InfoS("Predicted CPU usage for incoming pod", "podName", incomingPodName, "millis", incomingPodCPUMillis)

	nodeCPUUtilPercent, memoryUtilPercent, err := pl.getWatcherUtilizations(ctx, nodeName)
	if err != nil {
		klog.ErrorS(nil, "CPU & memory metric not found for node", "nodeName")
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
		podCPURequest := CalculatePodCPURequests(info.Pod)
		scheduledResourcesCPUMillis += podCPURequest
	}
	pl.eventHandler.RUnlock()

	klog.V(6).InfoS("Existing resource claims for node", "nodeName", nodeName, "scheduledResourcesCPUMillis", scheduledResourcesCPUMillis, "podCount", podCount)

	score := framework.MinNodeScore
	if nodeCPUCapMillis != 0 && scheduledResourcesCPUMillis != 0 {
		predictedCPUUsage := 100 * (nodeCPUUtilMillis + float64(incomingPodCPUMillis)) / nodeCPUCapMillis
		scheduledResourceReservations := 100 * float64(scheduledResourcesCPUMillis + incomingPodCPUMillis) / nodeCPUCapMillis
		if podCount > 130 {
			klog.V(6).InfoS("Node will NOT be prioritized because pod count is high", "nodeName", nodeName, "podCount", podCount)
		} else if scheduledResourceReservations > 94 {
			klog.V(6).InfoS("Node will NOT be prioritized because combined resources claims would be too high", "nodeName", nodeName, "scheduledResourceReservations", scheduledResourceReservations)
		} else if memoryUtilPercent > 60 {
			klog.V(6).InfoS("Node will NOT be prioritized because memory utilization is too high", "nodeName", nodeName, "memoryUtilPercent", memoryUtilPercent)
		} else if predictedCPUUsage > float64(hostTargetUtilizationPercent) {
			klog.V(6).InfoS("Node will NOT be prioritized because predicted CPU utilization is high", "nodeName", nodeName, "predictedCPUUsage", predictedCPUUsage)
			err := pl.setNodeTaint(ctx, nodeName, true)
			if err != nil {
				klog.Errorf("Unable to cordon node %v! Nodes may be overburdened soon!", nodeName)
			}
		} else {
			klog.V(6).InfoS("Node WILL be prioritized because scheduled resources and measured CPU utilization are within targets", "nodeName", nodeName, "scheduledResourceReservations", scheduledResourceReservations, "predictedCPUUsage", predictedCPUUsage, )
			score = framework.MaxNodeScore
		}
	}

	klog.V(6).InfoS("Score for host", "nodeName", nodeName, "score", score)
	return score, framework.NewStatus(framework.Success, "")
}

func (pl *TargetLoadPacking) setNodeTaint(ctx context.Context, nodeName string, doTaint bool) error {

	node, err := pl.k8sClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Unable to get node %v to taint it %v: %v", nodeName, doTaint, err)
		return err
	}

	if doTaint {
		err := controller.AddOrUpdateTaintOnNode(pl.k8sClient, nodeName, &SchedulerTaint)
		if err != nil {
			klog.Errorf("Unable to apply taint %v to %v: %v", SchedulerTaintName, nodeName, err)
		}
	} else {
		err := controller.RemoveTaintOffNode(pl.k8sClient, nodeName, node, &SchedulerTaint)
		if err != nil {
			klog.Errorf("Unable to remove taint %v to %v: %v", SchedulerTaintName, nodeName, err)
		}
	}

	klog.V(6).InfoS("Successfully changed taint of node", "nodeName", nodeName, "taint", SchedulerTaintName, "doTaint", doTaint)
	return nil
}

func (pl *TargetLoadPacking) ScoreExtensions() framework.ScoreExtensions {
	return pl
}


func (pl *TargetLoadPacking) getWatcherNodeMetrics(ctx context.Context, nodeName string) (*watcher.NodeMetrics, error) {
	// copy value lest updateMetrics() updates it and to avoid locking for rest of the function
	pl.mu.RLock()
	metrics := pl.metrics
	pl.mu.RUnlock()

	// This happens if metrics were never populated since scheduler started
	if metrics.Data.NodeMetricsMap == nil {
		return nil, fmt.Errorf("no metrics at all from watcher while checking %v", nodeName)
	}
	// This means the node is new (no metrics yet) or metrics are unavailable due to 404 or 500
	if _, ok := metrics.Data.NodeMetricsMap[nodeName]; !ok {
		return nil, fmt.Errorf("no metrics from watcher for %v", nodeName)
	}

	nodeMetrics := metrics.Data.NodeMetricsMap[nodeName]
	return &nodeMetrics, nil
}


func (pl *TargetLoadPacking) getWatcherUtilizations(ctx context.Context, nodeName string) (float64, float64, error){

	nodeMetrics, err := pl.getWatcherNodeMetrics(ctx, nodeName)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to assess node %v CPU & memory utilization: %v", nodeName, err)
	}

	var cpu float64 = 0
	var memory float64 = 0
	for _, metric := range nodeMetrics.Metrics {
		if metric.Type == watcher.CPU {
			if cpu == 0 && metric.Operator == watcher.Average {
				// Permit average if latest is not found.
				cpu = metric.Value
			}
			if metric.Operator == watcher.Latest {
				// Prefer latest to average
				cpu = metric.Value
			}
		} else if metric.Type == watcher.Memory {
			if memory == 0 && metric.Operator == watcher.Average {
				// Permit average if latest is not found.
				memory = metric.Value
			}
			if metric.Operator == watcher.Latest {
				// Prefer latest to average
				memory = metric.Value
			}
		}
	}

	if cpu > 0 && memory > 0 {
		return cpu, memory, nil
	}

	return 0, 0, fmt.Errorf("CPU & memory metric not found in node %v metrics: %v", nodeName, nodeMetrics.Metrics)
}

func (pl *TargetLoadPacking) FetchLiveHostMetrics(ctx context.Context, host string) (float64, float64, error) {

	nodeMetrics, err := pl.metricsClient.MetricsV1beta1().NodeMetricses().Get(ctx, host, metav1.GetOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("unable to read latest node metrics: %v", err)
	}
	node, err := pl.k8sClient.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("unable to get node information: %v", err)
	}

	cpuUsagePercent := float64(100*nodeMetrics.Usage.Cpu().MilliValue()) / float64(node.Status.Capacity.Cpu().MilliValue())
	memoryUsagePercent := float64(100*nodeMetrics.Usage.Memory().Value()) / float64(node.Status.Capacity.Memory().Value())

	return cpuUsagePercent, memoryUsagePercent, nil
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
				cpu, memory, err := pl.FetchLiveHostMetrics(ctx, nodeScore.Name)
				if err != nil {
					klog.Errorf("Final node utilization check failed for %v; will not assign: %v", nodeScore.Name, err)
				} else if cpu >= float64(hostTargetUtilizationPercent) || memory > 60 {
					klog.V(6).InfoS("Final node utilization check found node is overutilized; eliminating from consideration", "targetNode", nodeScore.Name, "cpu", cpu, "mem", memory)
					pl.setNodeTaint(ctx, nodeScore.Name, true)
					scoresMap[nodeScore.Name].Score = framework.MinNodeScore
				} else {
					klog.V(6).InfoS("Found first underutilized node; other nodes scores will be eliminated", "targetNode", nodeScore.Name, "score", nodeScore.Score)
					targetNodeFound = true
				}
			} else {
				klog.V(6).InfoS("Found overutilized node while searching for target", "name", nodeScore.Name, "score", nodeScore.Score)
			}
		}
	}

	for _, nodeScore := range scores {
		klog.V(6).InfoS("Scored node", "name", nodeScore.Name, "score", nodeScore.Score)
	}

	return nil
}

// Checks and returns true if the pod is assigned to a node
func isAssigned(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func CalculatePodCPURequests(pod *v1.Pod) int64 {
	var incomingPodCPUMillis int64
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
	return finalRequestMillis
}

// CalculateContainerCPURequests Predict utilization for a container based on its requests/limits
func CalculateContainerCPURequests(container *v1.Container) int64 {
	if _, ok := container.Resources.Limits[v1.ResourceCPU]; ok {
		return container.Resources.Limits.Cpu().MilliValue()
	} else if _, ok := container.Resources.Requests[v1.ResourceCPU]; ok {
		return int64(math.Round(float64(container.Resources.Requests.Cpu().MilliValue()) * requestsMultiplier))
	} else {
		return requestsMilliCores
	}
}
