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

package targetloadpacking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/util/taints"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paypal/load-watcher/pkg/watcher"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	pluginConfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/config/v1beta2"
)

const (
	Name               = "TargetLoadPacking"
    SchedulerTaintName = "ci-scheduler.openshift.io/overloaded"
	OvercommitRequestsAnnotationName   = "gravity-scheduler.openshift.io/overcommit-requests"
	TargetUtilizationOverrideLabelName = "gravity-scheduler.openshift.io/node-target-utilization"
)

var (
	requestsMilliCores           = v1beta2.DefaultRequestsMilliCores
	hostTargetUtilizationPercent = v1beta2.DefaultTargetUtilizationPercent
	hostMaximumMemoryUtilization = int64(60)
	requestsMultiplier float64
	SchedulerTaint     = v1.Taint{
		Key:    SchedulerTaintName,
		Effect: v1.TaintEffectNoSchedule,
	}
)

type TargetLoadPacking struct {
	handle       framework.Handle
	metrics      watcher.WatcherMetrics
	startTime	int64
	mu sync.RWMutex
	metricsClient metricsv.Interface
	k8sClient       clientset.Interface
	freshMetricsMap *FreshMetricsMap
	args *pluginConfig.TargetLoadPackingArgs
}

func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	kubeConfigPath, kubeConfigPresent := os.LookupEnv("KUBECONFIG")

	var config *rest.Config
	var err error
	kubeConfig := ""
	if kubeConfigPresent {
		kubeConfig = kubeConfigPath
	}

	config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		klog.Errorf("Error initializing metrics client config: %v", err)
		return nil, err
	}

	metricsClient, err := metricsv.NewForConfig(config)
	if err != nil {
		klog.Errorf("Error initializing metrics client: %v", err)
		return nil, err
	}

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	pl := &TargetLoadPacking{
		handle:          handle,
		k8sClient:       handle.ClientSet(),
		metricsClient:   metricsClient,
		freshMetricsMap: NewFreshMetricsMap(0, 220, 60),
		args: args,
	}

	hostTargetUtilizationPercent = pl.args.TargetUtilization
	requestsMilliCores = pl.args.DefaultRequests.Cpu().MilliValue()
	requestsMultiplier, _ = strconv.ParseFloat(pl.args.DefaultRequestsMultiplier, 64)

	if requestsMultiplier < 1 {
		// We can't support < 1 or we might overschedule and the kubelet will
		// eventually reject the work.
		requestsMultiplier = 1
	}

	pl.startTime = time.Now().Unix()

	go func() {
		ctx := context.TODO()
		for {
			err := pl.refreshAllNodeMetrics(ctx)
			if err != nil  {
				klog.Errorf("Unable to retrieve node list periodic metric refresh: %v", err)
			}
			time.Sleep(2 * time.Second)
		}
	}()

	go func() {
		ctx := context.TODO()
		hostTargetUtilizationPercent = args.TargetUtilization
		removeTaintCheck := time.NewTicker(time.Minute * 1)
		for range removeTaintCheck.C {

			// If the scheduler is restarted, we want to find all tainted nodes and not rely
			// on any internal state.
			nodeList, err := pl.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil  {
				klog.Errorf("Unable to retrieve node list for cluster during untainting check: %v", err)
				continue
			}

			taintedNodeNames := make([]*v1.Node, 0)
			for i := range nodeList.Items {
				node := &nodeList.Items[i]
				if taints.TaintExists(node.Spec.Taints, &SchedulerTaint) {
					taintedNodeNames = append(taintedNodeNames, &nodeList.Items[i])
				}
			}

			for _, node := range taintedNodeNames {
				nodeName := node.Name
				untaintTarget := int(float64(hostTargetUtilizationPercent) * 0.8)
				cpuUtilPercent, memoryUtilPercent, err := pl.fetchNodeMetricPercentages(ctx, node, 30)
				if err == nil {
					klog.V(6).InfoS("During tainting check node utilization", "nodeName", nodeName, "cpu", cpuUtilPercent, "memory", memoryUtilPercent, "cpuTarget%", untaintTarget)
					if cpuUtilPercent < untaintTarget {
						pl.setNodeTaint(ctx, node.Name, false)
					}
				} else {
					klog.Errorf("Unable to retrieve node %v metrics for tainting check: %v", nodeName, err)
				}
			}

		}
	}()

	return pl, nil
}

func (pl *TargetLoadPacking)  getPodAdder(pod *v1.Pod) (int64, int64) {
	if pod != nil {
		fakeNodeInfo := framework.NewNodeInfo(pod)
		// Ignore pods which were scheduled to nodes before we started
		// scheduling.
		if pod.GetCreationTimestamp().Unix() > pl.startTime {
			cpuAdder := int64(float64(fakeNodeInfo.Requested.MilliCPU) * requestsMultiplier)
			if cpuAdder < 100 {
				// if the pod is claiming very low CPU requests, reserve a reasonable amount
				// until it shows us what it really needs.
				cpuAdder = 500
			}
			memoryAdder := fakeNodeInfo.NonZeroRequested.Memory
			return cpuAdder, memoryAdder
		}
	}
	return 0, 0
}

func (pl *TargetLoadPacking) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	nodeName := nodeInfo.Node().Name
	podName := pod.Namespace + "/" + pod.Name

	// It is important to note here that Score() is not called if there is only one feasible node.
	// This is why we must do this check in Filter().
	fits, err := pl.doesPodFitNode(ctx, pod, nodeInfo, 10)

	if err != nil {
		// Don't taint. doesPodFitNode will have done this for an error, when appropriate.
		klog.Errorf("Unable to check pod %v node fit %v: %v", podName, nodeName, err)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Unable to check node %v metrics: %v", nodeName, err))
	}

	if ! fits {
		klog.V(6).InfoS("Filter() will NOT consider node with utilization", "nodeName", nodeName, "pod", podName)
		pl.setNodeTaint(ctx, nodeName, true)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Node %v is heavily utilized", nodeName))
	}

	// Unfortunately, if there is only one node, other lifecycle methods like Score() and NormalizeScore()
	// will not be called. So Filter() has to assume that there may only be one node and that Filter() is the final
	// decider. Thus, add the pod Adder now for all nodes and then remove it from unselected nodes
	// once IF Score() is called.
	cpuAdder, memoryAdder := pl.getPodAdder(pod)
	pl.freshMetricsMap.AddAdder(nodeName, cpuAdder, memoryAdder)
	klog.V(6).InfoS("Filter() WILL consider node", "nodeName", nodeName, "pod", podName, "cpuAdder", cpuAdder, "memoryAdder", memoryAdder)
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

func (pl *TargetLoadPacking) doesPodFitNode(ctx context.Context, pod *v1.Pod, nodeInfo *framework.NodeInfo, noOlderThan int64) (bool, error) {
	incomingPodName := pod.ObjectMeta.Namespace + "/" + pod.Name
	nodeName := nodeInfo.Node().Name

	nodeCPUMillisUsed, nodeMemoryUsed, err := pl.fetchNodeMetrics(ctx, nodeName, noOlderThan)
	if err != nil {
		klog.ErrorS(nil, "CPU & memory metric not found for node", "nodeName")
		_ = pl.setNodeTaint(ctx, nodeName, true)
		return false, err
	}

	nodeAllocatableCPUMillis := nodeInfo.Allocatable.MilliCPU
	nodeAllocatableMemory := nodeInfo.Allocatable.Memory

	// Construct a fake node to calculate requests
	fakeNodeInfo := framework.NewNodeInfo(pod)
	incomingRequestCPUMillis := fakeNodeInfo.Requested.MilliCPU

	if incomingRequestCPUMillis > nodeAllocatableCPUMillis {
		klog.ErrorS(nil, "Pod requests greater than all allocatable CPU", "nodeName", "podRequest", incomingRequestCPUMillis, "allocatable", nodeAllocatableCPUMillis)
		// Don't taint. Just let normal resource utilization filtering take over.
		return false, err
	}

	testCPUMillis := int64(float64(incomingRequestCPUMillis) * requestsMultiplier)
	maxMillis := hostTargetUtilizationPercent * nodeAllocatableCPUMillis / 100
	if testCPUMillis > maxMillis {
		// our request can't be larger than the node allocatable or
		// we continue to scale the cluster and never be able to seat the pod.
		incomingRequestCPUMillis = maxMillis
	} else {
		incomingRequestCPUMillis = testCPUMillis
	}

	incomingRequestMemory := fakeNodeInfo.Requested.Memory // bytes

	klog.V(6).InfoS("Requests for incoming pod", "podName", incomingPodName, "millis", incomingRequestCPUMillis)

	inUseCPUPercent := int((100 * nodeCPUMillisUsed) / nodeAllocatableCPUMillis)
	inUseMemoryPercent := int((100 * nodeMemoryUsed) / nodeAllocatableMemory)

	klog.V(6).InfoS("Measured current node utilization on node for incoming pod",
		"nodeName", nodeName,
		"inUseCPU%", inUseCPUPercent,
		"inUseMemory%", inUseMemoryPercent,
		"podName", incomingPodName,
	)


	scheduledRequestedCPUMillis := int64(float64(nodeInfo.Requested.MilliCPU) * requestsMultiplier)
	scheduledRequestedMemory := nodeInfo.Requested.Memory
	podCount := len(nodeInfo.Pods)

	klog.V(6).InfoS("Existing requests for node",
		"nodeName", nodeName,
		"requestsMultiplier", requestsMultiplier,
		"scheduledRequestedCPUMillis", scheduledRequestedCPUMillis,
		"scheduledRequestedMemory", scheduledRequestedMemory,
		"podCount", podCount,
	)

	fits := false
	if nodeAllocatableCPUMillis != 0 && nodeAllocatableMemory != 0 {

		incomingRequestPercent := (100 * incomingRequestCPUMillis) / nodeAllocatableCPUMillis

		// Based on measured utilization on the system, what might the CPU & memory become if we schedule this pod
		predictedCPUUsage := 100 * (nodeCPUMillisUsed + incomingRequestCPUMillis) / nodeAllocatableCPUMillis
		predictedMemoryUsage := 100 * (nodeMemoryUsed + incomingRequestMemory) / nodeAllocatableMemory

		// Based on total requests, what would the new requests be
		newCPURequestsPercent := 100 * (scheduledRequestedCPUMillis + incomingRequestCPUMillis) / nodeAllocatableCPUMillis
		newMemoryRequestsPercent := 100 * (scheduledRequestedMemory + incomingRequestMemory) / nodeAllocatableMemory

		bigPodFit := false
		if ((100 * incomingRequestPercent) / hostTargetUtilizationPercent > 80) && predictedCPUUsage < 100 {
			bigPodFit = true
			klog.V(6).InfoS("Running bigpod logic for", "nodeName", nodeName, "podName%", incomingPodName)
		}

		if podCount > 130 {
			klog.V(6).InfoS("Node will NOT be prioritized because pod count is high", "nodeName", nodeName, "podCount", podCount)
		} else if newCPURequestsPercent > 94 {
			klog.V(6).InfoS("Node will NOT be prioritized because combined CPU requests would be too high", "nodeName", nodeName, "newCPURequests%", newCPURequestsPercent)
		} else if newMemoryRequestsPercent > 94 {
			klog.V(6).InfoS("Node will NOT be prioritized because combined Memory requests would be too high", "nodeName", nodeName, "newMemoryRequests%", newMemoryRequestsPercent)
		} else if predictedMemoryUsage > hostMaximumMemoryUtilization {
			klog.V(6).InfoS("Node will NOT be prioritized because predicted memory utilization would be too high", "nodeName", nodeName, "predictedMemory%", predictedMemoryUsage)
		} else if !bigPodFit && predictedCPUUsage > hostTargetUtilizationPercent {
			klog.V(6).InfoS("Node will NOT be prioritized because predicted CPU utilization would be too high", "nodeName", nodeName, "predictedCPU%", predictedCPUUsage)
		} else {
			klog.V(6).InfoS("Node WILL be prioritized because scheduled resources and measured CPU utilization are within targets",
				"nodeName", nodeName,
				"predictedCPU%", predictedCPUUsage,
				"predictedMemory%", predictedMemoryUsage,
				"newCPURequests%", newCPURequestsPercent,
				"newMemoryRequests%", newMemoryRequestsPercent,
				)
			fits = true
		}
	}

	return fits, nil
}

func (pl *TargetLoadPacking) Score(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	// Filter() should have done all the calculations necessary to eliminate nodes
	// this pod will NOT fit. So everything remaining gets max score.
	var score = framework.MaxNodeScore
	klog.V(6).InfoS("Score for host", "nodeName", nodeName, "score", score)

	// Remove the Adder that Filter() would have added since the framework will call NormalizeScore
	// next -- which does a final check on fit.
	cpuAdder, memoryAdder := pl.getPodAdder(pod)
	pl.freshMetricsMap.AddAdder(nodeName, -1*cpuAdder, -1*memoryAdder)

	return score, framework.NewStatus(framework.Success, "")
}

type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value bool   `json:"value"`
}

func (pl *TargetLoadPacking) setNodeCordon(ctx context.Context, nodeName string, doCordon bool) error {
	payload := []patchStringValue{{
		Op:    "replace",
		Path:  "/spec/unschedulable",
		Value: doCordon,
	}}
	payloadBytes, _ := json.Marshal(payload)
	_, err := pl.k8sClient.CoreV1().Nodes().Patch(ctx, nodeName, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return err
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
		// We also cordon to convince the autoscaler to add new nodes
		pl.setNodeCordon(ctx, nodeName, true)
		if err != nil {
			klog.Errorf("Unable to apply cordon %v: %v", nodeName, err)
		}
	} else {
		err := controller.RemoveTaintOffNode(pl.k8sClient, nodeName, node, &SchedulerTaint)
		if err != nil {
			klog.Errorf("Unable to remove taint %v to %v: %v", SchedulerTaintName, nodeName, err)
		}
		pl.setNodeCordon(ctx, nodeName, false)
		if err != nil {
			klog.Errorf("Unable to remove cordon %v: %v", nodeName, err)
		}
	}

	klog.V(6).InfoS("Successfully changed taint of node", "nodeName", nodeName, "taint", SchedulerTaintName, "doTaint", doTaint)
	return nil
}

func (pl *TargetLoadPacking) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *TargetLoadPacking) _fetchLiveHostMetrics(ctx context.Context, nodeName string) (int64, int64, error) {
	nodeMetrics, err := pl.metricsClient.MetricsV1beta1().NodeMetricses().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("unable to read latest node metrics: %v", err)
	}
	return nodeMetrics.Usage.Cpu().MilliValue(), nodeMetrics.Usage.Memory().Value(), nil
}

// fetchNodeMetrics Retrieves fresh but not immediate API results from node.
func (pl *TargetLoadPacking) fetchNodeMetrics(ctx context.Context, nodeName string, noOlderThan int64) (int64, int64, error) {
	metricsSample, err := pl.freshMetricsMap.GetOrPut(nodeName, noOlderThan,
		func() (int64, int64, error) {
			cpuMillis, memory, err := pl._fetchLiveHostMetrics(ctx, nodeName)
			if err != nil {
				return 0, 0, fmt.Errorf("unable to refresh latest node metrics for %v: %v", nodeName, err)
			}
			return cpuMillis, memory, nil
		},
	)
	if err != nil {
		return 0, 0, err
	}
	return metricsSample.CpuMillis, metricsSample.Memory, nil
}

func (pl *TargetLoadPacking) refreshAllNodeMetrics(ctx context.Context) error {
	metrics, err := pl.metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Unable to list all node metrics: %v", err)
		return err
	}
	for _, v := range metrics.Items {
		pl.freshMetricsMap.Put(v.Name, v.Usage.Cpu().MilliValue(), v.Usage.Memory().Value())
	}
	return nil
}

func (pl *TargetLoadPacking) fetchNodeMetricPercentages(ctx context.Context, node *v1.Node, noOlderThan int64) (int, int, error) {
	nodeName := node.Name
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)

	if err != nil {
		return 0, 0, fmt.Errorf("error getting node %q from Snapshot: %v", nodeName, err)
	}

	cpuMillis, memory, err := pl.fetchNodeMetrics(ctx, nodeName, noOlderThan)
	if err != nil {
		klog.ErrorS(err, "CPU & memory metric not available for node", "nodeName")
		return 0, 0, err
	}

	nodeAllocatableCPUMillis := nodeInfo.Allocatable.MilliCPU
	nodeAllocatableMemory := nodeInfo.Allocatable.Memory // bytes

	nodeCPUPercentUsed := int((100 * cpuMillis) / nodeAllocatableCPUMillis)
	nodeMemoryPercentUsed := int((100 * memory) / nodeAllocatableMemory)

	return nodeCPUPercentUsed, nodeMemoryPercentUsed, nil
}


func (pl *TargetLoadPacking) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	podName := pod.Namespace + "/" + pod.Name

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
				nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeScore.Name)

				if err == nil {
					var fits bool
					fits, err = pl.doesPodFitNode(ctx, pod, nodeInfo, 1)
					if err == nil {
						if !fits {
							klog.V(6).InfoS("Final node utilization check found node is overutilized; eliminating from consideration", "targetNode", nodeScore.Name, "pod", podName)
						} else {
							// A final update to the Adder. It would be really nice if Score/NormalizeScore were called
							// if there were more than 1 feasible, node, but that will take upstream changes.
							cpuAdder, memoryAdder := pl.getPodAdder(pod)
							pl.freshMetricsMap.AddAdder(nodeScore.Name, cpuAdder, memoryAdder)
							klog.V(6).InfoS("Found first underutilized node; other nodes scores will be eliminated", "targetNode", nodeScore.Name, "score", nodeScore.Score)
							targetNodeFound = true
						}
					}
				}

				if err != nil {
					// No tainting here, doesPodFitNode should do so if it makes sense based on the error.
					klog.Errorf("Final node utilization check failed for pod %v on %v; will not assign: %v", podName, nodeScore.Name, err)
					scoresMap[nodeScore.Name].Score = framework.MinNodeScore
				} else if !targetNodeFound {
					pl.setNodeTaint(ctx, nodeScore.Name, true)
					scoresMap[nodeScore.Name].Score = framework.MinNodeScore
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
