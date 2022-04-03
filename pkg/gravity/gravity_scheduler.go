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

package gravity

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
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	pluginConfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/config/v1beta2"
)

const (
	Name                             = "Gravity"
	SchedulerTaintName               = "gravity-scheduler.k8s.io/overloaded"
	OvercommitRequestsAnnotationName = "ci-scheduler.openshift.io/overcommit-requests"
)

var (
	hostTargetUtilizationPercent = v1beta2.DefaultTargetUtilizationPercent
	hostMaximumMemoryUtilization = int64(60)
	SchedulerTaint               = v1.Taint{
		Key:    SchedulerTaintName,
		Effect: v1.TaintEffectNoSchedule,
	}
)

type Gravity struct {
	handle          framework.Handle
	metrics         watcher.WatcherMetrics
	startTime       int64
	mu              sync.RWMutex
	metricsClient   metricsv.Interface
	k8sClient       clientset.Interface
	freshMetricsMap *FreshMetricsMap
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
	hostTargetUtilizationPercent = args.TargetUtilization
	//hostMaximumMemoryUtilization = args.MaximumMemoryUtilization

	gravity := &Gravity{
		handle:          handle,
		k8sClient:       handle.ClientSet(),
		metricsClient:   metricsClient,
		freshMetricsMap: NewFreshMetricsMap(0, 220, 60),
	}

	gravity.startTime = time.Now().Unix()

	gravity.handle.SharedInformerFactory().Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return isAssigned(t)
				case cache.DeletedFinalStateUnknown:
					if pod, ok := t.Obj.(*v1.Pod); ok {
						return isAssigned(pod)
					}
					klog.Errorf("unable to convert object %T to *v1.Pod in %T", obj, gravity)
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, gravity))
					return false
				default:
					klog.Errorf("unable to handle object in %T: %T", gravity, obj)
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", gravity, obj))
					return false
				}
			},
			Handler: gravity,
		},
	)

	go func() {
		ctx := context.TODO()
		for {
			nodeList, err := gravity.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				klog.Errorf("Unable to retrieve node list periodic metric refresh: %v", err)
				continue
			}
			for _, node := range nodeList.Items {
				// Attempt to keep node metrics fresh for other users of the map
				_, _, _ = gravity.fetchNodeMetrics(ctx, node.Name, 5)
				time.Sleep(1)
			}
		}
	}()

	go func() {
		ctx := context.TODO()
		hostTargetUtilizationPercent = args.TargetUtilization
		removeTaintCheck := time.NewTicker(time.Minute * 1)
		for range removeTaintCheck.C {

			// If the scheduler is restarted, we want to find all tainted nodes and not rely
			// on any internal state.
			nodeList, err := gravity.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
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
				untaintTarget := int(float64(hostTargetUtilizationPercent) * 0.8)
				cpuUtilPercent, memoryUtilPercent, err := gravity.fetchNodeMetricPercentages(ctx, nodeName, 30)
				if err == nil {
					klog.V(6).InfoS("During tainting check node utilization", "nodeName", nodeName, "cpu", cpuUtilPercent, "memory", memoryUtilPercent, "cpuTarget%", untaintTarget)
					if cpuUtilPercent < untaintTarget {
						_ = gravity.setNodeTaint(ctx, nodeName, false)
					}
				} else {
					klog.Errorf("Unable to retrieve node %v metrics for tainting check: %v", nodeName, err)
				}
			}

		}
	}()

	return gravity, nil
}

func (gravity *Gravity) onPodAssign(pod *v1.Pod) {
	// When a pod is assigned to a node, its impact to the CPU may not be felt for some time.
	// To prevent a flood of pods from quickly being assigned to a seemingly underutilized nodes
	// and those pods subsequently consuming > 100% of the CPU, we create a temporary adder
	// to the node. This adder will increase all measured samples for the adderLifetime.
	// This is particularly important if there is only one feasible node for a Pod. In this
	// case, neither Score nor Normalize will be called -- only Filter.
	// To be performant, Filter can only pull delayed metrics from the node (on average),
	// so a quick influx of Pods can easily trick Filter into permitting the Pod
	// to the same node again and again -- until the node metrics eventually catch up.
	// By the time they catch up, it is too late. Adders provide a guard against this
	// situation.
	if pod != nil {
		fakeNodeInfo := framework.NewNodeInfo(pod)
		podName := pod.Namespace + "/" + pod.Name
		// Ignore pods which were scheduled to nodes before we started
		// scheduling.
		if pod.GetCreationTimestamp().Unix() > gravity.startTime {
			cpuAdder := fakeNodeInfo.Requested.MilliCPU
			if cpuAdder < 100 {
				// if the pod is claiming very low CPU requests, reserve a reasonable amount
				// until it shows us what it really needs.
				cpuAdder = 500
			}
			memoryAdder := fakeNodeInfo.NonZeroRequested.Memory
			klog.V(6).InfoS("Pod scheduled to node; supplying metrics adder", "nodeName", pod.Spec.NodeName, "pod", podName, "cpuAdder", cpuAdder, "memoryAdder", memoryAdder)
			gravity.freshMetricsMap.AddAdder(pod.Spec.NodeName, cpuAdder, memoryAdder)
		}
	}
}

func (gravity *Gravity) OnAdd(obj interface{}) {
	pod := obj.(*v1.Pod)
	if len(pod.Spec.NodeName) != 0 {
		gravity.onPodAssign(pod)
	}
}

func (gravity *Gravity) OnUpdate(oldObj, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	if oldPod.Spec.NodeName != newPod.Spec.NodeName {
		gravity.onPodAssign(newPod)
	}
}

func (gravity *Gravity) OnDelete(_ interface{}) {
}

func (gravity *Gravity) Filter(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	nodeName := nodeInfo.Node().Name
	podName := pod.Namespace + "/" + pod.Name

	// It is important to note here that Score() is not called if there is only one feasible node.
	// This is why we must do this check in Filter().
	fits, err := gravity.doesPodFitNode(ctx, pod, nodeInfo, 10)

	if err != nil {
		_ = gravity.setNodeTaint(ctx, nodeName, true)
		klog.Errorf("Unable to check node %v metrics: %v", nodeName, err)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Unable to check node %v metrics: %v", nodeName, err))
	}
	if !fits {
		klog.V(6).InfoS("Filter() will NOT consider node with utilization", "nodeName", nodeName, "pod", podName)
		_ = gravity.setNodeTaint(ctx, nodeName, true)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Node %v is heavily utilized", nodeName))
	}

	klog.V(6).InfoS("Filter() WILL consider node", "nodeName", nodeName, "pod", podName)
	return nil
}

func (gravity *Gravity) Name() string {
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

func (gravity *Gravity) doesPodFitNode(ctx context.Context, pod *v1.Pod, nodeInfo *framework.NodeInfo, noOlderThan int64) (bool, error) {
	incomingPodName := pod.ObjectMeta.Namespace + "/" + pod.Name
	nodeName := nodeInfo.Node().Name

	// Construct a fake node to calculate requests
	fakeNodeInfo := framework.NewNodeInfo(pod)
	incomingRequestCPUMillis := fakeNodeInfo.Requested.MilliCPU
	incomingRequestMemory := fakeNodeInfo.Requested.Memory // bytes

	klog.V(6).InfoS("Requests for incoming pod", "podName", incomingPodName, "millis", incomingRequestCPUMillis)

	nodeCPUMillisUsed, nodeMemoryUsed, err := gravity.fetchNodeMetrics(ctx, nodeName, noOlderThan)
	if err != nil {
		klog.ErrorS(nil, "CPU & memory metric not found for node", "nodeName")
		_ = gravity.setNodeTaint(ctx, nodeName, true)
		return false, err
	}

	nodeAllocatableCPUMillis := nodeInfo.Allocatable.MilliCPU
	nodeAllocatableMemory := nodeInfo.Allocatable.Memory

	inUseCPUPercent := int((100 * nodeCPUMillisUsed) / nodeAllocatableCPUMillis)
	inUseMemoryPercent := int((100 * nodeMemoryUsed) / nodeAllocatableMemory)

	klog.V(6).InfoS("Measured current node utilization on node for incoming pod",
		"nodeName", nodeName,
		"inUseCPU%", inUseCPUPercent,
		"inUseMemory%", inUseMemoryPercent,
		"podName", incomingPodName,
	)

	scheduledRequestedCPUMillis := nodeInfo.Requested.MilliCPU
	scheduledRequestedMemory := nodeInfo.Requested.Memory
	podCount := len(nodeInfo.Pods)

	klog.V(6).InfoS("Existing requests for node",
		"nodeName", nodeName,
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
		if ((100*incomingRequestPercent)/hostTargetUtilizationPercent > 80) && predictedCPUUsage < 100 {
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

func (gravity *Gravity) Score(_ context.Context, _ *framework.CycleState, _ *v1.Pod, nodeName string) (int64, *framework.Status) {
	// Filter() should have done all the calculations necessary to eliminate nodes
	// this pod will NOT fit. So everything remaining gets max score.
	var score = framework.MaxNodeScore
	klog.V(6).InfoS("Score for host", "nodeName", nodeName, "score", score)
	return score, framework.NewStatus(framework.Success, "")
}

type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value bool   `json:"value"`
}

func (gravity *Gravity) setNodeCordon(ctx context.Context, nodeName string, doCordon bool) error {
	payload := []patchStringValue{{
		Op:    "replace",
		Path:  "/spec/unschedulable",
		Value: doCordon,
	}}
	payloadBytes, _ := json.Marshal(payload)
	_, err := gravity.k8sClient.CoreV1().Nodes().Patch(ctx, nodeName, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return err
}

func (gravity *Gravity) setNodeTaint(ctx context.Context, nodeName string, doTaint bool) error {

	node, err := gravity.k8sClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Unable to get node %v to taint it %v: %v", nodeName, doTaint, err)
		return err
	}

	if doTaint {
		err := controller.AddOrUpdateTaintOnNode(gravity.k8sClient, nodeName, &SchedulerTaint)
		if err != nil {
			klog.Errorf("Unable to apply taint %v to %v: %v", SchedulerTaintName, nodeName, err)
		}
		// We also cordon to convince the autoscaler to add new nodes
		_ = gravity.setNodeCordon(ctx, nodeName, true)
		if err != nil {
			klog.Errorf("Unable to apply cordon %v: %v", nodeName, err)
		}
	} else {
		err := controller.RemoveTaintOffNode(gravity.k8sClient, nodeName, node, &SchedulerTaint)
		if err != nil {
			klog.Errorf("Unable to remove taint %v to %v: %v", SchedulerTaintName, nodeName, err)
		}
		_ = gravity.setNodeCordon(ctx, nodeName, false)
		if err != nil {
			klog.Errorf("Unable to remove cordon %v: %v", nodeName, err)
		}
	}

	klog.V(6).InfoS("Successfully changed taint of node", "nodeName", nodeName, "taint", SchedulerTaintName, "doTaint", doTaint)
	return nil
}

func (gravity *Gravity) ScoreExtensions() framework.ScoreExtensions {
	return gravity
}

func (gravity *Gravity) _fetchLiveHostMetrics(ctx context.Context, nodeName string) (int64, int64, error) {
	nodeMetrics, err := gravity.metricsClient.MetricsV1beta1().NodeMetricses().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("unable to read latest node metrics: %v", err)
	}
	return nodeMetrics.Usage.Cpu().MilliValue(), nodeMetrics.Usage.Memory().Value(), nil
}

// fetchNodeMetrics Retrieves fresh but not immediate API results from node.
func (gravity *Gravity) fetchNodeMetrics(ctx context.Context, nodeName string, noOlderThan int64) (int64, int64, error) {
	metricsSample, err := gravity.freshMetricsMap.GetOrPut(nodeName, noOlderThan,
		func() (int64, int64, error) {
			cpuMillis, memory, err := gravity._fetchLiveHostMetrics(ctx, nodeName)
			if err != nil {
				return 0, 0, fmt.Errorf("unable to refresh latest node metrics: %v", err)
			}
			return cpuMillis, memory, nil
		},
	)
	if err != nil {
		return 0, 0, err
	}
	return metricsSample.CpuMillis, metricsSample.Memory, nil
}

func (gravity *Gravity) fetchNodeMetricPercentages(ctx context.Context, nodeName string, noOlderThan int64) (int, int, error) {
	nodeInfo, err := gravity.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)

	if err != nil {
		return 0, 0, fmt.Errorf("error getting node %q from Snapshot: %v", nodeName, err)
	}

	cpuMillis, memory, err := gravity.fetchNodeMetrics(ctx, nodeName, noOlderThan)
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

func (gravity *Gravity) NormalizeScore(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
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
				nodeInfo, err := gravity.handle.SnapshotSharedLister().NodeInfos().Get(nodeScore.Name)

				if err == nil {
					var fits bool
					fits, err = gravity.doesPodFitNode(ctx, pod, nodeInfo, 1)
					if err == nil {
						if !fits {
							klog.V(6).InfoS("Final node utilization check found node is overutilized; eliminating from consideration", "targetNode", nodeScore.Name, "pod", podName)
						} else {
							klog.V(6).InfoS("Found first underutilized node; other nodes scores will be eliminated", "targetNode", nodeScore.Name, "score", nodeScore.Score)
							targetNodeFound = true
						}
					}
				}

				if err != nil {
					klog.Errorf("Final node utilization failed for pod %v on %v; will not assign: %v", podName, nodeScore.Name, err)
				}

				if !targetNodeFound {
					_ = gravity.setNodeTaint(ctx, nodeScore.Name, true)
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

// Checks and returns true if the pod is assigned to a node
func isAssigned(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}
