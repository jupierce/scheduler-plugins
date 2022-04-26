/*
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
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/util/taints"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
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
)

const (
	Name               = "Gravity"
	SchedulerTaintName = "gravity-scheduler.openshift.io/overloaded"

	// OvercommitCPURequestsAnnotationName If the webhook is turned on and this annotation == "true" in the Pod, its
	// requested resources will be cleared and will only be used as adders to the node. The adders will be
	// counted as consumed resources on the node for AdderTTL seconds. After that time, the pod should be
	// actually consuming the CPU it will require (i.e. CPU metrics from the node should reflect the pods
	// activity accurately).
	OvercommitCPURequestsAnnotationName = "gravity-scheduler.openshift.io/overcommit-cpu-requests"

	// OvercommitCPUHintAnnotationName Used by the webhook to squirrel away the original CPU requested by the pod.
	// This will be used as the adder for the Pod.
	OvercommitCPUHintAnnotationName = "gravity-scheduler.openshift.io/original-cpu-requests"

	// TapOutCPUPercent is a level of CPU utilization the scheduler considers extreme for edge case
	// decision-making purposes.
	TapOutCPUPercent = 94
	TapOutMemoryPercent = 90
)

var (
	DefaultTargetUtilizationPercent = int64(60)
	DefaultMaximumMemoryUtilization = int64(60)
	DefaultAdderTTL = int64(120)
	DefaultMaximumPodsPerNode = int64(130)
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
	decoder      *admission.Decoder
	pluginConfig *pluginConfig.GravityArgs
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

	args, err := getSchedulerPluginConfig(obj)
	if err != nil {
		return nil, err
	}

	gravity := &Gravity{
		handle:          handle,
		k8sClient:       handle.ClientSet(),
		metricsClient:   metricsClient,
		freshMetricsMap: NewFreshMetricsMap(0, 220, 60),
		pluginConfig:    args,
	}

	if gravity.pluginConfig.Webhook.Port != 0 {
		gravity.initWebhook(context.TODO())
	}

	gravity.startTime = time.Now().Unix()

	go func() {
		ctx := context.TODO()
		for {
			err := gravity.refreshAllNodeMetrics(ctx)
			if err != nil {
				klog.Errorf("Error refreshing node metrics: %v", err)
				continue
			}
			time.Sleep(2 * time.Second)
		}
	}()

	go func() {
		ctx := context.TODO()
		removeTaintCheck := time.NewTicker(time.Minute * 1)
		for range removeTaintCheck.C {

			// If the scheduler is restarted, we want to find all tainted nodes and not rely
			// on any internal state.
			nodeList, err := gravity.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				klog.Errorf("Unable to retrieve node list for cluster during untainting check: %v", err)
				continue
			}

			taintedNodes := make([]*v1.Node, 0)
			for i := range nodeList.Items {
				node := &nodeList.Items[i]
				if taints.TaintExists(node.Spec.Taints, &SchedulerTaint) {
					taintedNodes = append(taintedNodes, &nodeList.Items[i])
				}
			}

			for _, node := range taintedNodes {
				nodeName := node.Name
				untaintTarget := int(float64(gravity.pluginConfig.TargetUtilization) * 0.8)
				cpuUtilPercent, memoryUtilPercent, err := gravity.fetchNodeMetricPercentages(ctx, node, 30)
				if err == nil {
					klog.V(6).InfoS("During tainting check node utilization", "nodeName", nodeName, "cpu", cpuUtilPercent, "memory", memoryUtilPercent, "cpuTarget%", untaintTarget)
					if cpuUtilPercent < untaintTarget {
						_ = gravity.setNodeTaint(ctx, node.Name, false)
					}
				} else {
					klog.Errorf("Unable to retrieve node %v metrics for tainting check: %v", nodeName, err)
				}
			}

		}
	}()

	return gravity, nil
}

func (gravity *Gravity) getPodAdder(pod *v1.Pod) (int64, int64) {
	if pod != nil {
		// Ignore pods which were scheduled to nodes before we started
		// scheduling.
		if pod.GetCreationTimestamp().Unix() > gravity.startTime {
			fakeNodeInfo := framework.NewNodeInfo(pod)

			if pod.Annotations != nil {
				if stringVal, ok := pod.Annotations[OvercommitCPUHintAnnotationName]; ok {
					val, err := strconv.ParseInt(stringVal, 10, 64)
					if err != nil {
						klog.Errorf("unable to parse %v annotation in pod %v: %v", OvercommitCPUHintAnnotationName, pod.Name, err)
						val = 500 // Give the pod something to work with
					}
					fakeNodeInfo.Requested.MilliCPU += val
				}
			}
			// Even if this Pod is not using overcommit mode, we still establish a temporary adder
			// based on requested CPU & memory. This helps give Pods to spin up and use the resources
			// the claimed to before measurements. Ultimately, measuring pods that have spun up
			// will help us get a better metric to assess whether we are > target utilization.
			cpuAdder := fakeNodeInfo.NonZeroRequested.MilliCPU
			memoryAdder := fakeNodeInfo.NonZeroRequested.Memory
			return cpuAdder, memoryAdder
		}
	}
	return 0, 0
}

func (gravity *Gravity) Filter(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	nodeName := nodeInfo.Node().Name
	podName := pod.Namespace + "/" + pod.Name
	// It is important to note here that Score() is not called if there is only one feasible node.
	// This is why we must do this check in Filter().
	fits, err := gravity.doesPodFitNode(ctx, pod, nodeInfo, 10)

	if err != nil {
		// Don't taint. doesPodFitNode will have done this for an error, when appropriate.
		klog.Errorf("Unable to check pod %v node fit %v: %v", podName, nodeName, err)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Unable to check node %v metrics: %v", nodeName, err))
	}

	if !fits {
		klog.V(6).InfoS("Filter() will NOT consider node with utilization", "nodeName", nodeName, "pod", podName)
		_ = gravity.setNodeTaint(ctx, nodeName, true)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Node %v is heavily utilized", nodeName))
	}

	// Unfortunately, if there is only one node, other lifecycle methods like Score() and NormalizeScore()
	// will not be called. So Filter() has to assume that there may only be one node and that Filter() is the final
	// decider. Thus, add the pod Adder now for all nodes and then remove it from unselected nodes
	// once, IF Score() is called.
	cpuAdder, memoryAdder := gravity.getPodAdder(pod)
	gravity.freshMetricsMap.AddAdder(nodeName, cpuAdder, memoryAdder)
	klog.V(6).InfoS("Filter() WILL consider node", "nodeName", nodeName, "pod", podName, "cpuAdder", cpuAdder, "memoryAdder", memoryAdder)
	return nil
}

func (gravity *Gravity) Name() string {
	return Name
}

func getSchedulerPluginConfig(obj runtime.Object) (*pluginConfig.GravityArgs, error) {
	args, ok := obj.(*pluginConfig.GravityArgs)
	if !ok {
		return nil, fmt.Errorf("args must be of type GravityArgs, got %T", obj)
	}

	if args.TargetUtilization == 0 {
		args.TargetUtilization = DefaultTargetUtilizationPercent
	}

	if args.MaximumMemoryUtilization == 0 {
		args.MaximumMemoryUtilization = DefaultMaximumMemoryUtilization
	}

	if args.AdderTTL == 0 {
		args.AdderTTL = DefaultAdderTTL
	}

	if args.CPUOvercommitEnabled  {
		if args.Webhook.Port == 0 {
			return nil, fmt.Errorf("webhook configuration must be provided to use CPUOvercommitEnabled")
		}
	}

	if args.MaximumPodsPerNode == 0 {
		args.MaximumPodsPerNode = DefaultMaximumPodsPerNode
	}

	return args, nil
}

func (gravity *Gravity) doesPodFitNode(ctx context.Context, pod *v1.Pod, nodeInfo *framework.NodeInfo, noOlderThan int64) (bool, error) {

	// The default utilizations come from the scheduler's plugin config, but they can be overridden by labels.
	targetCPUUtilization := gravity.pluginConfig.TargetUtilization
	maximumMemoryUtilization := gravity.pluginConfig.MaximumMemoryUtilization

	incomingPodName := pod.ObjectMeta.Namespace + "/" + pod.Name
	nodeName := nodeInfo.Node().Name

	// Calculates node use if this pod landed on the node.
	predictedNodeInfo := framework.NewNodeInfo()
	// Approximates the load of an "empty" node by summing requirements of all daemonset pods
	nodeOverheadInfo := framework.NewNodeInfo()

	for _, podInfo := range nodeInfo.Pods {
		predictedNodeInfo.AddPod(podInfo.Pod)
		ownerReferences := podInfo.Pod.OwnerReferences
		if ownerReferences != nil {
			for i := range ownerReferences {
				reference := &ownerReferences[i]
				if reference.Kind == "DaemonSet" {
					nodeOverheadInfo.AddPod(podInfo.Pod)
				}
			}
		}
	}

	predictedNodeInfo.AddPod(pod) // finally, add the incoming pod to predicted node

	percentage := func(numerator int64, denominator int64) int64 {
		return (100 * numerator) / denominator
	}

	// On this node, how much is technically allocatable
	allocatableNodeCPUMillis := nodeInfo.Allocatable.MilliCPU
	allocatableNodeMemory := nodeInfo.Allocatable.MilliCPU

	// How much resource would a "fresh" node of this same type potentially provide
	usableTheoryNodeCPUMillis := allocatableNodeCPUMillis - nodeOverheadInfo.NonZeroRequested.MilliCPU
	usableTheoryNodeMemory := allocatableNodeMemory - nodeOverheadInfo.NonZeroRequested.Memory

	klog.V(6).InfoS("Calculated allocatable and theoretically usable resources for node",
		"nodeName", nodeName,
		"allocatableNodeCPUMillis", allocatableNodeCPUMillis,
		"allocatableNodeMemory", allocatableNodeMemory,
		"usableTheoryNodeCPUMillis", usableTheoryNodeCPUMillis,
		"usableTheoryNodeCPUMillis", usableTheoryNodeCPUMillis,
	)

	if usableTheoryNodeCPUMillis <= 0 || usableTheoryNodeMemory <= 0 {
		// Don't taint. Just let normal resource utilization scheduling take over.
		return false, fmt.Errorf("%v scheduler is unable to assess usable targets for node %v CPU and memory; deferring to normal schedulers", Name, nodeName)
	}

	// Construct a fake node to calculate the requests for the incoming pod
	fakeNodeInfo := framework.NewNodeInfo(pod)
	incomingPodRequestCPUMillis := fakeNodeInfo.NonZeroRequested.MilliCPU
	incomingPodRequestMemory := fakeNodeInfo.NonZeroRequested.Memory

	// Check for and defer scheduling of uncomfortably large pods relative to node allocatable
	if percentage(incomingPodRequestCPUMillis, usableTheoryNodeCPUMillis) > TapOutCPUPercent {
		klog.Warningf("Evaluated %v and pod %v CPU request %v greater too close to estimated usable CPU %v for nodes of this type; deferring to normal schedulers", nodeName, incomingPodName, incomingPodRequestCPUMillis, usableTheoryNodeCPUMillis)
		// Don't taint. Just let normal resource utilization scheduling take over.
		return false, fmt.Errorf("%v scheduler cannot help assign a pod using significant CPU relative to node size", Name)
	}
	if percentage(incomingPodRequestMemory, usableTheoryNodeMemory) > TapOutMemoryPercent {
		klog.Warningf("Evaluated %v and pod %v CPU request %v greater too close to estimated usable CPU %v for nodes of this type; deferring to normal schedulers", nodeName, incomingPodName, incomingPodRequestCPUMillis, usableTheoryNodeCPUMillis)
		// Don't taint. Just let normal resource utilization scheduling take over.
		return false, fmt.Errorf("%v scheduler cannot help assign a pod using significant memory relative to node size", Name)
	}

	nodeCPUMillisUseMetric, nodeMemoryUseMetric, err := gravity.fetchNodeMetrics(ctx, nodeName, noOlderThan)
	if err != nil {
		klog.ErrorS(nil, "CPU & memory metric not found for node", "nodeName")
		_ = gravity.setNodeTaint(ctx, nodeName, true)
		return false, fmt.Errorf("unable to retrieve node metrcs: %v", err)
	}

	inUseCPUPercent := percentage(nodeCPUMillisUseMetric, nodeInfo.Allocatable.MilliCPU)
	inUseMemoryPercent := percentage(nodeMemoryUseMetric, nodeInfo.Allocatable.Memory)

	klog.V(6).InfoS("Measured current node utilization for incoming pod",
		"nodeName", nodeName,
		"podName", incomingPodName,
		"inUseCPU%", inUseCPUPercent,
		"inUseMemory%", inUseMemoryPercent,
	)

	alreadyRequestedCPUMillis := nodeInfo.NonZeroRequested.MilliCPU
	alreadyRequestedMemory := nodeInfo.Requested.Memory
	podCount := len(nodeInfo.Pods)

	klog.V(6).InfoS("Existing requests for node",
		"nodeName", nodeName,
		"podName", incomingPodName,
		"alreadyRequestedCPUMillis", alreadyRequestedCPUMillis,
		"alreadyRequestedMemory", alreadyRequestedMemory,
		"podCount", podCount,
	)

	// At this point, we've verified that the incoming Pod could fit on a completely fresh node.
	// Now we should determine whether the Pod might be a good fit for this node.

	// First check on normal, resource request based fit. If this doesn't fit, nothing we do
	// would convince the kubelet to take the workload. In practice, this pod should have never
	// reached our scheduler for this node.
	if predictedNodeInfo.NonZeroRequested.MilliCPU > nodeInfo.Allocatable.MilliCPU || predictedNodeInfo.NonZeroRequested.Memory > nodeInfo.Allocatable.Memory {
		klog.V(6).InfoS("Node will NOT be prioritized because existing node requests plus incoming pod requests sum to greater than allocatable",
			"nodeName", nodeName,
			"podName", incomingPodName,
			"predictedRequestedCPU", predictedNodeInfo.NonZeroRequested.MilliCPU,
			"predictedRequestedMemory", predictedNodeInfo.NonZeroRequested.Memory,
		)
		return false, nil
	}

	// We now know the Pod could technically be scheduled. Now - do we want it there?

	// Not if there are too many pods
	if int64(podCount) > gravity.pluginConfig.MaximumPodsPerNode {
		klog.V(6).InfoS("Node will NOT be prioritized because pod count is high",
			"nodeName", nodeName,
			"podName", incomingPodName,
			"podCount", podCount)
		return false, nil
	}

	// We could naively check whether expected CPU utilization > targetCPUUtilization, but this will not suffice
	// for "big pods". We've already deferred huge pods (those which verge on unschedulable). Big pods are those
	// which might, by themselves, drive utilization greater than the target utilizations.
	cpuAdder, memoryAdder := gravity.getPodAdder(pod) // find requests in case pod is claiming

	// If we scheduled this pod, what to we think the actual consumed CPU & memory metrics might be
	predictionForCPUMillisMetric := nodeCPUMillisUseMetric + cpuAdder
	predictionForMemoryMetric := nodeMemoryUseMetric + memoryAdder

	if percentage(predictionForCPUMillisMetric, allocatableNodeCPUMillis) > targetCPUUtilization {
		// The pod would indeed drive us over the target utilization
		if percentage(cpuAdder, predictionForCPUMillisMetric - nodeOverheadInfo.NonZeroRequested.MilliCPU) >= 90 {
			klog.V(6).InfoS("Node CAN be prioritized because pod CPU request is large relative to node resources an would use most of the node",
				"nodeName", nodeName,
				"podName", incomingPodName,
				"requestedCPU", cpuAdder)
		} else {
			// The pod pushes over the target and is not at least 90% of the reason why. Let's wait for another node.
			klog.V(6).InfoS("Node will NOT be prioritized because new node CPU requests would exceed target",
				"nodeName", nodeName,
				"podName", incomingPodName,
				"requestedCPU", cpuAdder)
			return false, nil
		}
	} else if predictedNodeInfo.NonZeroRequested.MilliCPU > TapOutCPUPercent {
		// This node has most of its CPU spoken for. Let's leave some headroom.
		klog.V(6).InfoS("Node will NOT be prioritized because node CPU requests would approach 100%",
			"nodeName", nodeName,
			"podName", incomingPodName,
			"requestedCPU", cpuAdder)
		return false, nil
	}

	if percentage(predictionForMemoryMetric, allocatableNodeMemory) > maximumMemoryUtilization {
		// The pod would indeed drive us over the target memory utilization
		if percentage(memoryAdder, predictionForMemoryMetric - nodeOverheadInfo.NonZeroRequested.Memory) >= 90 {
			klog.V(6).InfoS("Node CAN be prioritized because pod is memory request is large relative to node resources and would use most of the node",
				"nodeName", nodeName,
				"podName", incomingPodName,
				"requestedMemory", memoryAdder)
		} else {
			// The pod pushes over the target and is not at least 90% of the reason why. Let's wait for another node.
			klog.V(6).InfoS("Node will NOT be prioritized because new node memory requests would exceed target",
				"nodeName", nodeName,
				"podName", incomingPodName,
				"requestedMemory", memoryAdder)
			return false, nil
		}
	} else if predictedNodeInfo.NonZeroRequested.Memory > TapOutMemoryPercent {
		// This node has most of its memory spoken for. Let's leave some headroom.
		klog.V(6).InfoS("Node will NOT be prioritized because node memory requests would approach 100%",
			"nodeName", nodeName,
			"podName", incomingPodName,
			"requestedMemory", memoryAdder)
		return false, nil
	}

	return true, nil
}
func (gravity *Gravity) Score(_ context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	// Filter() should have done all the calculations necessary to eliminate nodes
	// this pod will NOT fit. So everything remaining gets max score.
	var score = framework.MaxNodeScore
	klog.V(6).InfoS("Score for host", "nodeName", nodeName, "score", score)

	// If Score is called, there is more than one node up for consideration.
	// Remove the Adder that Filter() added since the framework will call NormalizeScore
	// next -- which does a final check on fit and does the addition for the final selected node.
	cpuAdder, memoryAdder := gravity.getPodAdder(pod)
	gravity.freshMetricsMap.AddAdder(nodeName, -1*cpuAdder, -1*memoryAdder)
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

	klog.V(6).InfoS("Changing taint of node", "nodeName", nodeName, "taint", SchedulerTaintName, "doTaint", doTaint)

	if doTaint {
		err := controller.AddOrUpdateTaintOnNode(gravity.k8sClient, nodeName, &SchedulerTaint)
		if err != nil {
			klog.Errorf("Unable to apply taint %v to %v: %v", SchedulerTaintName, nodeName, err)
		}
		// We also cordon to convince the autoscaler to add new nodes
		err = gravity.setNodeCordon(ctx, nodeName, true)
		if err != nil {
			klog.Errorf("Unable to apply cordon %v: %v", nodeName, err)
		}
	} else {
		err := controller.RemoveTaintOffNode(gravity.k8sClient, nodeName, node, &SchedulerTaint)
		if err != nil {
			klog.Errorf("Unable to remove taint %v to %v: %v", SchedulerTaintName, nodeName, err)
		}
		err = gravity.setNodeCordon(ctx, nodeName, false)
		if err != nil {
			klog.Errorf("Unable to remove cordon %v: %v", nodeName, err)
		}
	}

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

func (gravity *Gravity) refreshAllNodeMetrics(ctx context.Context) error {
	metrics, err := gravity.metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Unable to list all node metrics: %v", err)
		return err
	}
	for _, v := range metrics.Items {
		gravity.freshMetricsMap.Put(v.Name, v.Usage.Cpu().MilliValue(), v.Usage.Memory().Value())
	}
	return nil
}

func (gravity *Gravity) fetchNodeMetricPercentages(ctx context.Context, node *v1.Node, noOlderThan int64) (int, int, error) {
	nodeName := node.Name
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
							// A final update to the Adder. It would be really nice if Score/NormalizeScore were called
							// if there were more than 1 feasible, node, but that will take upstream changes.
							cpuAdder, memoryAdder := gravity.getPodAdder(pod)
							gravity.freshMetricsMap.AddAdder(nodeScore.Name, cpuAdder, memoryAdder)
							klog.V(6).InfoS("Found first underutilized node; other nodes scores will be eliminated from consideration", "targetNode", nodeScore.Name, "score", nodeScore.Score)
							targetNodeFound = true
						}
					} else {
						// An error from doesNodeFit means the scheduler does not feel in should be participating in a decision for this pod.
						// Clear all scores and defer to another scheduler.
						klog.Errorf("Error returned from node fit check. No scheduling score will be made by this scheduler.", podName, nodeScore.Name, err)
						for i := range scores {
							scores[i].Score = framework.MinNodeScore
						}
						return nil
					}
				} else {
					klog.Errorf("Unable to acquire nodeInfo for %v on %v; will not assign: %v", podName, nodeScore.Name, err)
					scoresMap[nodeScore.Name].Score = framework.MinNodeScore
				}

			} else {
				klog.V(6).InfoS("Found overutilized node while searching for target", "name", nodeScore.Name, "score", nodeScore.Score)
			}
		}
	}

	if !targetNodeFound {
		klog.V(6).InfoS("Pod could not fit any existing node, tainting all nodes to bring up fresh node", "podName", podName)
		// We tried to fit all nodes and failed. To make sure the cluster autoscale brings up a new node,
		// We have to cordon everything else.
		for _, nodeScore := range scores {
			_ = gravity.setNodeTaint(ctx, nodeScore.Name, true)
		}
	}

	for _, nodeScore := range scores {
		klog.V(6).InfoS("Scored node", "name", nodeScore.Name, "score", nodeScore.Score)
	}

	return nil
}
