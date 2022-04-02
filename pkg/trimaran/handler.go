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
Package Trimaran provides common code for plugins developed for real load aware scheduling like TargetLoadPacking etc.
*/

package trimaran

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// This is the maximum staleness of metrics possible by load watcher
	cacheCleanupIntervalMinutes = 5
	// Time interval in seconds for each metrics agent ingestion.
	metricsAgentReportingIntervalSeconds = 5 * 60
)

var _ clientcache.ResourceEventHandler = &PodAssignEventHandler{}

// This event handler watches assigned Pod and caches them locally
type PodAssignEventHandler struct {
	// Maintains the node-name to podInfo mapping for pods successfully bound to nodes
	ScheduledPodsCache map[string]map[string]podInfo
	sync.RWMutex
}

// Stores Timestamp and Pod spec info object
type podInfo struct {
	// This timestamp is initialised when adding it to ScheduledPodsCache after successful binding
	Timestamp time.Time
	Pod       *v1.Pod
}

// Returns a new instance of PodAssignEventHandler, after starting a background go routine for cache cleanup
func New() *PodAssignEventHandler {
	p := PodAssignEventHandler{
		ScheduledPodsCache: make(map[string]map[string]podInfo),
	}
	go func() {
		cacheCleanerTicker := time.NewTicker(time.Minute * cacheCleanupIntervalMinutes)
		for range cacheCleanerTicker.C {
			p.cleanupCache()
		}
	}()
	return &p
}

func (p *PodAssignEventHandler) OnAdd(obj interface{}) {
	pod := obj.(*v1.Pod)
	klog.V(6).InfoS("Adding pod", "pod", klog.KObj(pod))
	p.updateCache(pod)
}

func (p *PodAssignEventHandler) OnUpdate(oldObj, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	klog.V(6).InfoS("Updating pod", "pod", klog.KObj(newPod))

	if oldPod.Spec.NodeName != newPod.Spec.NodeName {
		p.removePodInfo(oldPod)
	}
	p.setPodInfo(newPod)
}

func (p *PodAssignEventHandler) OnDelete(obj interface{}) {
	pod := obj.(*v1.Pod)
	klog.V(6).InfoS("Deleting pod", "pod", klog.KObj(pod))
	p.removePodInfo(pod)
}

// Must lock before accessing.
// Returns existing or creates empty map for node podInfos
func (p *PodAssignEventHandler) getNodeMap(nodeName string) map[string]podInfo {
	if _, ok := p.ScheduledPodsCache[nodeName]; !ok {
		p.ScheduledPodsCache[nodeName] = make(map[string]podInfo)
	}
	return p.ScheduledPodsCache[nodeName]
}

func (p *PodAssignEventHandler) podKey(pod *v1.Pod) string {
	return string(pod.UID) + "/" + pod.Namespace + "/" + pod.Name
}

// Adds or updates pod information for node.
func (p *PodAssignEventHandler) setPodInfo(pod *v1.Pod) {
	p.Lock()
	defer p.Unlock()
	nodeMap := p.getNodeMap(pod.Spec.NodeName)
	podKey := p.podKey(pod)
	nodeMap[podKey] = podInfo{Timestamp: time.Now(), Pod: pod}
}

// Must lock before accessing.
// Removes a pod from a node
func (p *PodAssignEventHandler) removePodInfo(pod *v1.Pod) {
	p.Lock()
	defer p.Unlock()
	nodeMap := p.getNodeMap(pod.Spec.NodeName)
	podKey := p.podKey(pod)
	delete(nodeMap, podKey)
}


func (p *PodAssignEventHandler) updateCache(pod *v1.Pod) {
	p.setPodInfo(pod)
}

// To with regular updates from informer, we can check whether
// pods are getting old and remove them if we haven't gotten
// an update in awhile.
func (p *PodAssignEventHandler) cleanupCache() {
	p.Lock()
	defer p.Unlock()
	deadNodes := make([]string, 0)
	for nodeName := range p.ScheduledPodsCache {
		cache := p.ScheduledPodsCache[nodeName]
		if len(cache) == 0 {
			deadNodes = append(deadNodes, nodeName)
		}
	}
	for _, nodeName := range deadNodes {
		klog.V(6).InfoS("Removing node", "nodeName", nodeName)
		delete(p.ScheduledPodsCache, nodeName)
	}
}
