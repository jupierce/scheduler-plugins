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

package config

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	// TODO: eliminate the "versioned" import, i.e., schedulerconfig.ResourceSpec should be unversioned.ResourceSpec.
	schedulerconfig "k8s.io/kube-scheduler/config/v1"
	unversioned "k8s.io/kubernetes/pkg/scheduler/apis/config"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CoschedulingArgs defines the parameters for Coscheduling plugin.
type CoschedulingArgs struct {
	metav1.TypeMeta

	// PermitWaitingTime is the wait timeout in seconds.
	PermitWaitingTimeSeconds int64
	// DeniedPGExpirationTimeSeconds is the expiration time of the denied podgroup store.
	DeniedPGExpirationTimeSeconds int64
}

// ModeType is a "string" type.
type ModeType string

const (
	// Least is the string "Least".
	Least ModeType = "Least"
	// Most is the string "Most".
	Most ModeType = "Most"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourcesAllocatableArgs holds arguments used to configure NodeResourcesAllocatable plugin.
type NodeResourcesAllocatableArgs struct {
	metav1.TypeMeta `json:",inline"`

	// Resources to be considered when scoring.
	// Allowed weights start from 1.
	// An example resource set might include "cpu" (millicores) and "memory" (bytes)
	// with weights of 1<<20 and 1 respectfully. That would mean 1 MiB has equivalent
	// weight as 1 millicore.
	Resources []schedulerconfig.ResourceSpec `json:"resources,omitempty"`

	// Whether to prioritize nodes with least or most allocatable resources.
	Mode ModeType `json:"mode,omitempty"`
}

// MetricProviderType is a "string" type.
type MetricProviderType string

const (
	KubernetesMetricsServer MetricProviderType = "KubernetesMetricsServer"
	Prometheus              MetricProviderType = "Prometheus"
	SignalFx                MetricProviderType = "SignalFx"
)

// Denote the spec of the metric provider
type MetricProviderSpec struct {
	// Types of the metric provider
	Type MetricProviderType
	// The address of the metric provider
	Address string
	// The authentication token of the metric provider
	Token string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TargetLoadPackingArgs holds arguments used to configure TargetLoadPacking plugin.
type TargetLoadPackingArgs struct {
	metav1.TypeMeta

	// Default requests to use for best effort QoS
	DefaultRequests v1.ResourceList
	// Default requests multiplier for busrtable QoS
	DefaultRequestsMultiplier string
	// Node target CPU Utilization for bin packing
	TargetUtilization int64
	// Metric Provider to use when using load watcher as a library
	MetricProvider MetricProviderSpec
	// Address of load watcher service
	WatcherAddress string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LoadVariationRiskBalancingArgs holds arguments used to configure LoadVariationRiskBalancing plugin.
type LoadVariationRiskBalancingArgs struct {
	metav1.TypeMeta

	// Metric Provider to use when using load watcher as a library
	MetricProvider MetricProviderSpec
	// Address of load watcher service
	WatcherAddress string
	// Multiplier of standard deviation in risk value
	SafeVarianceMargin float64
	// Root power of standard deviation in risk value
	SafeVarianceSensitivity float64
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GravityArgs holds plugin arguments used to configure Gravity scheduler plugin.
type GravityArgs struct {
	metav1.TypeMeta

	// If multiple instances of the scheduler are running,
	// the instance name will differentiate them (e.g.
	// the name of the taints they use). If multiple instances
	// are to be used, pods scheduled with each instance MUST
	// target different nodes via taints or nodeSelectors.
	// Each instance will assume it has full management
	// authority over the feasible nodes found for the pods
	// it handles.
	InstanceName string

	// Node target CPU Utilization for bin packing
	TargetUtilization int64

	// When a pod is scheduled to a node, its impact on the CPU & memory
	// will not be immediate. Gravity will temporarily consider Pod
	// container requests as part of measured resource usage on the Node.
	// These requests will be considered for this time period (seconds)
	// before the Pod must actually start using those resources.
	// For example, if TargetUtilization is 50% and a Pod requests
	// 60% of the CPU, the scheduler will not consider adding more
	// Pods to the node for AdderTTL seconds. After the adder expires
	// the scheduler will only be looking at real utilization for the
	// Pod. If it is consuming < TargetUtilization, more Pods may be
	// scheduled to the Node.
	// In short, how long should the scheduler wait for the Pod to
	// start consuming the resources it claims to? Defaults to
	// 60 seconds.
	AdderTTL int64

	// Set >1 if workloads are underreporting their CPU utilization
	CPURequestMultiplier float64

	// Maximum memory utilization
	MaximumMemoryUtilization int64

	MaximumPodsPerNode int64

	// If the webhook should modify pods to support Gravity's CPU overcommit functionality
	CPUOvercommitEnabled bool

	Webhook GravityWebhookConfig
}

type GravityWebhookConfig struct {
	// Port is the port number that the server will serve.
	// It will be defaulted to 9443 if unspecified.
	Port int64

	// CertDir is the directory that contains the server key and certificate. The
	// server key and certificate.
	CertDir string

	// CertName is the server certificate name. Defaults to tls.crt.
	CertName string

	// KeyName is the server key name. Defaults to tls.key.
	KeyName string

}

// ScoringStrategyType is a "string" type.
type ScoringStrategyType string

const (
	// MostAllocated strategy favors node with the least amount of available resource
	MostAllocated ScoringStrategyType = "MostAllocated"
	// BalancedAllocation strategy favors nodes with balanced resource usage rate
	BalancedAllocation ScoringStrategyType = "BalancedAllocation"
	// LeastAllocated strategy favors node with the most amount of available resource
	LeastAllocated ScoringStrategyType = "LeastAllocated"
)

// ScoringStrategy define ScoringStrategyType for node resource topology plugin
type ScoringStrategy struct {
	// Type selects which strategy to run.
	Type ScoringStrategyType

	// Resources a list of pairs <resource, weight> to be considered while scoring
	// allowed weights start from 1.
	Resources []schedulerconfig.ResourceSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourceTopologyMatchArgs holds arguments used to configure the NodeResourceTopologyMatch plugin
type NodeResourceTopologyMatchArgs struct {
	metav1.TypeMeta

	// ScoringStrategy a scoring model that determine how the plugin will score the nodes.
	ScoringStrategy ScoringStrategy
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PreemptionTolerationArgs reuses DefaultPluginArgs.
type PreemptionTolerationArgs unversioned.DefaultPreemptionArgs
