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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strconv"
)

func (gravity *Gravity) initWebhook(ctx context.Context) {
	mgr, err := manager.New(gravity.handle.KubeConfig(), manager.Options{})

	if err != nil {
		panic(err)
	}
	config := gravity.config

	hookServer := &webhook.Server{
		Port: int(config.Webhook.Port),
	}

	if len(config.Webhook.CertDir) == 0 {
		hookServer.CertDir = config.Webhook.CertDir
	}

	if len(config.Webhook.KeyName) == 0 {
		hookServer.KeyName = config.Webhook.KeyName
	}

	if len(config.Webhook.CertName) == 0 {
		hookServer.CertName = config.Webhook.CertName
	}

	if err := mgr.Add(hookServer); err != nil {
		panic(err)
	}

	// Register the webhooks in the server.
	hookServer.Register("/mutating", &webhook.Admission{Handler: gravity})
	if err := hookServer.StartStandalone(ctx, scheme.Scheme); err != nil {
		panic(err)
	}

}

func (gravity *Gravity) shouldAugment(pod *corev1.Pod) bool {
	if pod.Annotations != nil {
		if v, ok := pod.Annotations[OvercommitCPURequestsAnnotationName]; ok {
			if b, err := strconv.ParseBool(v); err == nil && b {
				return true
			}
		}
	}
	return false
}

func (gravity *Gravity) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}

	err := gravity.decoder.Decode(req, pod)
	if err != nil {
		klog.Errorf("Error decoding pod: %v", err)
		return admission.Errored(http.StatusBadRequest, err)
	}

	if gravity.shouldAugment(pod) {
		nodeInfo := framework.NewNodeInfo(pod)
		pod.Annotations[OvercommitCPUHintAnnotationName] = strconv.FormatInt(nodeInfo.NonZeroRequested.MilliCPU, 10)

		reduceRequests := func(container *corev1.Container) {
			requests := &container.Resources.Requests
			if requests.Cpu() != nil {
				requests.Cpu().Set(0)
			}

			// Limits must be cleared as well, otherwise they will be used for Requests
			limits := &container.Resources.Limits
			if limits.Cpu() != nil {
				limits.Cpu().Set(0)
			}
		}

		if pod.Spec.InitContainers != nil {
			for i, _ := range pod.Spec.InitContainers {
				reduceRequests(&pod.Spec.InitContainers[i])
			}
		}

		for i, _ := range pod.Spec.Containers {
			reduceRequests(&pod.Spec.Containers[i])
		}

	}

	marshaledPod, err := json.Marshal(pod)

	if err != nil {
		klog.Errorf("Error marshaling pod: %v", err)
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// InjectDecoder injects the decoder.
func (gravity *Gravity) InjectDecoder(d *admission.Decoder) error {
	gravity.decoder = d
	return nil
}
