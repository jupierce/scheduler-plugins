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
		Port: config.WebHook.Port,
	}

	if len(config.WebHook.CertDir) == 0 {
		hookServer.CertDir = config.WebHook.CertDir
	}

	if len(config.WebHook.KeyName) == 0 {
		hookServer.KeyName = config.WebHook.KeyName
	}

	if len(config.WebHook.CertName) == 0 {
		hookServer.CertName = config.WebHook.CertName
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
		if v, ok := pod.Annotations[OvercommitRequestsAnnotationName]; ok {
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
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}

		nodeInfo := framework.NewNodeInfo(pod)
		pod.Annotations[OvercommitCPUHintAnnotationName] = strconv.FormatInt(nodeInfo.NonZeroRequested.MilliCPU, 10)
		pod.Annotations[OvercommitMemoryHintAnnotationName] = strconv.FormatInt(nodeInfo.NonZeroRequested.Memory, 10)

		reduceRequests := func(container *corev1.Container) {
			requests := &container.Resources.Requests
			if requests.Cpu() != nil {
				requests.Cpu().Set(1)
			}

			// Limits must be cleared as well, otherwise they will be used for Requests
			limits := &container.Resources.Limits
			if limits.Cpu() != nil {
				limits.Cpu().Set(1)
			}
		}

		if pod.Spec.InitContainers != nil {
			for i, _ := range pod.Spec.InitContainers {
				reduceRequests(&pod.Spec.InitContainers[i])
			}
			for i, _ := range pod.Spec.Containers {
				reduceRequests(&pod.Spec.InitContainers[i])
			}
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
