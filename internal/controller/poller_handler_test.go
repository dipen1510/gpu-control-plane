package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	gpuv1alpha1 "github.com/dipen1510/gpu-control-plane/api/v1alpha1"
	"github.com/dipen1510/gpu-control-plane/internal/controller/poller"
)

func newQueue(t *testing.T) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	return q
}

// TestPollerAndInformerShareOneKey: a poller event and an informer event for
// the same instance must produce one identical key, so they dedupe and only
// one worker ever reconciles that instance at a time.
func TestPollerAndInformerShareOneKey(t *testing.T) {
	const id, ns = "inst_ab", DefaultInstanceNamespace
	cr := &gpuv1alpha1.GPUInstance{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: gpuv1alpha1.K8sName(id),
		Labels: map[string]string{gpuv1alpha1.LabelInstanceID: id},
	}}
	q := newQueue(t)

	// For(&GPUInstance{}) uses EnqueueRequestForObject.
	informer := &handler.EnqueueRequestForObject{}
	informer.Create(t.Context(), event.CreateEvent{Object: client.Object(cr)}, q)
	pollerHandler(ns).Generic(t.Context(), poller.Event{Object: id}, q)
	pollerHandler(ns).Generic(t.Context(), poller.Event{Object: id}, q)

	if q.Len() != 1 {
		t.Fatalf("queue len = %d; want 1 (poller and informer keys differ)", q.Len())
	}
	got, _ := q.Get()
	want := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "inst-ab"}}
	if got != want {
		t.Fatalf("key = %v; want %v", got, want)
	}
	if gotID, ok := instanceIDFromCR(cr); !ok || gotID != id {
		t.Fatalf("instanceIDFromCR = %q, %v; want %q", gotID, ok, id)
	}
}

func TestDefaultInstanceNamespace(t *testing.T) {
	if got := (Options{}).withDefaults().InstanceNamespace; got != "gpucp" {
		t.Fatalf("default namespace = %q; want gpucp", got)
	}
}
