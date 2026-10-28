package batch

import (
	"testing"
	"time"
)

type heldBatch struct{ entered, release, done chan struct{} }
type heldBatchHandler struct{ *BaseReqHandler }

func (h *heldBatchHandler) Process(_ int64, requests []interface{}, _ string) {
	batch := requests[0].(*heldBatch)
	defer close(batch.done)
	defer h.activeRunnableNum.Dec()
	close(batch.entered)
	<-batch.release
}
func TestStopDoesNotLoseInflightActivityAcrossRestart(t *testing.T) {
	q := NewReqQueue(4)
	h := &heldBatchHandler{NewBaseReqHandler(1, 1, 1, 1, q, "process", "retrieve")}
	old := &heldBatch{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	current := &heldBatch{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("batch did not reach synchronization point")
		}
	}
	q.SubmitRequest(old)
	go h.AsyncHandleReqs(h)
	await(old.entered)
	h.Stop()
	q.SubmitRequest(current)
	go h.AsyncHandleReqs(h)
	await(current.entered)
	close(old.release)
	await(old.done)
	active := h.IsActive()
	close(current.release)
	await(current.done)
	if !active {
		t.Fatal("old completion made current in-flight batch appear idle")
	}
	if h.IsActive() {
		t.Fatal("finished batches left activity registered")
	}
}
