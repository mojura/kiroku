package kiroku

import (
	"sync"
	"time"
)

func newBatcher(batchDuration time.Duration, createTxn func(TransactionFn) error) *batcher {
	var b batcher
	b.batchDuration = batchDuration
	b.createTxn = createTxn
	return &b
}

type batcher struct {
	mux sync.RWMutex

	batchDuration time.Duration

	createTxn func(fn TransactionFn) error

	txn *Transaction
}

func (b *batcher) Batch(fn BatchFn) (err error) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if b.txn != nil {
		fn(b.txn)
		return
	}

	if err = b.create(); err != nil {
		return
	}

	fn(b.txn)
	return
}

func (b *batcher) create() (err error) {
	ch := make(chan struct{})
	errCh := make(chan error)
	defer close(ch)
	go func() {
		errCh <- b.createTxn(func(txn *Transaction) (err error) {
			b.txn = txn
			ch <- struct{}{}
			return b.completeTransaction(txn)
		})
	}()

	select {
	case <-ch:
	case err = <-errCh:
	}

	return
}

func (b *batcher) completeTransaction(txn *Transaction) (err error) {
	// Keep transaction open for the batch duration
	time.Sleep(b.batchDuration)
	// Attempt to acquire lock before completing the transaction
	b.mux.Lock()
	defer b.mux.Unlock()
	// De-reference transaction
	b.txn = nil
	return
}

type BatchFn func(*Transaction)
