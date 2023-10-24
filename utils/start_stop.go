package utils

import (
	"context"
	"errors"
	"time"
)

type StartStop struct {
	ctx          context.Context
	cancel       context.CancelFunc
	doneCh       chan struct{}
	blockingFunc func(context.Context) error
}

func NewStartStop(ctx context.Context, blockingFunc func(context.Context) error) *StartStop {
	cancelableCtx, cancel := context.WithCancel(ctx)

	return &StartStop{
		ctx:          cancelableCtx,
		cancel:       cancel,
		doneCh:       make(chan struct{}),
		blockingFunc: blockingFunc,
	}
}

func (h *StartStop) Start() error {
	defer func() {
		h.doneCh <- struct{}{}
	}()
	return h.blockingFunc(h.ctx)
}

func (h *StartStop) Stop(timeout time.Duration) error {
	h.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return errors.New("failed to finished within timeout")
	case <-h.doneCh:
		return nil
	}
}
