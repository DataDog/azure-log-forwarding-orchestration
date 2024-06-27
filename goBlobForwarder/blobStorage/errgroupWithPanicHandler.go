package blobStorage

import (
	"context"
	"log"

	"golang.org/x/sync/errgroup"
)

// Rerun if changes are made to the interface
//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

type ErrGroup interface {
	Go(f func() error)
	Wait() error
	SetLimit(int)
}

type ErrGroupPanicHandler struct {
	Group ErrGroup
}

var _ ErrGroup = (*ErrGroupPanicHandler)(nil)

func NewErrGroupWithContext(ctx context.Context) (ErrGroupPanicHandler, context.Context) {
	group, ctx := errgroup.WithContext(ctx)
	return ErrGroupPanicHandler{Group: group}, ctx
}

func (w *ErrGroupPanicHandler) Go(f func() error) {
	w.Group.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				log.Println("panic occurred:", r)
				return
			}
		}()

		return f()
	})
}

func (w *ErrGroupPanicHandler) Wait() error {
	defer func() {
		if err := recover(); err != nil {
			log.Println("panic occurred:", err)
		}
	}()
	return w.Group.Wait()
}

func (w *ErrGroupPanicHandler) SetLimit(n int) {
	w.Group.SetLimit(n)
}
