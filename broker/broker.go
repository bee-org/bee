package broker

import (
	"context"
	"github.com/fanjindong/bee"
	"github.com/fanjindong/bee/middleware"
	"time"
)

type IBroker interface {
	Start() error
	Close() error
	Register(name string, handler bee.Handler, opts ...bee.Option)
	Middleware(mws ...middleware.Middleware)

	Send(ctx context.Context, name string, data interface{}) error
	SendDelay(ctx context.Context, name string, data interface{}, delay time.Duration) error
}
