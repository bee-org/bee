package broker

import (
	"context"
	"github.com/fanjindong/bee"
)

type IBroker interface {
	Start() error
	Close() error
	Send(ctx context.Context, name string, data interface{}) error
	Register(name string, handler bee.Handler, opts ...bee.Option)
}
