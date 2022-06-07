package bee

import (
	"context"
	"encoding/json"
	"github.com/bee-org/bee/message"
	"time"
)

type Context struct {
	ctx context.Context
	msg message.Message
	req interface{}
}

func NewCtx(ctx context.Context, m message.Message) *Context {
	return &Context{ctx: ctx, msg: m}
}

func (c *Context) Name() string {
	return c.msg.GetName()
}

func (c *Context) Parse(v interface{}) error {
	// Recording request parameters
	defer func() { c.req = v }()
	return json.Unmarshal(c.msg.GetBody(), v)
}

// Req must be called after the Parse, Return the req recorded at parsing time.
func (c *Context) Req() interface{} {
	return c.req
}

func (c *Context) Message() message.Message {
	return c.msg
}

func (c *Context) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *Context) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *Context) Err() error {
	return c.ctx.Err()
}

func (c *Context) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}
