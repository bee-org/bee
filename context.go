package bee

import (
	"context"
	"encoding/json"
	"time"
)

type Context struct {
	ctx  context.Context
	name string
	Body []byte
	req  interface{}
}

func NewContext(ctx context.Context, name string, body []byte) *Context {
	return &Context{ctx: ctx, name: name, Body: body}
}

func (c *Context) Name() string {
	return c.name
}

func (c *Context) Parse(v interface{}) error {
	// Recording request parameters
	defer func() { c.req = v }()
	return json.Unmarshal(c.Body, v)
}

// Req must be called after the Parse, Return the req recorded at parsing time.
func (c *Context) Req() interface{} {
	return c.req
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
