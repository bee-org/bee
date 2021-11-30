package bee

import (
	"context"
	"encoding/json"
	"time"
)

type Context struct {
	ctx  context.Context
	Name string
	Body []byte
}

func NewContext(ctx context.Context, name string, body []byte) *Context {
	return &Context{ctx: ctx, Name: name, Body: body}
}

func (c *Context) Parse(v interface{}) error {
	return json.Unmarshal(c.Body, v)
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
