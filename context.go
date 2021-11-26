package bee

type Context struct {
	body []byte
}

func NewContext(body []byte) *Context {
	return &Context{body: body}
}
