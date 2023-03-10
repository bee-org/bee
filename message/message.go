package message

//Message Concurrency is unsafe
type Message interface {
	GetName() string
	GetValue() interface{}
	GetBody() []byte
	GetVersion() uint8
	GetRetryCount() uint8
	SetBody(body []byte) Message
	SetVersion(v uint8) Message
	SetRetryCount(rc uint8) Message
	IncrRetryCount() uint8
	GetMsgId() string
	SetMsgId(id string) Message
}

//Msg Concurrency is unsafe
type Msg struct {
	id         string
	version    uint8
	retryCount uint8
	name       string
	value      interface{}
	body       []byte
}

func NewMsg(name string, value interface{}) Message {
	return &Msg{name: name, value: value}
}

func (m *Msg) GetName() string                { return m.name }
func (m *Msg) GetValue() interface{}          { return m.value }
func (m *Msg) GetBody() []byte                { return m.body }
func (m *Msg) GetVersion() uint8              { return m.version }
func (m *Msg) GetRetryCount() uint8           { return m.retryCount }
func (m *Msg) SetVersion(v uint8) Message     { m.version = v; return m }
func (m *Msg) SetRetryCount(rc uint8) Message { m.retryCount = rc; return m }
func (m *Msg) SetBody(body []byte) Message    { m.body = body; return m }
func (m *Msg) IncrRetryCount() uint8          { m.retryCount++; return m.retryCount }
func (m *Msg) GetMsgId() string               { return m.id }
func (m *Msg) SetMsgId(id string) Message     { m.id = id; return m }
