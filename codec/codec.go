package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bee-org/bee/message"
	"github.com/pkg/errors"
)

type Codec interface {
	Encode
	Decode
}

type Encode interface {
	Encode(m message.Message) (data []byte, err error)
}
type Decode interface {
	Decode(data []byte) (m message.Message, err error)
}

var (
	NameIsNullError = errors.New("name is ''")
	DataDecodeError = errors.New("data decode error")
)

//VNDCodec 版本域（Version）+ 名域(Name) + 数据域（Data）
// ┌───────┬───────┬───────┬────────────────┐
// │version│ retry │ length│     name       │ header
// ├───────┴───────┴───────┴────────────────┤
// │                  body                  │
// └────────────────────────────────────────┘
type VNDCodec struct {
}

func (c *VNDCodec) Encode(m message.Message) (data []byte, err error) {
	name := m.GetName()
	if len(name) == 0 {
		err = NameIsNullError
		return
	}
	var body []byte
	if value := m.GetValue(); value != nil {
		body, err = json.Marshal(value)
		if err != nil {
			return
		}
		m.SetBody(body)
	} else {
		body = m.GetBody()
	}

	buf := bytes.NewBuffer(make([]byte, 0, len(body)+len(name)+3))
	buf.WriteByte(m.GetVersion())    // version
	buf.WriteByte(m.GetRetryCount()) // retry counter
	buf.WriteByte(uint8(len(name)))  // name length
	buf.WriteString(name)            // name
	if len(body) > 0 {
		buf.Write(body)
	}
	return buf.Bytes(), nil
}

func (c *VNDCodec) Decode(data []byte) (m message.Message, err error) {
	if len(data) < 4 {
		err = errors.Wrap(DataDecodeError, fmt.Sprintf("%v", data))
		return
	}
	//version := data[0]
	retryCount := data[1]
	length := int(data[2])
	name := string(data[3 : length+3])
	body := data[length+3:]
	m = message.NewMsg(name, nil).SetRetryCount(retryCount).SetBody(body)
	return
}
