package codec

import (
	"bytes"
	"encoding/json"
)

type Codec interface {
	Encode
	Decode
}

type Encode interface {
	Encode(header *Header, value interface{}) (data []byte, err error)
}
type Decode interface {
	Decode(data []byte) (header Header, body []byte)
}

type Header struct {
	Name  string
	Retry uint8
}

//LNBCodec 长度域（Length）+ 名域(Name) + 值域（Value）
type LNBCodec struct {
}

func (c *LNBCodec) Encode(header *Header, value interface{}) (data []byte, err error) {
	var bodyBytes []byte
	//switch body.(type) {
	//case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64, string, bool, complex64,complex128:
	//	bodyBytes =
	//}
	if value != nil {
		bodyBytes, err = json.Marshal(value)
	}
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(bodyBytes)+len(header.Name)+1))
	//if err := binary.Write(buf, binary.BigEndian, uint8(len(name))); err != nil {
	//	return nil, err
	//}
	buf.WriteByte(byte(uint8(len(header.Name))))
	buf.WriteString(header.Name)
	if len(bodyBytes) > 0 {
		buf.Write(bodyBytes)
	}
	return buf.Bytes(), nil
}

func (c *LNBCodec) Decode(data []byte) (header Header, body []byte) {
	length := int(uint8(data[0]))
	if len(data) < length+1 {
		return
	}
	header.Name = string(data[1 : length+1])
	body = data[length+1:]
	return
}

//VND 版本域（Version）+ 名域(Name) + 数据域（Data）
// ┌───────┬───────┬───────┬────────────────┐
// │version│ retry │ length│     name       │ header
// ├───────┴───────┴───────┴────────────────┤
// │                  body                  │
// └────────────────────────────────────────┘
type VND struct {
}

func (c *VND) Encode(header *Header, value interface{}) (data []byte, err error) {
	var body []byte
	if value != nil {
		if v, ok := value.([]byte); ok {
			body = v
		} else {
			body, err = json.Marshal(value)
		}
	}
	if err != nil {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(body)+len(header.Name)+3))
	//if err := binary.Write(buf, binary.BigEndian, uint8(len(name))); err != nil {
	//	return nil, err
	//}
	buf.WriteByte(uint8(0))                // version
	buf.WriteByte(uint8(header.Retry))     // retry counter
	buf.WriteByte(uint8(len(header.Name))) // name length
	buf.WriteString(header.Name)
	if len(body) > 0 {
		buf.Write(body)
	}
	return buf.Bytes(), nil
}

func (c *VND) Decode(data []byte) (header Header, body []byte) {
	//version := data[0]
	header.Retry = data[1]
	length := int(data[2])
	if len(data) < length+3 {
		return
	}
	header.Name = string(data[3 : length+3])
	body = data[length+3:]
	return
}
