package bee

import (
	"bytes"
	"encoding/json"
)

type Codec interface {
	Encode
	Decode
}

type Encode interface {
	Encode(name string, body interface{}) (data []byte, err error)
}
type Decode interface {
	Decode(data []byte) (name string, body []byte)
}

//LNBCodec 长度域（Length）+ 名域(Name) + 值域（Value）
type LNBCodec struct {
}

func (c LNBCodec) Encode(name string, body interface{}) (data []byte, err error) {
	bs, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(bs)+len(name)+1))
	//if err := binary.Write(buf, binary.BigEndian, uint8(len(name))); err != nil {
	//	return nil, err
	//}
	buf.WriteByte(byte(uint8(len(name))))
	buf.WriteString(name)
	buf.Write(bs)
	return buf.Bytes(), nil
}

func (c LNBCodec) Decode(data []byte) (name string, body []byte) {
	length := int(uint8(data[0]))
	name = string(data[1 : length+1])
	body = data[length+1:]
	return
}
