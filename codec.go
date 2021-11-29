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
	var bodyBytes []byte
	//switch body.(type) {
	//case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64, string, bool, complex64,complex128:
	//	bodyBytes =
	//}
	if body != nil {
		bodyBytes, err = json.Marshal(body)
	}
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(bodyBytes)+len(name)+1))
	//if err := binary.Write(buf, binary.BigEndian, uint8(len(name))); err != nil {
	//	return nil, err
	//}
	buf.WriteByte(byte(uint8(len(name))))
	buf.WriteString(name)
	if len(bodyBytes) > 0 {
		buf.Write(bodyBytes)
	}
	return buf.Bytes(), nil
}

func (c LNBCodec) Decode(data []byte) (name string, body []byte) {
	length := int(uint8(data[0]))
	name = string(data[1 : length+1])
	body = data[length+1:]
	return
}
