package codec

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestLNBCodec(t *testing.T) {
	c := LNBCodec{}
	name := "func1"
	body := map[string]string{"foo": "bar"}
	data, err := c.Encode(name, nil)
	if err != nil {
		panic(err)
	}
	gotName, gotBody := c.Decode(data)
	if gotName != name {
		t.Error(name, gotName)
	}
	gotM := make(map[string]string)
	if err := json.Unmarshal(gotBody, &gotM); err != nil {
		t.Error(gotBody, err)
	}
	if !reflect.DeepEqual(gotM, body) {
		t.Error(body, gotM)
	}
}
