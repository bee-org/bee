package codec

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestLNBCodec(t *testing.T) {
	c := LNBCodec{}
	header := &Header{Name: "func1"}
	value := map[string]string{"foo": "bar"}
	data, err := c.Encode(header, value)
	if err != nil {
		panic(err)
	}
	gotHeader, gotBody := c.Decode(data)
	if reflect.DeepEqual(header, gotHeader) {
		t.Error(header, gotHeader)
	}
	gotM := make(map[string]string)
	if err := json.Unmarshal(gotBody, &gotM); err != nil {
		t.Error(gotBody, err)
	}
	if !reflect.DeepEqual(gotM, value) {
		t.Error(value, gotM)
	}
}

func TestVND(t *testing.T) {
	type args struct {
		header *Header
		value  interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{args: args{header: &Header{Name: "func1"}, value: 1}},
		{args: args{header: &Header{Name: "func2"}, value: "a"}},
		{args: args{header: &Header{Name: "func3"}, value: map[string]interface{}{"a": 1, "b": "2"}}},
	}
	c := &VND{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotData, err := c.Encode(tt.args.header, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotHeader, gotBody := c.Decode(gotData)
			wantBody, _ := json.Marshal(tt.args.value)
			if !reflect.DeepEqual(&gotHeader, tt.args.header) {
				t.Errorf("Encode() gotHeader = %v, want %v", gotHeader, tt.args.header)
			}
			if !reflect.DeepEqual(gotBody, wantBody) {
				t.Errorf("Encode() gotBody = %v, want %v", gotBody, wantBody)
			}
		})
	}
}
