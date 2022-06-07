package codec

import (
	"encoding/json"
	"github.com/bee-org/bee/message"
	"reflect"
	"testing"
)

func TestVND(t *testing.T) {
	type args struct {
		m message.Message
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{args: args{m: message.NewMsg("func1", nil)}},
		{args: args{m: message.NewMsg("func1", 1)}},
		{args: args{m: message.NewMsg("func2", "a").SetRetryCount(1)}},
		{args: args{m: message.NewMsg("func3", map[string]interface{}{"a": 1, "b": "2"})}},
		{args: args{m: message.NewMsg("func1", nil).SetRetryCount(1)}},
		{args: args{m: message.NewMsg("func1", nil).SetRetryCount(1).SetBody([]byte{1})}},
		{args: args{m: message.NewMsg("func1", nil).SetBody([]byte{})}},
		{args: args{m: message.NewMsg("func2", nil).SetBody([]byte{1, 2, 3})}},
	}
	c := &VNDCodec{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotData, err := c.Encode(tt.args.m)
			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, _ := c.Decode(gotData)
			if value := tt.args.m.GetValue(); value != nil {
				body, _ := json.Marshal(value)
				tt.args.m.SetBody(body)
			}
			if tt.args.m.GetBody() == nil {
				tt.args.m.SetBody([]byte{})
			}

			if !reflect.DeepEqual(got.GetName(), tt.args.m.GetName()) {
				t.Errorf("Encode() got.GetName = %v, want.GetName %v", got.GetName(), tt.args.m.GetName())
			}
			if !reflect.DeepEqual(got.GetVersion(), tt.args.m.GetVersion()) {
				t.Errorf("Encode() got.GetVersion = %v, want.GetVersion %v", got.GetVersion(), tt.args.m.GetVersion())
			}
			if !reflect.DeepEqual(got.GetRetryCount(), tt.args.m.GetRetryCount()) {
				t.Errorf("Encode() got.GetRetryCount = %v, want.GetRetryCount %v", got.GetRetryCount(), tt.args.m.GetRetryCount())
			}
			if !reflect.DeepEqual(got.GetBody(), tt.args.m.GetBody()) {
				t.Errorf("Encode() got.GetBody = %v, want.GetBody %v", got.GetBody(), tt.args.m.GetBody())
			}
		})
	}
}
