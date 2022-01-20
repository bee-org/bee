package middleware

import (
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/example"
	"testing"
)

func TestRecoverPanic(t *testing.T) {
	tests := []struct {
		name    string
		handler bee.Handler
		ctx     *bee.Context
		wantErr bool
	}{
		{handler: example.PanicHandler, ctx: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := RecoverPanic()(tt.handler)
			if err := handler(tt.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
