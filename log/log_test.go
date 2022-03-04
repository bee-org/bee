package log

import (
	"os"
	"testing"
)

var l ILogger

func TestMain(m *testing.M) {
	l = NewDefaultLogger()
	os.Exit(m.Run())
}

func TestLogger_Errorf(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{args: args{format: "name: %s, age: %d", args: []interface{}{"a", 18}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l.Errorf(tt.args.format, tt.args.args...)
		})
	}
}

func TestLogger_Errorln(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{args: args{args: []interface{}{"my", "name", "is", "a"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l.Errorln(tt.args.args...)
		})
	}
}

func TestLogger_Infof(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{args: args{format: "name: %s, age: %d", args: []interface{}{"a", 18}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l.Infof(tt.args.format, tt.args.args...)
		})
	}
}

func TestLogger_Infoln(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{args: args{args: []interface{}{"a", 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l.Infoln(tt.args.args...)
		})
	}
}

func TestLogger_Warningf(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{args: args{format: "name: %s, age: %d", args: []interface{}{"a", 18}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l.Warningf(tt.args.format, tt.args.args...)
		})
	}
}

func TestLogger_Warningln(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{args: args{args: []interface{}{"a", 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l.Warningln(tt.args.args...)
		})
	}
}
