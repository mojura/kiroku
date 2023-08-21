package kiroku

import (
	"context"
	"testing"
	"time"

	"github.com/hatchify/errors"
)

func Test_sleep(t *testing.T) {
	type args struct {
		ctx           func() context.Context
		sleepDuration time.Duration
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "context close",
			args: args{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				},
				sleepDuration: time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "context close",
			args: args{
				ctx: func() context.Context {
					return context.Background()
				},
				sleepDuration: time.Millisecond,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := sleep(tt.args.ctx(), tt.args.sleepDuration); (err != nil) != tt.wantErr {
				t.Errorf("sleep() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handleTwoErrors(t *testing.T) {
	type args struct {
		a error
		b error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "both",
			args: args{
				a: errors.Error("one"),
				b: errors.Error("one"),
			},
			wantErr: true,
		},
		{
			name: "a",
			args: args{
				a: errors.Error("one"),
			},
			wantErr: true,
		},
		{
			name: "b",
			args: args{
				b: errors.Error("one"),
			},
			wantErr: true,
		},
		{
			name:    "none",
			args:    args{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handleTwoErrors(tt.args.a, tt.args.b); (err != nil) != tt.wantErr {
				t.Errorf("handleTwoErrors() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_isNilSource(t *testing.T) {
	type args struct {
		s func() Source
	}

	type testcase struct {
		name      string
		args      args
		wantIsNil bool
	}

	tests := []testcase{
		{
			name: "basic",
			args: args{
				s: func() Source { return &NOOP{} },
			},
			wantIsNil: false,
		},
		{

			name: "unset",
			args: args{
				s: func() Source {
					return nil
				},
			},
			wantIsNil: true,
		},
		{

			name: "nil",
			args: args{
				s: func() Source {
					var noop *NOOP
					return noop
				},
			},
			wantIsNil: true,
		},
		{

			name: "nil interface",
			args: args{
				s: func() Source {
					var s Source
					return s
				},
			},
			wantIsNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotIsNil := isNilSource(tt.args.s()); gotIsNil != tt.wantIsNil {
				t.Errorf("isNilSource() = %v, want %v", gotIsNil, tt.wantIsNil)
			}
		})
	}
}
