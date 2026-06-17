package gospice

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

func TestIsADBCAuthError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "adbc unauthenticated",
			err:  adbc.Error{Code: adbc.StatusUnauthenticated, Msg: "[FlightSQL] Invalid credentials"},
			want: true,
		},
		{
			name: "adbc unauthorized",
			err:  adbc.Error{Code: adbc.StatusUnauthorized, Msg: "[FlightSQL] forbidden"},
			want: true,
		},
		{
			name: "wrapped adbc unauthenticated (the prepared-statement failure shape)",
			err: fmt.Errorf("error preparing statement: %w",
				adbc.Error{Code: adbc.StatusUnauthenticated, Msg: "[FlightSQL] Invalid credentials (Unauthenticated; Prepare)"}),
			want: true,
		},
		{
			name: "adbc internal is not an auth error",
			err:  adbc.Error{Code: adbc.StatusInternal, Msg: "boom"},
			want: false,
		},
		{
			name: "plain error mentioning Unauthenticated (fallback)",
			err:  errors.New("rpc error: code = Unauthenticated desc = invalid"),
			want: true,
		},
		{
			name: "unrelated transient error",
			err:  errors.New("connection refused"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isADBCAuthError(tt.err); got != tt.want {
				t.Errorf("isADBCAuthError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
