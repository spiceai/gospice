package gospice

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A FixedSizeBinaryParam whose value is not exactly its byte width must be
// reported as an error. Arrow's builder panics on a length mismatch, which
// crashed the caller's process from inside SqlWithParams.
func TestAppendValueToBuilderFixedSizeBinaryLength(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		width   int
		wantErr string
	}{
		{name: "exact width", value: []byte{1, 2, 3, 4}, width: 4},
		{name: "too short", value: []byte{1, 2, 3}, width: 16, wantErr: "cannot convert 3 bytes to fixed size binary of width 16"},
		{name: "too long", value: bytes.Repeat([]byte{0xab}, 17), width: 16, wantErr: "cannot convert 17 bytes to fixed size binary of width 16"},
		{name: "empty", value: []byte{}, width: 2, wantErr: "cannot convert 0 bytes to fixed size binary of width 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := array.NewFixedSizeBinaryBuilder(memory.NewGoAllocator(), &arrow.FixedSizeBinaryType{ByteWidth: tt.width})
			defer builder.Release()

			err := appendValueToBuilder(builder, tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("appendValueToBuilder() error = %v, want %q", err, tt.wantErr)
				}
				if builder.Len() != 0 {
					t.Fatalf("builder holds %d values after a rejected append, want 0", builder.Len())
				}
				return
			}
			if err != nil {
				t.Fatalf("appendValueToBuilder() error = %v", err)
			}
			arr := builder.NewArray()
			defer arr.Release()
			if got := arr.(*array.FixedSizeBinary).Value(0); !bytes.Equal(got, tt.value) {
				t.Fatalf("value = %x, want %x", got, tt.value)
			}
		})
	}
}
