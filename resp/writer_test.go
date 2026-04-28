package resp

import (
	"bufio"
	"bytes"
	"testing"
)

func TestWriteValue(t *testing.T) {
	tests := []struct {
		name  string
		value Value
		want  string
	}{
		{
			name:  "Simple String",
			value: SimpleStringValue("PONG"),
			want:  "+PONG\r\n",
		},
		{
			name:  "Error",
			value: ErrorValue("ERR test"),
			want:  "-ERR test\r\n",
		},
		{
			name:  "Integer",
			value: IntegerValue(12345),
			want:  ":12345\r\n",
		},
		{
			name:  "Bulk String",
			value: BulkStringValue("hello"),
			want:  "$5\r\nhello\r\n",
		},
		{
			name:  "Null Value",
			value: NullValue(),
			want:  "$-1\r\n",
		},
		{
			name: "Array of elements",
			value: ArrayValue([]Value{
				SimpleStringValue("OK"),
				IntegerValue(-42),
			}),
			want: "*2\r\n+OK\r\n:-42\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWriter(bufio.NewWriter(&buf))
			err := writer.WriteValue(tt.value)
			if err != nil {
				t.Fatalf("Writer.WriteValue() error = %v", err)
			}
			writer.Flush()
			got := buf.String()
			if got != tt.want {
				t.Errorf("Writer.WriteValue() = %q, want %q", got, tt.want)
			}
		})
	}
}
