package resp

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestReadValue(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Value
		wantErr bool
	}{
		{
			name:  "Simple String",
			input: "+OK\r\n",
			want:  Value{Typ: SimpleString, Str: "OK"},
		},
		{
			name:  "Error",
			input: "-ERR unknown command\r\n",
			want:  Value{Typ: Error, Str: "ERR unknown command"},
		},
		{
			name:  "Integer",
			input: ":1000\r\n",
			want:  Value{Typ: Integer, Num: 1000},
		},
		{
			name:  "Bulk String",
			input: "$6\r\nfoobar\r\n",
			want:  Value{Typ: BulkString, Str: "foobar"},
		},
		{
			name:  "Null Bulk String",
			input: "$-1\r\n",
			want:  Value{Typ: Null},
		},
		{
			name:  "Empty Array",
			input: "*0\r\n",
			want:  Value{Typ: Array, Array: []Value{}},
		},
		{
			name:  "Null Array",
			input: "*-1\r\n",
			want:  Value{Typ: Null},
		},
		{
			name:  "Array of elements",
			input: "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n",
			want: Value{Typ: Array, Array: []Value{
				{Typ: BulkString, Str: "SET"},
				{Typ: BulkString, Str: "a"},
				{Typ: BulkString, Str: "1"},
			}},
		},
		{
			name:  "Inline Command",
			input: "PING\r\n",
			want: Value{Typ: Array, Array: []Value{
				{Typ: BulkString, Str: "PING"},
			}},
		},
		{
			name:  "Inline Command Multiple Args",
			input: "SET mykey myval\n",
			want: Value{Typ: Array, Array: []Value{
				{Typ: BulkString, Str: "SET"},
				{Typ: BulkString, Str: "mykey"},
				{Typ: BulkString, Str: "myval"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBufferString(tt.input)
			reader := NewReader(bufio.NewReader(buf))
			got, err := reader.ReadValue()
			if (err != nil) != tt.wantErr {
				t.Errorf("Reader.ReadValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reader.ReadValue() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
