// Package resp implements the Redis Serialization Protocol (RESP2) parser and writer.
// It supports all five RESP2 wire types: simple strings, errors, integers, bulk strings,
// and arrays, as well as inline command parsing for netcat/telnet clients.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ValueType represents the type of a RESP value.
type ValueType int

const (
	// SimpleString represents a RESP simple string (+).
	SimpleString ValueType = iota
	// Error represents a RESP error (-).
	Error
	// Integer represents a RESP integer (:).
	Integer
	// BulkString represents a RESP bulk string ($).
	BulkString
	// Array represents a RESP array (*).
	Array
	// Null represents a RESP null bulk string ($-1) or null array (*-1).
	Null
)

// RESP protocol type prefix bytes.
const (
	prefixSimpleString = '+'
	prefixError        = '-'
	prefixInteger      = ':'
	prefixBulkString   = '$'
	prefixArray        = '*'
)

// String returns the human-readable name of the ValueType.
func (vt ValueType) String() string {
	switch vt {
	case SimpleString:
		return "simple_string"
	case Error:
		return "error"
	case Integer:
		return "integer"
	case BulkString:
		return "bulk_string"
	case Array:
		return "array"
	case Null:
		return "null"
	default:
		return "unknown"
	}
}

// Value represents a parsed RESP value. Depending on Typ, one of the fields
// Str, Num, or Array will be populated.
type Value struct {
	Typ   ValueType
	Str   string  // populated for SimpleString, Error, BulkString
	Num   int64   // populated for Integer
	Array []Value // populated for Array
}

// BulkStringValue creates a new BulkString Value with the given string.
func BulkStringValue(s string) Value {
	return Value{Typ: BulkString, Str: s}
}

// SimpleStringValue creates a new SimpleString Value with the given string.
func SimpleStringValue(s string) Value {
	return Value{Typ: SimpleString, Str: s}
}

// ErrorValue creates a new Error Value with the given message.
func ErrorValue(msg string) Value {
	return Value{Typ: Error, Str: msg}
}

// IntegerValue creates a new Integer Value with the given number.
func IntegerValue(n int64) Value {
	return Value{Typ: Integer, Num: n}
}

// NullValue creates a new Null Value.
func NullValue() Value {
	return Value{Typ: Null}
}

// ArrayValue creates a new Array Value with the given elements.
func ArrayValue(elems []Value) Value {
	return Value{Typ: Array, Array: elems}
}

// Common RESP error prefixes.
var (
	ErrInvalidSyntax  = errors.New("resp: invalid syntax")
	ErrUnexpectedType = errors.New("resp: unexpected type prefix")
	ErrInvalidLength  = errors.New("resp: invalid length")
)

// Reader reads and parses RESP values from a buffered reader.
type Reader struct {
	r *bufio.Reader
}

// NewReader creates a new RESP Reader wrapping the given buffered reader.
func NewReader(r *bufio.Reader) *Reader {
	return &Reader{r: r}
}

// ReadValue reads and returns the next RESP value from the underlying reader.
// It automatically detects inline commands (text not starting with a RESP prefix)
// and parses them as arrays of bulk strings.
func (rd *Reader) ReadValue() (Value, error) {
	b, err := rd.r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch b {
	case prefixSimpleString:
		return rd.readSimpleString()
	case prefixError:
		return rd.readError()
	case prefixInteger:
		return rd.readInteger()
	case prefixBulkString:
		return rd.readBulkString()
	case prefixArray:
		return rd.readArray()
	default:
		// Inline command: unread the byte and read the entire line,
		// then split by whitespace into an array of bulk strings.
		if err := rd.r.UnreadByte(); err != nil {
			return Value{}, fmt.Errorf("resp: failed to unread byte: %w", err)
		}
		return rd.readInline()
	}
}

// readLine reads bytes until \r\n and returns the line content without the trailing CRLF.
func (rd *Reader) readLine() (string, error) {
	line, err := rd.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Strip trailing \r\n
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	// Handle \n only (lenient)
	return strings.TrimRight(line, "\r\n"), nil
}

// readSimpleString reads a RESP simple string value after the + prefix has been consumed.
func (rd *Reader) readSimpleString() (Value, error) {
	line, err := rd.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Typ: SimpleString, Str: line}, nil
}

// readError reads a RESP error value after the - prefix has been consumed.
func (rd *Reader) readError() (Value, error) {
	line, err := rd.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Typ: Error, Str: line}, nil
}

// readInteger reads a RESP integer value after the : prefix has been consumed.
func (rd *Reader) readInteger() (Value, error) {
	line, err := rd.readLine()
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %v", ErrInvalidSyntax, err)
	}
	return Value{Typ: Integer, Num: n}, nil
}

// readBulkString reads a RESP bulk string value after the $ prefix has been consumed.
// Handles null bulk strings ($-1\r\n).
func (rd *Reader) readBulkString() (Value, error) {
	line, err := rd.readLine()
	if err != nil {
		return Value{}, err
	}
	length, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %v", ErrInvalidSyntax, err)
	}

	// Null bulk string
	if length < 0 {
		return Value{Typ: Null}, nil
	}

	// Read exactly `length` bytes using io.ReadFull to avoid short reads
	buf := make([]byte, length)
	if _, err := io.ReadFull(rd.r, buf); err != nil {
		return Value{}, fmt.Errorf("resp: failed to read bulk string body: %w", err)
	}

	// Consume trailing \r\n
	if _, err := rd.r.ReadByte(); err != nil {
		return Value{}, err
	}
	if _, err := rd.r.ReadByte(); err != nil {
		return Value{}, err
	}

	return Value{Typ: BulkString, Str: string(buf)}, nil
}

// readArray reads a RESP array value after the * prefix has been consumed.
// Handles null arrays (*-1\r\n).
func (rd *Reader) readArray() (Value, error) {
	line, err := rd.readLine()
	if err != nil {
		return Value{}, err
	}
	count, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %v", ErrInvalidSyntax, err)
	}

	// Null array
	if count < 0 {
		return Value{Typ: Null}, nil
	}

	elems := make([]Value, count)
	for i := int64(0); i < count; i++ {
		val, err := rd.ReadValue()
		if err != nil {
			return Value{}, err
		}
		elems[i] = val
	}

	return Value{Typ: Array, Array: elems}, nil
}

// readInline reads an inline command (plain text terminated by \n or \r\n)
// and splits it into an array of bulk string values.
func (rd *Reader) readInline() (Value, error) {
	line, err := rd.readLine()
	if err != nil {
		return Value{}, err
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return Value{Typ: Array, Array: []Value{}}, nil
	}

	parts := strings.Fields(line)
	elems := make([]Value, len(parts))
	for i, part := range parts {
		elems[i] = Value{Typ: BulkString, Str: part}
	}

	return Value{Typ: Array, Array: elems}, nil
}
