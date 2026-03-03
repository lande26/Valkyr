package resp

import (
	"bufio"
	"strconv"
)

// Writer writes RESP-encoded values to a buffered writer.
// Each Write method appends the correctly framed RESP bytes.
// Call Flush() after writing to ensure data is sent to the underlying writer.
type Writer struct {
	w *bufio.Writer
}

// NewWriter creates a new RESP Writer wrapping the given buffered writer.
func NewWriter(w *bufio.Writer) *Writer {
	return &Writer{w: w}
}

// WriteSimpleString writes a RESP simple string: +<str>\r\n
func (w *Writer) WriteSimpleString(s string) error {
	w.w.WriteByte(prefixSimpleString)
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteError writes a RESP error: -<msg>\r\n
func (w *Writer) WriteError(msg string) error {
	w.w.WriteByte(prefixError)
	w.w.WriteString(msg)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteInteger writes a RESP integer: :<n>\r\n
func (w *Writer) WriteInteger(n int64) error {
	w.w.WriteByte(prefixInteger)
	w.w.WriteString(strconv.FormatInt(n, 10))
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteBulkString writes a RESP bulk string: $<len>\r\n<data>\r\n
func (w *Writer) WriteBulkString(s string) error {
	w.w.WriteByte(prefixBulkString)
	w.w.WriteString(strconv.Itoa(len(s)))
	w.w.WriteString("\r\n")
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteNull writes a RESP null bulk string: $-1\r\n
func (w *Writer) WriteNull() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// WriteArrayHeader writes the RESP array header: *<n>\r\n
// The caller must write exactly n elements after this call.
func (w *Writer) WriteArrayHeader(n int) error {
	w.w.WriteByte(prefixArray)
	w.w.WriteString(strconv.Itoa(n))
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteNullArray writes a RESP null array: *-1\r\n
func (w *Writer) WriteNullArray() error {
	_, err := w.w.WriteString("*-1\r\n")
	return err
}

// WriteValue writes an arbitrary Value in the correct RESP encoding.
// This is the primary method used by command handlers to send responses.
func (w *Writer) WriteValue(v Value) error {
	switch v.Typ {
	case SimpleString:
		return w.WriteSimpleString(v.Str)
	case Error:
		return w.WriteError(v.Str)
	case Integer:
		return w.WriteInteger(v.Num)
	case BulkString:
		return w.WriteBulkString(v.Str)
	case Null:
		return w.WriteNull()
	case Array:
		if err := w.WriteArrayHeader(len(v.Array)); err != nil {
			return err
		}
		for _, elem := range v.Array {
			if err := w.WriteValue(elem); err != nil {
				return err
			}
		}
		return nil
	default:
		return w.WriteError("ERR unknown value type")
	}
}

// Flush flushes the underlying buffered writer, ensuring all written data
// is sent to the network connection.
func (w *Writer) Flush() error {
	return w.w.Flush()
}
