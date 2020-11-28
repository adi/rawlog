package rawlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

// RawBytesLog holds an opened log
type RawBytesLog struct {
	logFile *os.File
}

// Reader holds the opened log from which we read the next log entry
// It allows one to read log entries independent of other readers and the writer
type Reader struct {
	logFile *os.File
}

// Open creates or opens a raw log
func Open(logFileName string) (*RawBytesLog, error) {
	logFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open (for append) log at '%s': %w", logFileName, err)
	}
	return &RawBytesLog{
		logFile: logFile,
	}, nil
}

func writeTimestamp(dst io.Writer, ts *time.Time) error {
	tsBytes, err := ts.MarshalBinary()
	if err != nil {
		return fmt.Errorf("can't encode: %w", err)
	}
	_, err = dst.Write(tsBytes)
	if err != nil {
		return fmt.Errorf("can't write: %w", err)
	}
	return nil
}

func writeBytesWithLen16(dst io.Writer, bytes []byte) error {
	itemLen := make([]byte, 2)
	binary.LittleEndian.PutUint16(itemLen, uint16(len(bytes)))
	_, err := dst.Write(itemLen)
	if err != nil {
		return fmt.Errorf("can't write: %w", err)
	}
	_, err = dst.Write(bytes)
	if err != nil {
		return fmt.Errorf("can't write: %w", err)
	}
	return nil
}

func writeBytesWithLen32(dst io.Writer, bytes []byte) error {
	itemLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(itemLen, uint32(len(bytes)))
	_, err := dst.Write(itemLen)
	if err != nil {
		return fmt.Errorf("can't write: %w", err)
	}
	_, err = dst.Write(bytes)
	if err != nil {
		return fmt.Errorf("can't write: %w", err)
	}
	return nil
}

// Append a set of bytes for the given key
func (rbl *RawBytesLog) Append(key []byte, bytes []byte, ts *time.Time) error {
	if ts == nil {
		currentTs := time.Now()
		ts = &currentTs
	}
	err := writeTimestamp(rbl.logFile, ts)
	if err != nil {
		return fmt.Errorf("Couldn't store log entry timestamp: %w", err)
	}
	err = writeBytesWithLen16(rbl.logFile, key)
	if err != nil {
		return fmt.Errorf("Couldn't store log entry key: %w", err)
	}
	err = writeBytesWithLen32(rbl.logFile, bytes)
	if err != nil {
		return fmt.Errorf("Couldn't store log entry bytes: %w", err)
	}
	return nil
}

// NewReader creates a RawBytesLogReader
func (rbl *RawBytesLog) NewReader() (*Reader, error) {
	logFile, err := os.OpenFile(rbl.logFile.Name(), os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open (for reading) log at '%s': %w", rbl.logFile.Name(), err)
	}
	reader := &Reader{
		logFile: logFile,
	}
	return reader, nil
}

// Close stops reading and cleans open file reference
func (rbl *RawBytesLog) Close() error {
	return rbl.logFile.Close()
}

func readTimestamp(src io.Reader) (*time.Time, error) {
	tsBytes := make([]byte, 15)
	n, err := src.Read(tsBytes)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != 15 {
		return nil, fmt.Errorf("can't read: %w", err)
	}
	ts := &time.Time{}
	err = ts.UnmarshalBinary(tsBytes)
	if err != nil {
		return nil, fmt.Errorf("can't decode: %w", err)
	}
	return ts, nil
}

func readBytesWithLen16(src io.Reader) ([]byte, error) {
	itemLen := make([]byte, 2)
	n, err := src.Read(itemLen)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != 2 {
		return nil, fmt.Errorf("can't read: %w", err)
	}
	itemLenUint16 := binary.LittleEndian.Uint16(itemLen)
	item := make([]byte, itemLenUint16)
	n, err = src.Read(item)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != int(itemLenUint16) {
		return nil, fmt.Errorf("can't read: %w", err)
	}
	return item, nil
}

func readBytesWithLen32(src io.Reader) ([]byte, error) {
	itemLen := make([]byte, 4)
	n, err := src.Read(itemLen)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != 4 {
		return nil, fmt.Errorf("can't read: %w", err)
	}
	itemLenUint32 := binary.LittleEndian.Uint32(itemLen)
	item := make([]byte, itemLenUint32)
	n, err = src.Read(item)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != int(itemLenUint32) {
		return nil, fmt.Errorf("can't read: %w", err)
	}
	return item, nil
}

// Next gets the following log entry
func (r *Reader) Next() ([]byte, []byte, *time.Time, error) {
	ts, err := readTimestamp(r.logFile)
	if err == io.EOF {
		return nil, nil, nil, err
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Couldn't retrieve log entry timestamp: %w", err)
	}
	key, err := readBytesWithLen16(r.logFile)
	if err == io.EOF {
		return nil, nil, nil, err
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Couldn't retrieve log entry key: %w", err)
	}
	bytes, err := readBytesWithLen32(r.logFile)
	if err == io.EOF {
		return nil, nil, nil, err
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Couldn't retrieve log entry bytes: %w", err)
	}
	return key, bytes, ts, nil
}

// Close stops reading and cleans open file reference
func (r *Reader) Close() error {
	return r.logFile.Close()
}
