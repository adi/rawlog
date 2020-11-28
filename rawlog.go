package rawlog

import (
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
