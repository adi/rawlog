package rawlog

import (
	"os"
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

// Entry contains one log piece
type Entry struct {
	Key   []byte
	Bytes []byte
}

// Open creates or opens a raw log
func Open(logFileName string) (*RawBytesLog, error) {
	logFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &RawBytesLog{
		logFile: logFile,
	}, nil
}

// Append a set of bytes for the given key
func (rbl *RawBytesLog) Append(entry *Entry) error {
	err := writeBytesWithLen16(rbl.logFile, entry.Key)
	if err != nil {
		return err
	}
	err = writeBytesWithLen32(rbl.logFile, entry.Bytes)
	if err != nil {
		return err
	}
	return nil
}

// NewReader creates a RawBytesLogReader
func (rbl *RawBytesLog) NewReader() (*Reader, error) {
	logFile, err := os.OpenFile(rbl.logFile.Name(), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
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
func (r *Reader) Next() (*Entry, error) {
	key, err := readBytesWithLen16(r.logFile)
	if err != nil {
		return nil, err
	}
	bytes, err := readBytesWithLen32(r.logFile)
	if err != nil {
		return nil, err
	}
	return &Entry{
		Key:   key,
		Bytes: bytes,
	}, nil
}

// Close stops reading and cleans open file reference
func (r *Reader) Close() error {
	return r.logFile.Close()
}
