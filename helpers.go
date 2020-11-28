package rawlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

func writeTimestamp(dst io.Writer, ts *time.Time) error {
	tsUnix := ts.Unix()
	tsUnixBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(tsUnixBytes, uint32(tsUnix))
	_, err := dst.Write(tsUnixBytes)
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

func readTimestamp(src io.Reader) (*time.Time, error) {
	tsUnixBytes := make([]byte, 4)
	n, err := src.Read(tsUnixBytes)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != 4 {
		return nil, fmt.Errorf("can't read: %w", err)
	}
	tsUnix := binary.LittleEndian.Uint32(tsUnixBytes)
	ts := time.Unix(int64(tsUnix), 0)
	return &ts, nil
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
