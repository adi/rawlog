package rawlog

import (
	"encoding/binary"
	"io"
)

func writeBytesWithLen16(dst io.Writer, bytes []byte) error {
	itemLen := make([]byte, 2)
	binary.LittleEndian.PutUint16(itemLen, uint16(len(bytes)))
	_, err := dst.Write(itemLen)
	if err != nil {
		return err
	}
	_, err = dst.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func writeBytesWithLen32(dst io.Writer, bytes []byte) error {
	itemLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(itemLen, uint32(len(bytes)))
	_, err := dst.Write(itemLen)
	if err != nil {
		return err
	}
	_, err = dst.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func readBytesWithLen16(src io.Reader) ([]byte, error) {
	itemLen := make([]byte, 2)
	n, err := src.Read(itemLen)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != 2 {
		return nil, err
	}
	itemLenUint16 := binary.LittleEndian.Uint16(itemLen)
	item := make([]byte, itemLenUint16)
	n, err = src.Read(item)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != int(itemLenUint16) {
		return nil, err
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
		return nil, err
	}
	itemLenUint32 := binary.LittleEndian.Uint32(itemLen)
	item := make([]byte, itemLenUint32)
	n, err = src.Read(item)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || n != int(itemLenUint32) {
		return nil, err
	}
	return item, nil
}
