package rawlog

import (
	"io"
	"log"
	"testing"
)

func TestX(t *testing.T) {
	rbl, err := Open("tmp/log.test")
	if err != nil {
		panic(err)
	}
	err = rbl.Append(&Entry{Key: []byte("alpha"), Bytes: []byte("something")})
	if err != nil {
		panic(err)
	}
	err = rbl.Append(&Entry{Key: []byte("alpha"), Bytes: []byte("something else")})
	if err != nil {
		panic(err)
	}
	err = rbl.Append(&Entry{Key: []byte("alpha"), Bytes: []byte("something different")})
	if err != nil {
		panic(err)
	}
	err = rbl.Append(&Entry{Key: []byte("beta"), Bytes: []byte("anything")})
	if err != nil {
		panic(err)
	}
	r, err := rbl.NewReader()
	if err != nil {
		panic(err)
	}
	for {
		entry, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		log.Printf("'%v':'%v'\n", string(entry.Key), string(entry.Bytes))
	}
	r.Close()
	rbl.Close()
}
