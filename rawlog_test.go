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
	err = rbl.Append([]byte("alpha"), []byte("something"), nil)
	if err != nil {
		panic(err)
	}
	err = rbl.Append([]byte("alpha"), []byte("something else"), nil)
	if err != nil {
		panic(err)
	}
	err = rbl.Append([]byte("alpha"), []byte("something different"), nil)
	if err != nil {
		panic(err)
	}
	err = rbl.Append([]byte("beta"), []byte("anything"), nil)
	if err != nil {
		panic(err)
	}
	r, err := rbl.NewReader()
	if err != nil {
		panic(err)
	}
	for {
		key, bytes, ts, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		log.Printf("[%v] '%v':'%v'\n", ts, string(key), string(bytes))
	}
}
