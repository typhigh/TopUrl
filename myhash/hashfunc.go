package myhash

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
)

// convert []byte to int32
func bytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

// MyHashFunction is a simple hash function using MD5
func MyHashFunction(s string) int {
	singByte := []byte(s)
	hash := md5.New()
	hash.Write(singByte)
	return bytesToInt(hash.Sum(nil))
}
