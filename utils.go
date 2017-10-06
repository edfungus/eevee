package main

import (
	"encoding/binary"
)

func bytes2int(in []byte) (out int) {
	return int(binary.LittleEndian.Uint16(in))
}

func int2bytes(in int) (out []byte) {
	out = make([]byte, 4)
	binary.LittleEndian.PutUint16(out, uint16(in))
	return out
}

func uint162int(in uint16) (out int) {
	return int(in)
}
