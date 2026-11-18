package catalog

import (
	"encoding/binary"
	"testing"
)

func graphID(id int) [16]byte {
	var key [16]byte
	binary.BigEndian.PutUint64(key[8:], uint64(id))
	return key
}

func BenchmarkVerifyGraph100k(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		g := graph{nodes: make(map[[16]byte]int)}
		for i := range 100000 {
			g.add(graphID(i), graphID(i+1))
		}
		if !g.acyclic() {
			b.Fatal("chain has no cycle")
		}
	}
}
