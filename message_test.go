package message

import (
	"testing"
)

func BenchmarkStringsBlockGetMessage(b *testing.B) {
	a := []string{"first", "second"}
	s := NewStrings(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.s = a
		s.BlockGetMessage()
	}
}
