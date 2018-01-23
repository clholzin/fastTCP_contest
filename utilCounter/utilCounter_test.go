package utilCounter

import (
	"testing"
)

func TestDumpCounts(t *testing.T) {
	t.Log("Struct with integer will be zeroed out")
	countStruct := new(Counter)
	countStruct.Total = 4
	countStruct.DumpCounts()
	if countStruct.Total != 0 {
		t.Fail()
	}
}
