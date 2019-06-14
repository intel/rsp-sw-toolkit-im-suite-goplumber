package goplumber

import (
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"testing"
)

func TestNewPlumber(t *testing.T) {
	w := expect.WrapT(t)
	src := NewMemoryStore()
	p := NewPlumber(src)
	w.As("default tasks").
		ShouldContain(p.TaskGenerators, []string{"http", "mqtt", "validation"})
}
