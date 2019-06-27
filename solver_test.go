package goplumber

import (
	"fmt"
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"testing"
)

func makeDependencyPipe(d map[string][]string) *PipelineConfig {
	tm := taskMap{}
	for taskName, dependencies := range d {
		if len(dependencies) == 0 {
			tm[taskName] = &Task{}
			continue
		}
		links := map[string]Link{}
		for idx, linkSource := range dependencies {
			links[fmt.Sprintf("%d", idx)] = Link{Source: linkSource}
		}
		tm[taskName] = &Task{Links: links}
	}
	return &PipelineConfig{Tasks: tm}
}

func getNames(t []*Task) (s []string) {
	for i := 0; i < len(t); i++ {
		s = append(s, t[i].name)
	}
	return
}

func TestPipeline_sortTasks_oneDep(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"b": {},
	})
	taskOrder := w.ShouldHaveResult(sortTasks(p.Tasks)).([]*Task)
	w.ShouldContain(getNames(taskOrder), []string{"b", "a"})
}

func TestPipeline_sortTasks_independentTasks(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {},
		"b": {},
		"c": {},
	})
	taskOrder := w.ShouldHaveResult(sortTasks(p.Tasks)).([]*Task)
	w.ShouldContain(getNames(taskOrder), []string{"a", "b", "c"})
}

func TestPipeline_sortTasks_nonChain(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"c": {"b"},
		"b": {},
	})
	taskOrder := w.ShouldHaveResult(sortTasks(p.Tasks)).([]*Task)
	w.Log(taskOrder)
	w.ShouldBeEqual(taskOrder[0].name, "b")
	w.ShouldContain(getNames(taskOrder[1:]), []string{"a", "c"})
}

func TestPipeline_sortTasks_chain(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"c": {"a"},
		"b": {},
	})
	taskOrder := w.ShouldHaveResult(sortTasks(p.Tasks)).([]*Task)
	w.Log(taskOrder)
	order := []string{"b", "a", "c"}
	w.ShouldBeEqual(getNames(taskOrder), order)
}

func TestPipeline_sortTasks_longChain(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b", "d", "e"},
		"b": {},
		"c": {"a", "b"},
		"d": {"e"},
		"e": {},
	})
	taskOrder := w.ShouldHaveResult(sortTasks(p.Tasks)).([]*Task)
	w.Log(taskOrder)
	w.ShouldContain(getNames(taskOrder[0:2]), []string{"e", "b"})
	w.ShouldBeEqual(getNames(taskOrder[2:]), []string{"d", "a", "c"})
}

func TestPipeline_sortTasks_diamond(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b", "c"},
		"b": {"d"},
		"c": {"d"},
		"d": {},
		"e": {"a"},
	})
	taskOrder := w.ShouldHaveResult(sortTasks(p.Tasks)).([]*Task)
	w.ShouldBeEqual(taskOrder[0].name, "d")
	w.ShouldContain(getNames(taskOrder[1:3]), []string{"b", "c"})
	w.ShouldBeEqual(getNames(taskOrder[3:5]), []string{"a", "e"})
}

func TestPipeline_sortTasks_cycles(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"b": {"c"},
		"c": {"a"},
	})
	w.Log(w.ShouldHaveError(sortTasks(p.Tasks)))

	p = makeDependencyPipe(map[string][]string{
		"a": {"c"},
		"c": {"a"},
		"b": {},
	})
	w.Log(w.ShouldHaveError(sortTasks(p.Tasks)))
}

func TestPipeline_sortTasks_longCycle(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"b": {"c"},
		"c": {"d"},
		"d": {"e"},
		"e": {"a"},
		"f": {"a", "b", "c"},
		"g": {"d", "e", "f"},
	})
	w.Log(w.ShouldHaveError(sortTasks(p.Tasks)))
}
