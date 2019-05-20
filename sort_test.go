package goplumber

import (
	"fmt"
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"testing"
)

func makeDependencyPipe(d map[string][]string) *Pipeline {
	tm := map[string]*Task{}
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
	return &Pipeline{TaskMap: tm}
}

func TestPipeline_sortTasks_oneDep(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"b": {},
	})
	w.ShouldSucceed(p.checkTasks())
	w.ShouldSucceed(p.sortTasks())
	w.Log(p.taskOrder)
	w.ShouldBeEqual(p.taskOrder, []string{"b", "a"})
}

func TestPipeline_sortTasks_independentTasks(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {},
		"b": {},
		"c": {},
	})
	w.ShouldSucceed(p.checkTasks())
	w.ShouldSucceed(p.sortTasks())
	w.Log(p.taskOrder)
	w.ShouldContain(p.taskOrder, p.TaskMap)
}

func TestPipeline_sortTasks_nonChain(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"c": {"b"},
		"b": {},
	})
	w.ShouldSucceed(p.checkTasks())
	w.ShouldSucceed(p.sortTasks())
	w.Log(p.taskOrder)
	w.ShouldBeEqual(p.taskOrder[0], "b")
	w.ShouldContain(p.taskOrder[1:], []string{"a", "c"})
}

func TestPipeline_sortTasks_chain(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"c": {"a"},
		"b": {},
	})
	w.ShouldSucceed(p.checkTasks())
	w.ShouldSucceed(p.sortTasks())
	w.Log(p.taskOrder)
	w.ShouldBeEqual(p.taskOrder, []string{"b", "a", "c"})
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
	w.ShouldSucceed(p.checkTasks())
	w.ShouldSucceed(p.sortTasks())
	w.Log(p.taskOrder)
	w.ShouldContain(p.taskOrder[0:2], []string{"e", "b"})
	w.ShouldBeEqual(p.taskOrder[2:], []string{"d", "a", "c"})
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
	w.ShouldSucceed(p.checkTasks())
	w.ShouldSucceed(p.sortTasks())
	w.Log(p.taskOrder)
	w.ShouldBeEqual(p.taskOrder[0], "d")
	w.ShouldContain(p.taskOrder[1:3], []string{"b", "c"})
	w.ShouldBeEqual(p.taskOrder[3:5], []string{"a", "e"})
}

func TestPipeline_sortTasks_cycles(t *testing.T) {
	w := expect.WrapT(t)
	p := makeDependencyPipe(map[string][]string{
		"a": {"b"},
		"b": {"c"},
		"c": {"a"},
	})
	w.ShouldSucceed(p.checkTasks())
	err := w.ShouldFail(p.sortTasks())
	w.Log(err)

	p = makeDependencyPipe(map[string][]string{
		"a": {"c"},
		"c": {"a"},
		"b": {},
	})
	w.ShouldSucceed(p.checkTasks())
	w.Log(w.ShouldFail(p.sortTasks()))
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
	w.ShouldSucceed(p.checkTasks())
	w.Log(w.ShouldFail(p.sortTasks()))
}
