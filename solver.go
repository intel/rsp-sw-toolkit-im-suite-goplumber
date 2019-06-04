package goplumber

import (
	"github.com/pkg/errors"
	"sort"
)

// sortTasks sorts a taskMap based on the tasks' dependencies.
//
// This returns an error if there are circular dependencies (i.e., the dependency
// graph must form a DAG).
//
// NOTE: This method assumes that the pipeline's Links have already been verified;
// if Links declare dependent tasks that don't exist, then this method will panic.
//
// Currently, this function only returns a valid partial order (topological sort).
// When it comes to ensuring dependencies are resolved correctly, any topological
// order will do; however, if we intend to add parallel task execution, we're
// better off if we can spread dependent tasks out as far as possible. To do so,
// we can use something like Coffman-Graham, but right now it's an over-optimization.
func sortTasks(tm taskMap) ([]string, error) {
	taskOrder := make([]string, len(tm))

	taskFinishTime := make(map[*Task]int, len(tm))

	// determine the finish time for each task
	taskIdx := 0
	for taskName, task := range tm {
		taskOrder[taskIdx] = taskName
		taskIdx++
		finishTime, dag := getFinishTime(tm, taskFinishTime, task)
		if !dag {
			return taskOrder, errors.Errorf("task '%s' is part of a cycle", taskName)
		}
		taskFinishTime[task] = finishTime
	}

	// sort the task order list so that tasks that finish earlier come first
	sort.Slice(taskOrder, func(t1, t2 int) bool {
		return taskFinishTime[tm[taskOrder[t1]]] < taskFinishTime[tm[taskOrder[t2]]]
	})
	return taskOrder, nil
}

// getFinishTime returns the number of tasks which must complete for this one to
// finish, including the task itself, along with a bool indicating whether or not
// the task is part of a cycle, in which case the finish time is undetermined
// and the second return value is false.
//
// The minimum finish time for any task is 1 (provided the graph has no cycles).
func getFinishTime(tasks taskMap, tFinishTime map[*Task]int, t *Task) (int, bool) {
	if ft, ok := tFinishTime[t]; ok {
		return ft, ft != -1
	}
	tFinishTime[t] = -1
	maxChild := 0
	isDag := true

	checkChild := func(c string) {
		if !isDag {
			return
		}
		var childFinish int
		childFinish, isDag = getFinishTime(tasks, tFinishTime, tasks[c])
		if childFinish > maxChild {
			maxChild = childFinish
		}
		return
	}

	for _, child := range t.Links {
		checkChild(child.Source)
	}
	for _, child := range t.Successes {
		checkChild(child)
	}
	for _, child := range t.Failures {
		checkChild(child)
	}
	if !isDag {
		return -1, false
	}

	tFinishTime[t] = maxChild + 1
	return maxChild+1, true
}
