package goplumber

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"reflect"
)

// Plumber is used to create pipelines.
type Plumber struct {
	// TemplateLoader tells the Plumber from where it should load templates.
	TemplateSource DataSource
	// TaskGenerators define how the Plumber maps task types to Pipes when creating Tasks.
	TaskGenerators map[string]TaskGenerator
}

// Returns a new Plumber with the given template loader and default task types.
//
// You can modify the TaskGenerators map to change how tasks are constructed,
// which tasks are allowed, and what backends can/should be used. By default,
// the map includes "http", "mqtt", and "validation", but you may find it useful
// to add, for instance, a "secrets" type to load Docker secrets, a "consul"
// type for key value pairs, or other types of sources/sinks.
func NewPlumber(templateLoader DataSource) Plumber {
	var defaultGenerators = map[string]TaskGenerator{
		"http":       SimpleTask(func() Pipe { return &HTTPTask{} }),
		"mqtt":       SimpleTask(func() Pipe { return NewMQTTTask() }),
		"validation": SimpleTask(func() Pipe { return &JSONValidationTask{} }),
	}

	p := Plumber{
		TemplateSource: templateLoader,
		TaskGenerators: defaultGenerators,
	}

	return p
}

type SimpleTask func() Pipe

func (f SimpleTask) GetPipe(task *Task) (Pipe, error) {
	pipe := f()
	if len(task.Raw) != 0 {
		if err := json.Unmarshal(task.Raw, pipe); err != nil {
			return nil, err
		}
	}
	return pipe, nil
}

type TaskFunc func(task *Task) (Pipe, error)

func (f TaskFunc) GetPipe(task *Task) (Pipe, error) {
	pipe, err := f(task)
	if err != nil {
		return nil, err
	}
	if len(task.Raw) != 0 {
		if err := json.Unmarshal(task.Raw, pipe); err != nil {
			return nil, err
		}
	}
	return pipe, nil
}

func NewGenerator(i interface{}) TaskGenerator {
	if _, ok := i.(Pipe); !ok {
		logrus.Panicf("%T is not a Pipe", i)
	}
	t := reflect.TypeOf(i)
	return TaskFunc(func(task *Task) (Pipe, error) {
		pipe := reflect.New(t).Interface().(Pipe)
		if err := json.Unmarshal(task.Raw, pipe); err != nil {
			return nil, err
		}
		return pipe, nil
	})
}
