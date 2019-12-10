/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package goplumber

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Plumber constructs Pipelines.
//
// The Plumber keeps a map of task types to PipeGenerators, which interpret Task
// definitions to generate Pipes. The Plumber parses the config and links these
// pipes together.
type Plumber struct {
	// Client define how the Plumber maps Tasks to Pipes.
	Clients map[string]Client
}

// SetClient configures a Plumber with a particular task type.
func (plumber Plumber) SetClient(taskType string, client Client) {
	plumber.Clients[taskType] = client
}

// SetSource configures the Plumber to supply data from a given source.
func (plumber Plumber) SetSource(taskType string, source DataSource) {
	plumber.Clients[taskType] = NewSourceClient(source)
}

// SetSink configures the Plumber to send data to a given sink.
func (plumber Plumber) SetSink(taskType string, sink Sink) {
	plumber.Clients[taskType] = NewSinkClient(sink)
}

// SetTemplateSource adds a template client under the given name.
func (plumber Plumber) SetTemplateSource(taskType string, source DataSource) {
	plumber.Clients[taskType] = NewTemplateClient(source)
}

// NewPlumber returns a new Plumber with the default task types.
//
// You can modify the Clients map to change how tasks are constructed and which
// tasks are allowed.
//
// By default, the Plumber includes the following task types:
// - http: execute an HTTPTask with the Go's default HTTP client
// - validation: validate some dat against an JSON schema
// - input: accept input from another task, as is used for Pipeline-based Clients.
func NewPlumber() Plumber {
	return Plumber{Clients: map[string]Client{
		"http":       SimpleJSONPipe(func() Pipe { return &HTTPTask{} }),
		"validation": SimpleJSONPipe(func() Pipe { return &JSONValidationTask{} }),
		"input":      SimpleJSONPipe(func() Pipe { return &InputTask{} }),
	}}
}

// PipeFunc adapts a function into a Client without any modifications.
type PipeFunc func(task *Task) (Pipe, error)

func (f PipeFunc) GetPipe(task *Task) (Pipe, error) {
	return f(task)
}

// JSONPipe adapts a function into a Client that unmarshal's the Task's
// raw data into the returned Pipe.
type JSONPipe func(task *Task) (Pipe, error)

// SimpleJSONPipe adapts functions that just return a new instance of a Pipe into
// PipeGenerators that unmarshals the task's Raw data into the Pipe.
type SimpleJSONPipe func() Pipe

func (f JSONPipe) GetPipe(task *Task) (Pipe, error) {
	pipe, err := f(task)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to create task of type '%s'",
			task.TaskType)
	}
	if len(task.Raw) != 0 {
		if err := json.Unmarshal(task.Raw, pipe); err != nil {
			return nil, errors.Wrapf(err, "failed to create task of type '%s'",
				task.TaskType)
		}
	}
	return pipe, nil
}

func (f SimpleJSONPipe) GetPipe(task *Task) (Pipe, error) {
	pipe := f()
	if len(task.Raw) != 0 {
		if err := json.Unmarshal(task.Raw, pipe); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return pipe, nil
}

// NewPipeline constructs a new Pipeline from a given PipelineConfig.
func (plumber Plumber) NewPipeline(conf *PipelineConfig) (*Pipeline, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	pipeline := &Pipeline{pipelineConfig: conf}
	if err := plumber.initPipes(pipeline); err != nil {
		return nil, err
	}

	return pipeline, nil
}

type dependType int

const (
	dLink     = dependType(iota) // "normal" dependency -- use its Task output
	dState                       // ignore the value; confirm link has a certain state
	dStart                       // instead of the Link's output, link its StartedAt time
	dComplete                    // instead of the Link's output, link its CompletedAt time
	dErr                         // instead of the Link's output, link its Err value (which may be nil)
)

// dependency encapsulates the information needed to link together tasks.
//
// Task definitions declare Links to other tasks -- values which should be included
// in its input map under a given key. The value that ends up in the map is usually
// the Task's output, but could otherwise be part of the link's status.
//
// Additionally, a Task may not need the link's output, but instead depends upon
// the link completing with a certain state.
type dependency struct {
	task  *Task      // pointer to the linked task definition
	as    string     // the link name; i.e., the key for the input map
	state State      // required state of the linked task; by default, Success
	dType dependType // how to interpret the dependency
}

// parsedTask is the result of parsing a Task definition in a Plumber's context.
//
// A parsedTask links together the task's definition, the pipe that will satisfy
// it, and all of its dependencies.
type parsedTask struct {
	task      *Task
	pipe      Pipe
	dependsOn []*dependency
}

func (plumber Plumber) initPipes(pipeline *Pipeline) error {
	taskOrder, err := sortTasks(pipeline.pipelineConfig.Tasks)
	if err != nil {
		return err
	}

	pipeline.pts = make([]*parsedTask, len(taskOrder))

	for taskIdx, task := range taskOrder {
		generator, ok := plumber.Clients[task.TaskType]
		if !ok {
			return errors.Errorf("task '%s' has unknown type '%s'",
				task.name, task.TaskType)
		}

		pipe, err := generator.GetPipe(task)
		if err != nil {
			return errors.WithMessagef(err,
				"failed to create %s pipe for task '%s'",
				task.TaskType, task.name)
		}

		pt := parsedTask{
			pipe: pipe,
			task: task,
		}

		for linkName, to := range task.Links {
			from := pipeline.pipelineConfig.Tasks[to.Source]
			from.dependants++
			d := &dependency{
				dType: dLink,
				task:  from,
				state: Success,
				as:    linkName,
			}
			pt.dependsOn = append(pt.dependsOn, d)
			if to.Using != nil {
				switch *to.Using {
				case "state":
					d.dType = dState
				case "startTime":
					d.dType = dStart
				case "completionTime":
					d.dType = dComplete
				case "error":
					d.dType = dErr
				default:
					return errors.Errorf("task '%s' link '%s' has unknown"+
						" value for using: '%s'", task.name, to.Source, *to.Using)
				}
			}
		}

		for _, s := range task.Successes {
			d := pipeline.pipelineConfig.Tasks[s]
			d.dependants++
			pt.dependsOn = append(pt.dependsOn, &dependency{
				dType: dState,
				task:  d,
				state: Success,
			})
		}
		for _, s := range task.Failures {
			d := pipeline.pipelineConfig.Tasks[s]
			d.dependants++
			pt.dependsOn = append(pt.dependsOn, &dependency{
				dType: dState,
				task:  d,
				state: Failed,
			})
		}
		pipeline.pts[taskIdx] = &pt

		logrus.WithFields(logrus.Fields{
			"taskType": task.TaskType,
			"taskName": task.name,
		}).Debugf("Initialized Pipe.")
	}
	return nil
}
