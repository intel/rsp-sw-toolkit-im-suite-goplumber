package goplumber

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"time"
)

const defaultTimeoutSecs = 30

type Settings struct {
	Trigger       Trigger `json:"trigger"`
	DefaultOutput *string `json:"defaultOutput"`
}

type PipelineConfig struct {
	Name          string  `json:"name"`
	Description   string  `json:"description"`
	Trigger       Trigger `json:"trigger"`
	Tasks         taskMap `json:"tasks"`
	TimeoutSecs   *int    `json:"timeoutSeconds,omitempty"`
	DefaultOutput *string `json:"defaultOutput"`
}

// Task tells a Plumber how it should construct a specific Pipe.
//
// Just as Blueprints are configuration for Pipelines, Tasks are configuration
// for Pipes. It's up to the Plumber to interpret how the Task should be satisfied.
//
// Tasks can be directly unmarshaled from JSON.
type Task struct {
	TaskType         string          `json:"type"`
	Raw              json.RawMessage `json:"raw,omitempty"`
	Links            map[string]Link `json:"links,omitempty"`        // dependencies
	Successes        []string        `json:"ifSuccessful,omitempty"` // tasks that must finish, but data doesn't matter
	Failures         []string        `json:"ifFailed,omitempty"`     // not currently used
	StopIfEmpty      bool            `json:"stopIfEmpty"`
	ErrorIfEmpty     bool            `json:"errorIfEmpty"`
	DisableResultLog bool            `json:"disableResultLog"`
	ContinueOnError  bool            `json:"continueOnError"`
	name             string // used for logging
	dependants       int    // number of task that depend on this one
}

type taskMap = map[string]*Task
type linkMap = map[string][]byte
type Link struct {
	Source string  `json:"from"`
	Using  *string `json:"using,omitempty"`
}

type Interval struct {
	Milliseconds int64 `json:"milliseconds"`
	Seconds      int64 `json:"seconds"`
	Minutes      int64 `json:"minutes"`
	Hours        int64 `json:"hours"`
}

// Duration returns the duration represented by this Interval.
func (i Interval) Duration() time.Duration {
	return time.Duration(i.Milliseconds)*time.Millisecond +
		time.Duration(i.Seconds)*time.Second +
		time.Duration(i.Minutes)*time.Minute +
		time.Duration(i.Hours)*time.Hour
}

func (t *Task) Dependencies(f func(taskName string)) {
	for _, l := range t.Links {
		f(l.Source)
	}
	for _, s := range t.Successes {
		f(s)
	}
	for _, fail := range t.Failures {
		f(fail)
	}
}

type Trigger struct {
	Interval Interval `json:"interval,omitempty"` // run the pipeline every N intervals
	// HTTP     bool     `json:"webhook,omitempty"`  // run when triggered by a POST call matching the pipeline name
}

// Status tracks the status of a Pipeline or Job.
type Status struct {
	State       State
	StartedAt   time.Time
	CompletedAt time.Time
	Err         error
	Attempts    int // number of times executed
}

func (s *Status) logResult(pipelineName string) {
	logrus.WithFields(logrus.Fields{
		"pipeline": pipelineName,
		"status":   s.State,
		"duration": s.Duration(),
		"error":    s.Err,
	}).Debug("Pipeline complete.")
}

func (s *Status) start() {
	if s.State == Waiting {
		s.State = Running
		s.StartedAt = time.Now().UTC()
	} else {
		s.State = Retrying
	}
	s.Attempts++
}

func (s *Status) complete(err error) {
	if err != nil {
		s.State = Failed
		s.Err = errors.WithMessagef(err, "failed after #%d attempts", s.Attempts)
	} else {
		s.State = Success
	}
	s.CompletedAt = time.Now().UTC()
}

// ExecutionTime returns the Duration of time between CompletedAt and StartedAt.
//
// If CompletedAt.IsZero() is true (likely implying that the State is Waiting),
// then this method returns a Duration of 0.
func (s *Status) Duration() time.Duration {
	if s.CompletedAt.IsZero() {
		return 0
	}
	return s.CompletedAt.Sub(s.StartedAt)
}

// State represents the current execution state of a task or pipeline.
type State int

const (
	Waiting = State(iota) // not running/failed
	Running
	Success
	Failed    // failed in an unrecoverable way, or exceeded retries
	Retrying  // failed, but might be able to succeed after retry
)

func (ps State) String() string {
	switch ps {
	case Waiting:
		return "Waiting"
	case Running:
		return "Running"
	case Success:
		return "Success"
	case Failed:
		return "Failed"
	case Retrying:
		return "Retrying"
	}
	return "<unknown pipe state>"
}

func (ps State) MarshalJSON() ([]byte, error) {
	switch ps {
	case Waiting:
		return []byte(`"Waiting"`), nil
	case Running:
		return []byte(`"Running"`), nil
	case Success:
		return []byte(`"Success"`), nil
	case Failed:
		return []byte(`"Failed"`), nil
	case Retrying:
		return []byte(`"Retrying"`), nil
	}
	return nil, errors.New("unknown state")
}

func (ps Status) MarshalJSON() ([]byte, error) {
	type status struct {
		State       State
		StartedAt   int64
		CompletedAt int64
		Err         string
	}
	return json.Marshal(status{
		State:       ps.State,
		StartedAt:   ps.StartedAt.UnixNano() / 1e6,
		CompletedAt: ps.CompletedAt.UnixNano() / 1e6,
		Err:         fmt.Sprintf("%s", ps.Err),
	})
}

// TaskGenerator generates Pipes from Task definitions.
type Client interface {
	GetPipe(task *Task) (Pipe, error)
}

// DataSource returns data from a source, or possibly a default value if the
// key wasn't present. If the source returns a default, it should indicate this
// by returning `false` for wasPresent.
type DataSource interface {
	Get(ctx context.Context, key string) (data []byte, wasPresent bool, err error)
}

// Sink accepts a single <key, value> pair, presumably to store it.
type Sink interface {
	Put(ctx context.Context, key string, value []byte) error
}

// SendTo is a convenience wrapper to send a string value to a Sink without
// managing the context.
func SendTo(s Sink, key, value string) error {
	ctx := context.Background()
	return s.Put(ctx, key, []byte(value))
}

// Validate a PipelineConfig by ensuring all required fields are set, all
// references have a known referent, and the task graph is acyclic.
func (pc *PipelineConfig) Validate() error {
	if pc.Name == "" {
		return errors.New("pipeline must have a name")
	}

	var taskName string
	var task *Task
	invalid := func(message string, args ...interface{}) error {
		args = append([]interface{}{pc.Name, taskName}, args...)
		return errors.Errorf("error validating pipeline %s, task '%s': "+message,
			args...)
	}

	for taskName, task = range pc.Tasks {
		if taskName == "" {
			return errors.New("all tasks must have a name")
		}

		for linkName, link := range task.Links {
			if linkName == "" {
				return invalid("task declares a link without a name")
			}
			if link.Source == "" {
				return invalid("task declares a link without a source")
			}
			if _, ok := pc.Tasks[link.Source]; !ok {
				return invalid("task declares a link with an unknown source '%s'",
					link.Source)
			}
		}

		for _, name := range task.Successes {
			if name == "" {
				return invalid("task has an empty 'success' dependency")
			}
			if _, ok := pc.Tasks[name]; !ok {
				return invalid("task depends on the success of the "+
					"unknown task '%s'", name)
			}
		}
	}

	if pc.DefaultOutput != nil {
		if _, ok := pc.Tasks[*pc.DefaultOutput]; !ok {
			return errors.Errorf("pipeline declares the default output "+
				"task '%s', but no task has that name", *pc.DefaultOutput)
		}
	}

	// validate that tasks have a partial order
	_, err := sortTasks(pc.Tasks)
	if err != nil {
		return errors.WithMessagef(err,
			"unable to establish a partial order on pipeline %s", pc.Name)
	}

	return nil
}
