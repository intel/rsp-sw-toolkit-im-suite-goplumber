package goplumber

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"text/template"
	"time"
)

const defaultTimeoutSecs = 30

type taskMap = map[string]*Task
type Pipeline struct {
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	Trigger       Trigger  `json:"trigger"`
	Tasks         taskMap  `json:"tasks"`
	TemplateRefs  []string `json:"templateRefs"`
	TimeoutSecs   int      `json:"timeoutSeconds"`
	taskOrder     []string
	rootNamespace *template.Template
}

type Task struct {
	pipe             Pipe
	TaskType         string          `json:"type"`
	TemplateName     *string         `json:"template,omitempty"`
	Raw              json.RawMessage `json:"raw,omitempty"`
	Links            map[string]Link `json:"links,omitempty"`        // dependencies
	Successes        []string        `json:"ifSuccessful,omitempty"` // tasks that must finish, but data doesn't matter
	Failures         []string        `json:"ifFailed,omitempty"`     // not currently used
	StopIfEmpty      bool            `json:"stopIfEmpty"`
	ErrorIfEmpty     bool            `json:"errorIfEmpty"`
	DisableResultLog bool            `json:"disableResultLog"`
}

type Link struct {
	Source string  `json:"from"`
	Using  *string `json:"using,omitempty"`
	Elem   *string `json:"elem,omitempty"`
}

type Interval struct {
	Seconds int64 `json:"seconds"`
}

type Trigger struct {
	Interval Interval `json:"interval,omitempty"` // run the pipeline every N seconds
	// HTTP     bool     `json:"webhook,omitempty"`  // run when triggered by a POST call matching the pipeline name
}

// PipeStatus is used to track the status of either a pipeline or a task.
type PipeStatus struct {
	State       PipeState
	StartedAt   time.Time
	CompletedAt time.Time
	Err         error
}

// PipeState represents the current execution state of a task or pipeline.
type PipeState int

const (
	Waiting = PipeState(iota) // not running/failed
	Running
	Success
	Failed     // failed in an unrecoverable way, or exceeded retries
	Retrying   // failed, but might be able to succeed after retry
)

func (ps PipeState) MarshalJSON() ([]byte, error) {
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

func (ps PipeStatus) MarshalJSON() ([]byte, error) {
	type status struct {
		State       PipeState
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
type TaskGenerator interface {
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

// Backend implementers can load and store <key, value> pairs for a pipeline.
type Backend interface {
	DataSource
	Sink
}
