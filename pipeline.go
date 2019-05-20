package goplumber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"
)

type Pipeline struct {
	Name          string           `json:"name"`
	Description   string           `json:"description"`
	Trigger       Trigger          `json:"trigger"`
	TaskMap       map[string]*Task `json:"tasks"`
	TemplateRefs  []string         `json:"templateRefs"`
	TimeoutSecs   int              `json:"timeoutSeconds"`
	taskOrder     []string
	rootNamespace *template.Template
}

type Task struct {
	finishTime       int
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

type Pipe interface {
	Fill(input map[string][]byte) error             // accept data from Links
	Execute(ctx context.Context, w io.Writer) error // perform an operation and write result
}

type Interval struct {
	Seconds int64 `json:"seconds"`
}

type Trigger struct {
	Interval Interval `json:"interval,omitempty"` // run the pipeline every N seconds
	HTTP     bool     `json:"webhook,omitempty"`  // run when triggered by a POST call matching the pipeline name
}

// PipeStatus is used to track the status of either a pipeline or a task.
type PipeStatus struct {
	State       PipeState
	StartedAt   time.Time
	CompletedAt time.Time
	Err         error
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

// PipeState represents the current execution state of a task or pipeline.
type PipeState int

const (
	Waiting = PipeState(iota) // not yet scheduled
	Running
	Success
	Failed    // failed in an unrecoverable way
	Retrying  // failed, but should retry after updating info about task
	Archiving // updating info about a task that failed multiple times
	Archived  // task failed in a potentially recoverable way, but exceeded retries
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
	}
	return nil, errors.New("unknown state")
}

type PipelineConnector struct {
	TemplateLoader TemplateLoader
	KVData         PipelineStore
	Secrets        PipelineStore
}

// TemplateLoader loads templates.
type TemplateLoader interface {
	// LoadTemplate returns the template data for a given template reference.
	LoadTemplateNamespace(id string) (string, error)
}

// PipelineStore gets and stores key, value pairs for a pipeline.
type PipelineStore interface {
	// Get returns data from the Store, or possibly a default value if the key
	// wasn't present. If the Store returns a default, it should indicate this
	// by returning false for wasPresent. If there was a problem loading the
	// value, as might be the case for external stores, it should return an error.
	Get(ctx context.Context, key string) (data []byte, wasPresent bool, err error)
	Put(ctx context.Context, key string, value []byte) error
}

func NewPipeline(config []byte, pcon PipelineConnector) (Pipeline, error) {
	p := Pipeline{}
	if err := json.Unmarshal(config, &p); err != nil {
		return p, errors.Wrap(err, "failed to unmarshal pipeline config")
	}
	if p.Name == "" {
		return p, errors.New("pipeline must have a name")
	}
	if err := p.loadTemplates(pcon.TemplateLoader); err != nil {
		return p, err
	}
	if err := p.checkTasks(); err != nil {
		return p, err
	}
	if err := p.initTasks(pcon); err != nil {
		return p, err
	}
	if err := p.sortTasks(); err != nil {
		return p, err
	}

	return p, nil
}

// LoadTemplates uses a given loader to load the pipeline's templates.
func (p *Pipeline) loadTemplates(loader TemplateLoader) error {
	tmpl, err := baseTmpl.Clone()
	if err != nil {
		return errors.Wrapf(err, "unable to clone base template")
	}
	p.rootNamespace = tmpl

	for idx, id := range p.TemplateRefs {
		if id == "" {
			return errors.Errorf("template reference #%d has no ID", idx)
		}
		tmplVal, err := loader.LoadTemplateNamespace(id)
		if err != nil {
			return err
		}
		if _, err := p.rootNamespace.
			Option("missingkey=error").
			Parse(tmplVal); err != nil {
			return err
		}
	}

	// make sure all declared templates are defined
	var missing []string
	var allTmpls []string
	for _, tmpl := range p.rootNamespace.Templates() {
		if p.rootNamespace.Lookup(tmpl.Name()) == nil {
			missing = append(missing, tmpl.Name())
		}
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			allTmpls = append(allTmpls, tmpl.Name())
		}
	}
	logrus.WithField("defined templates", allTmpls).
		WithField("missing", missing).
		WithField("pipeline", p.Name).
		Debug("Parsing complete.")
	if len(missing) != 0 {
		return errors.Errorf("missing template definitions for: %s",
			strings.Join(missing, ", "))
	}

	return nil
}

// CheckTasks verifies that task definitions are valid.
//
// It checks the tasks all have a name, that all links have a matching task,
// and all named templates are loaded.
func (p *Pipeline) checkTasks() error {
	for taskName, task := range p.TaskMap {
		if taskName == "" {
			return errors.New("all tasks must have a name")
		}

		if task.TemplateName != nil {
			tmplName := *task.TemplateName
			if tmplName == "" {
				return errors.Errorf(
					"in pipeline %s, task '%s' references a template with "+
						"an empty name", p.Name, taskName)
			}
			if p.rootNamespace.Lookup(tmplName) == nil {
				return errors.Errorf("in pipeline %s, task '%s' references "+
					"template '%s', but it's not defined in any of the loaded templates: [%s]",
					p.Name, taskName, tmplName, strings.Join(p.TemplateRefs, ", "))
			}
		}

		for linkName, link := range task.Links {
			if linkName == "" {
				return errors.Errorf("in pipeline %s, task '%s' has link "+
					"without a name", p.Name, taskName)
			}
			if link.Source == "" {
				return errors.Errorf("in pipeline %s, task '%s' has link "+
					"link '%s' without a source", p.Name, taskName, linkName)
			}
			if _, ok := p.TaskMap[link.Source]; !ok {
				return errors.Errorf("in pipeline %s, task '%s' has link "+
					"'%s' with unknown source '%s'", p.Name, taskName, linkName, link.Source)
			}
			if link.Elem != nil && *link.Elem == "" {
				return errors.Errorf("in pipeline %s, task '%s' references "+
					"link '%s' to '%s' with an empty elem name",
					p.Name, taskName, linkName, link.Source)
			}
		}

		for _, name := range task.Successes {
			if name == "" {
				return errors.Errorf("in pipeline %s, task '%s' has an "+
					"empty 'success' dependency", p.Name, taskName)
			}
			if _, ok := p.TaskMap[name]; !ok {
				return errors.Errorf("in pipeline %s, task '%s' depends "+
					"on success of unknown task '%s'", p.Name, taskName, name)
			}
		}

		for _, name := range task.Failures {
			if name == "" {
				return errors.Errorf("in pipeline %s, task '%s' has an "+
					"empty 'failure' dependency", p.Name, taskName)
			}
			if _, ok := p.TaskMap[name]; !ok {
				return errors.Errorf("in pipeline %s, task '%s' depends "+
					"on failure of unknown task '%s'", p.Name, taskName, name)
			}
		}
	}

	return nil
}

// initTasks initializes the task instances from their raw configurations.
func (p *Pipeline) initTasks(pcon PipelineConnector) error {
	for taskName, task := range p.TaskMap {
		hasRaw := len(task.Raw) != 0
		switch task.TaskType {
		case "template":
			// handle templates a bit differently -- template names MUST be 'raw',
			// and we use the root namespace associated with the pipeline.
			ts, err := NewTemplateTask(task.Raw, p.rootNamespace, pcon.TemplateLoader)
			if err != nil {
				return err
			}
			task.pipe = ts
		case "http":
			// todo: allow configuring the http client
			task.pipe = &HTTPTask{}
		case "mqtt":
			task.pipe = NewMQTTTask()
		case "json":
			task.pipe = &JSONTask{}
		case "validation":
			task.pipe = &JSONValidationTask{}
		case "secret":
			task.DisableResultLog = true
			task.pipe = &LoadStoreTask{
				store:  pcon.Secrets,
				isLoad: true,
			}
		case "get", "put":
			task.pipe = &LoadStoreTask{
				store:  pcon.KVData,
				isLoad: task.TaskType == "get",
			}
		default:
			return errors.Errorf("pipeline %s has unknown task type '%s'",
				p.Name, task.TaskType)
		}
		if hasRaw {
			if err := json.Unmarshal(task.Raw, task.pipe); err != nil {
				return errors.Wrapf(err, "pipeline %s failed to init %s task '%s'",
					p.Name, task.TaskType, taskName)
			}
		}
		logrus.WithField("pipeline", p.Name).
			Debugf("initialized %s task '%s'",
				task.TaskType, taskName)
	}
	return nil
}

// sortTasks sets the taskOrder based on their dependencies.
//
// This returns an error if there are circular dependencies (i.e., the dependency
// graph must form a DAG).
//
// Technically, any topological sort will do, but we're better off if we can
// spread dependent tasks out as far as possible, especially if we add parallelism.
// We can use something like Coffman-Graham to do so, but it's an over-optimization
// at the moment.
//
// NOTE: This method assumes that the pipeline's Links have already been verified;
// if Links declare dependent tasks that don't exist, then this method will panic.
func (p *Pipeline) sortTasks() error {
	p.taskOrder = make([]string, 0, len(p.TaskMap))

	for taskName, task := range p.TaskMap {
		p.taskOrder = append(p.taskOrder, taskName)
		_, dag := p.getFinishTime(task)
		if !dag {
			return errors.Errorf("pipeline %s has task '%s' as part of a cycle",
				p.Name, taskName)
		}
	}

	sort.Slice(p.taskOrder, func(t1, t2 int) bool {
		return p.TaskMap[p.taskOrder[t1]].finishTime < p.TaskMap[p.taskOrder[t2]].finishTime
	})
	return nil
}

// getFinishTime returns the number of tasks which must complete for this one to
// finish, including the task itself, along with a bool indicating whether or not
// the task is part of a cycle, in which case the finish time is undetermined
// and the second return value is false.
//
// The minimum finish time for any task is 1, provided the graph has no cycles.
func (p *Pipeline) getFinishTime(t *Task) (int, bool) {
	if t.finishTime == -1 {
		return -1, false
	}
	if t.finishTime != 0 {
		return t.finishTime, true
	}
	t.finishTime = -1
	maxChild := 0

	for _, child := range t.Links {
		childFinish, dag := p.getFinishTime(p.TaskMap[child.Source])
		if !dag {
			return childFinish, false
		}
		if childFinish > maxChild {
			maxChild = childFinish
		}
	}
	for _, child := range t.Successes {
		childFinish, dag := p.getFinishTime(p.TaskMap[child])
		if !dag {
			return childFinish, false
		}
		if childFinish > maxChild {
			maxChild = childFinish
		}
	}
	for _, child := range t.Failures {
		childFinish, dag := p.getFinishTime(p.TaskMap[child])
		if !dag {
			return childFinish, false
		}
		if childFinish > maxChild {
			maxChild = childFinish
		}
	}
	t.finishTime = 1 + maxChild
	return t.finishTime, true
}

const defaultTimeout = 30

func getRemaining(ctx context.Context) time.Duration {
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(time.Second * time.Duration(defaultTimeout))
	}

	remaining := time.Until(deadline)
	if remaining.Seconds() < 0 {
		return 0
	}
	return remaining
}

func (p Pipeline) Execute(ctx context.Context) error {
	if p.TimeoutSecs < 1 {
		logrus.WithField("pipeline", p.Name).Debugf("Pipeline '%s' has no configured timeout; using default %d",
			p.Name, defaultTimeout)
		p.TimeoutSecs = defaultTimeout
	}

	timeout := time.Duration(p.TimeoutSecs) * time.Second
	pCtx, curCancel := context.WithTimeout(ctx, timeout)
	defer curCancel()

	logrus.WithField("pipeline", p.Name).Debugf("Starting pipeline '%s' with timeout %ds",
		p.Name, p.TimeoutSecs)

	// make a map for outputs from earlier stages used in later ones
	dataMap := make(map[string][]byte)
	statusMap := make(map[string]*PipeStatus)

	// need a buffer for each template tasks (for the output, that is).
	inUseBuffers := make([]*bytes.Buffer, 0)
	defer func() {
		for _, b := range inUseBuffers {
			b.Reset()
			bufferPool.Put(b)
		}
	}()

	for idx, taskName := range p.taskOrder {
		if err := pCtx.Err(); err != nil {
			return errors.Wrapf(err, "pipeline %s canceled", p.Name)
		}

		task, ok := p.TaskMap[taskName]
		if !ok {
			// really shouldn't be possible, but still we'll check
			return errors.Errorf("pipeline %s missing task named '%s'",
				p.Name, taskName)
		}

		if shouldRun, err := p.shouldExecute(task, statusMap); !shouldRun {
			if err != nil {
				return err
			}
			logrus.WithField("pipeline", p.Name).
				Debugf("Skipping task %d: '%s'", idx, taskName)
			continue
		}

		logrus.WithField("pipeline", p.Name).
			Debugf("Preparing input map for task %d: '%s'", idx, taskName)
		if err := p.addLinkedInput(task, dataMap, statusMap); err != nil {
			return err
		}

		buff := bufferPool.Get().(*bytes.Buffer)
		inUseBuffers = append(inUseBuffers, buff)
		status := &PipeStatus{State: Running, StartedAt: time.Now().UTC()}
		statusMap[taskName] = status

		logrus.WithField("pipeline", p.Name).
			Debugf("Starting task %d '%s' at %v", idx, taskName, status.StartedAt)
		err := task.pipe.Execute(pCtx, buff)
		status.CompletedAt = time.Now().UTC()
		if err != nil {
			status.State = Failed
			// TODO: handle retries; allow failures to 'continue' to execute tasks allowed to do so
			return err
		} else {
			status.State = Success
		}

		// TODO: Commit the intermediate result for easier recovery processing.
		// TODO: Update & save the current task result in a database.

		content := buff.Bytes()
		logTaskResult(taskName, task, content)
		if len(content) == 0 {
			if task.ErrorIfEmpty {
				return errors.Errorf("pipeline %s's task '%s' returned no "+
					"content and is configured to consider this an error", p.Name, taskName)
			}
			if task.StopIfEmpty {
				logrus.WithField("pipeline", p.Name).
					Debugf("Stopping at task '%s' because its result "+
						"is empty and it's configured to stop in such a case.", taskName)
				return nil
			}
		}

		dataMap[taskName] = content
		// TODO: For tasks with output that won't be used later (i.e., no later
		//  tasks declare it a "link"), we can drop it from the dataMap and return
		//  the buffer to the buffer pool.
		// TODO: Some tasks (e.g., JSON tasks) hold on to (potentially large)
		//   chunks of memory -- we should set these to nil so the GC can reclaim it.
		//   Need a clear/automatic way to do this (i.e., have them only 'store'
		//   their memory in the byte buffers managed by this run)
	}

	return nil
}

func logTaskResult(taskName string, task *Task, content []byte) {
	if task.DisableResultLog {
		return
	}

	if len(content) == 0 {
		logrus.Debugf("Task '%s' returned no content", taskName)
		return
	}
	if len(content) < 10000000 {
		logrus.Debugf("Result for task '%s': %s", taskName, content)
		return
	}

	logrus.Debugf("Result for task '%s' (truncated from %d bytes): %s...",
		taskName, len(content), content[:100])
}

var emptyDataMap = map[string][]byte{}

// addLinkedInput passes the result of Linked tasks to this Task's Fill method.
func (p *Pipeline) addLinkedInput(task *Task, dataMap map[string][]byte, statusMap map[string]*PipeStatus) error {
	if len(task.Links) == 0 {
		logrus.WithField("pipeline", p.Name).Debug("Task has no links.")
		return task.pipe.Fill(emptyDataMap)
	}

	inputMap := map[string][]byte{}
	for varName, link := range task.Links {
		logrus.WithField("pipeline", p.Name).Debugf("Linking '%s' from %s", varName, link.Source)

		var val []byte
		var err error
		var ok bool
		switch {
		case link.Using == nil:
			val, ok = dataMap[link.Source]
			if !ok {
				// this should be prevented by earlier checks, but just to be safe...
				return errors.Errorf("pipeline %s missing data from link '%s'",
					p.Name, link.Source)
			}
		case *link.Using == "status":
			// todo: benchmark keeping a cache of these marshaled statuses as needed
			//   or marking a Task to indicate its status is a Link for another Task.
			status := statusMap[link.Source]
			logrus.WithField("pipeline", p.Name).Debugf("Linking status of '%s' as '%s': %+v",
				link.Source, varName, status)
			val, err = json.Marshal(status)
		default:
			err = errors.Errorf("unknown value for 'using': '%s'", *link.Using)
		}
		if err != nil {
			return err
		}

		if link.Elem != nil {
			logrus.WithField("pipeline", p.Name).
				Debugf("Extracting linked var %s using elem %s.", varName, *link.Elem)
			if *link.Elem == "" {
				// again, this is just secondary validation for something that shouldn't happen
				return errors.Errorf("link to '%s' declares 'elem', but has no element name",
					link.Source)
			}

			// treat the link as a map & try to extract the element
			// todo: cache these
			var linkedMap map[string]json.RawMessage
			if err := json.Unmarshal(val, &linkedMap); err != nil {
				return errors.Wrapf(err, "link source '%s' cannot be "+
					"unmarshaled to extract elem '%s'", link.Source, *link.Elem)
			}
			logrus.WithField("pipeline", p.Name).
				Debugf("Successfully unmarshaled var %s from link %s for elem %s.",
					varName, link.Source, *link.Elem)

			val, ok = linkedMap[*link.Elem]
			if !ok {
				// this one we can't check this ahead of time
				return errors.Errorf("elem '%s' not found in linked source '%s'",
					*link.Elem, link.Source)
			}
			logrus.WithField("pipeline", p.Name).
				Debugf("Successfully got value for link %s for elem %s: %s",
					link.Source, *link.Elem, val)
		}

		logrus.WithField("pipeline", p.Name).Debugf("Adding %s to input map.", varName)
		inputMap[varName] = val
	}

	logrus.Debug("Filling task...")
	return task.pipe.Fill(inputMap)
}

func (p *Pipeline) shouldExecute(task *Task, statuses map[string]*PipeStatus) (bool, error) {
	for _, t := range task.Successes {
		s, ok := statuses[t]
		if !ok {
			// shouldn't happen, but just in case
			return false, errors.Errorf("task requires success for '%s', but "+
				"it has not yet completed", t)
		}
		if s.State != Success {
			logrus.WithField("pipeline", p.Name).
				Debugf("Task requires '%s' to complete successfully, "+
					"but it has state %v", t, s.State)
			return false, nil
		}
	}
	for _, t := range task.Failures {
		s, ok := statuses[t]
		if !ok {
			// shouldn't happen, but just in case
			return false, errors.Errorf("task requires failure for '%s', but "+
				"it has not yet completed", t)
		}
		if s.State != Failed {
			logrus.WithField("pipeline", p.Name).
				Debugf("Task requires '%s' to fail, but it has state %v", t, s.State)
			return false, nil
		}
	}
	return true, nil
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func withBuffer(f func(buf *bytes.Buffer)) {
	b := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		b.Reset()
		bufferPool.Put(b)
	}()
	f(b)
}

func isPermanentPipeError(err error) bool {
	if err == nil {
		return false
	}
	pErr, isPipeError := err.(pipeError)
	return isPipeError && pErr.isPermanent
}

type pipeError struct {
	error
	isPermanent bool
}

// permanent wraps an error in a permanent pipeError.
//
// If the error is nil, this returns nil. If the error is already a pipeError,
// its original inner error is used for the returned value.
//
// permanent errors require manual intervention to resolve and thus there is no
// need to retry.
func permanent(err error) error {
	if err == nil {
		return nil
	}
	if pErr, ok := err.(pipeError); ok {
		return pipeError{
			error:       pErr.error,
			isPermanent: true,
		}
	}
	return pipeError{
		error:       err,
		isPermanent: true,
	}
}

// transient wraps a non-permanent error in a transient pipeError.
//
// If the error is nil, this returns nil. If the error is already a pipeError,
// its original inner error is used for the returned value. If that error was
// marked permanent, it will still be marked permanent.
//
// transient errors have a chance of resolving on their own, and so it may be
// reasonable to retry the operation.
func transient(err error) error {
	if err == nil {
		return nil
	}
	if pErr, ok := err.(pipeError); ok {
		if pErr.isPermanent {
			return pErr
		}
		return pipeError{
			error:       pErr.error,
			isPermanent: false,
		}
	}
	return pipeError{
		error:       err,
		isPermanent: false,
	}
}
