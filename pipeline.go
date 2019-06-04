package goplumber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"strings"
	"sync"
	"time"
)

type Pipe interface {
	Fill(input map[string][]byte) error             // accept data from Links
	Execute(ctx context.Context, w io.Writer) error // perform an operation and write result
}

type CachedSource struct {
	PipelineSource
	cached []byte
}

func (s *CachedSource) Get(ctx context.Context, key string) (val []byte, isDefault bool, err error) {
	if s.cached != nil {
		return s.cached, true, nil
	}
	val, isDefault, err = s.PipelineSource.Get(ctx, key)
	if err == nil {
		s.cached = val
	}
	return val, isDefault, err
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
	Failed     // failed in an unrecoverable way
	Retrying   // failed, but should retry after updating info about task
	Archiving  // updating info about a task that failed multiple times
	Archived   // task failed in a potentially recoverable way, but exceeded retries
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

type TaskType = string
type TaskTypeMap = map[TaskType]TaskGenerator
type StoreType = string
type StoreTypeMap = map[StoreType]PipelineStore

// Plumber is used to create pipelines.
type Plumber struct {
	// TemplateLoader tells the Plumber from where it should load templates.
	TemplateLoader TemplateLoader
	// TaskTypes define how the Plumber maps task types to Pipes when creating tasks.
	TaskTypes TaskTypeMap
	// StoreTypes tell the Plumber what stores to use when loading/storing data.
	StoreTypes StoreTypeMap
}

// Returns a new Plumber with the given template loader and default task types.
//
// You can modify the TaskTypes map to change how tasks are constructed (and which
// tasks are allowed). Modify the StoreTypes to add or remove storage backends
// for load/store tasks.
func NewPlumber(tl TemplateLoader, storeTypeMap StoreTypeMap) Plumber {
	var defaultGenerators = map[StoreType]TaskGenerator{
		"http":       SimpleTask(func() Pipe { return &HTTPTask{} }),
		"mqtt":       SimpleTask(func() Pipe { return NewMQTTTask() }),
		"json":       SimpleTask(func() Pipe { return &JSONTask{} }),
		"validation": SimpleTask(func() Pipe { return &JSONValidationTask{} }),
		"secret": SimpleTask(func() Pipe {
			return &LoadStoreTask{
				store:  storeTypeMap["secrets"],
				isLoad: false,
			}
		}),
		"put": SimpleTask(func() Pipe {
			return &LoadStoreTask{
				store:  storeTypeMap["memory"],
				isLoad: false,
			}
		}),
		"get": TaskFunc(func(t *Task) (Pipe, error) {
			p, err := SimpleTask(func() Pipe {
				return &LoadStoreTask{
					store:  storeTypeMap["memory"],
					isLoad: false,
				}
			}).GetPipe(t)
			if err != nil {
				return nil, err
			}
			lst := p.(*LoadStoreTask)
			store, ok := storeTypeMap[lst.Source]
			if !ok {
				return nil, errors.Errorf("unknown store '%s'", lst.Source)
			}
			lst.store = store
			return lst, nil
		}),
	}

	p := Plumber{
		TemplateLoader: tl,
		TaskTypes:      defaultGenerators,
		StoreTypes:     StoreTypeMap{},
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

type TaskFunc func(t *Task) (Pipe, error)

func (f TaskFunc) GetPipe(task *Task) (Pipe, error) {
	pipe, err := f(task)
	if err != nil {
		return pipe, err
	}

	if len(task.Raw) != 0 {
		if err := json.Unmarshal(task.Raw, pipe); err != nil {
			return nil, err
		}
	}
	return pipe, err
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

	taskOrder, err := sortTasks(p.Tasks)
	if err != nil {
		return p, err
	}
	p.taskOrder = taskOrder

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
	for taskName, task := range p.Tasks {
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
			if _, ok := p.Tasks[link.Source]; !ok {
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
			if _, ok := p.Tasks[name]; !ok {
				return errors.Errorf("in pipeline %s, task '%s' depends "+
					"on success of unknown task '%s'", p.Name, taskName, name)
			}
		}

		for _, name := range task.Failures {
			if name == "" {
				return errors.Errorf("in pipeline %s, task '%s' has an "+
					"empty 'failure' dependency", p.Name, taskName)
			}
			if _, ok := p.Tasks[name]; !ok {
				return errors.Errorf("in pipeline %s, task '%s' depends "+
					"on failure of unknown task '%s'", p.Name, taskName, name)
			}
		}
	}

	return nil
}

func (p *Pipeline) initTasks2(plumber Plumber) error {
	for taskName, task := range p.Tasks {
		if task.TaskType == "template" {
			// handle templates a bit differently -- template names MUST be 'raw',
			// and we use the root namespace associated with the pipeline.
			ts, err := NewTemplateTask(task.Raw, p.rootNamespace, plumber.TemplateLoader)
			if err != nil {
				return errors.Wrapf(err, "pipeline %s failed to create "+
					"template task '%s'", p.Name, taskName)
			}
			task.pipe = ts
		}

		generator, ok := plumber.TaskTypes[task.TaskType]
		if !ok {
			return errors.Errorf("pipeline %s defines task '%s' with "+
				"unknown type '%s'", p.Name, taskName, task.TaskType)
		}

		var err error
		task.pipe, err = generator.GetPipe(task)
		if err != nil {
			return errors.Errorf("pipeline %s failed to create %s task named '%s'",
				p.Name, task.TaskType, taskName)
		}

		logrus.
			WithField("pipeline", p.Name).
			WithField("taskType", task.TaskType).
			WithField("taskName", taskName).
			Debugf("initialized task")
	}
	return nil
}

// initTasks initializes the task instances from their raw configurations.
func (p *Pipeline) initTasks(pcon PipelineConnector) error {
	for taskName, task := range p.Tasks {
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

		task, ok := p.Tasks[taskName]
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

const truncateOutputAt = 1000 // bytes
func logTaskResult(taskName string, task *Task, content []byte) {
	if task.DisableResultLog {
		return
	}

	if len(content) == 0 {
		logrus.Debugf("Task '%s' returned no content", taskName)
		return
	}
	if len(content) < truncateOutputAt {
		logrus.Debugf("Result for task '%s': %s", taskName, content)
		return
	}

	logrus.Debugf("Result for task '%s' (truncated from %d bytes): %s...",
		taskName, len(content), content[:truncateOutputAt])
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

// determine whether a task should execute based on its conditions.
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

// bufferPool maintains a pool of reusable byte buffers.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// withBuffer handles executing a function that needs a temporary byte buffer.
//
// It automatically pulls a buffer from the buffer pool and ensures the buffer
// is returned when the function's execution completes, even if the function
// panics.
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
