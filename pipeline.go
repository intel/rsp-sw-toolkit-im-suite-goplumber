package goplumber

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

// Pipe implementers handle the work required to execute a Task.
type Pipe interface {
	// Execute executes an operation and writes its result to the given writer.
	// If the Pipe doesn't have a result, it may choose to simply not write
	// anything. An empty result isn't typically considered an error, but a Task
	// may choose to view an empty result as an error explicitly. The input map
	// represents data from Links, mapped as linkName: pipeOutput.
	Execute(ctx context.Context, w io.Writer, input map[string][]byte) error
}

// Pipeline is a series of Pipes through which data can flow.
//
// Pipelines hold on to a reference to their underlying PipelineConfig. Multiple
// Pipelines can share the same PipelineConfig, and although you can execute a
// Pipeline concurrently multiple times, you should not modify the PipelineConfig
// while a Pipeline using it is running.
type Pipeline struct {
	pipelineConfig *PipelineConfig
	pts            []*parsedTask
}

// PipelineContext represents the current execution state of a Pipeline.
//
// Every time a Pipeline is executed, a new PipelineContext is created for it,
// used to store the status and output of the various Pipes during execution.
type PipelineContext struct {
	baseEntry  *logrus.Entry
	output     *job
	jobs       []*job
	jobsByTask map[*Task]*job
	status     *Status
}

func newPipeContext(pline *Pipeline) *PipelineContext {
	pctx := &PipelineContext{
		status:     &Status{},
		jobsByTask: make(map[*Task]*job, len(pline.pts)),
		jobs:       make([]*job, len(pline.pts)),
		baseEntry: logrus.WithFields(logrus.Fields{
			"pipelineName": pline.pipelineConfig.Name,
		}),
	}

	for idx, ptask := range pline.pts {
		j := &job{
			parsedTask: ptask,
			dependants: ptask.task.dependants,
		}
		pctx.jobs[idx] = j
		pctx.jobsByTask[ptask.task] = j
	}
	return pctx
}

// job is a single pipe's execution status.
type job struct {
	*parsedTask
	status     *Status
	buffer     *bytes.Buffer // a buffer is only allocated while/if their are dependants
	result     []byte        // result is backed by the buffer, and only valid if buffer is not nil
	dependants int           // this drops as dependencies take the job's output
}

// InputTask serves input to Pipeline-based task types.
//
// Optionally, Input tasks can include default values.
type InputTask struct {
	Default json.RawMessage `json:"default"`
}

func (it *InputTask) Execute(ctx context.Context, w io.Writer, input linkMap) error {
	var err error
	if it.Default != nil {
		_, err = w.Write(it.Default)
	}
	return err
}

// PipelineTask allows you to use another Pipeline as if it were its own Task type.
//
// The output of the Pipeline is the output of the task with name "OutputTask".
// Inputs may come from links, as usual, or may be given as "Defaults".
type PipelineTask struct {
	OutputTask string                     `json:"outputTask"`
	Inputs     map[string]json.RawMessage `json:"inputs"`
	pipeline   *Pipeline
}

// NewTaskType returns a Client that can be used directly as new types.
//
// The resulting generator generates Pipes that, when executed, execute the
// underlying Pipeline, making it possible to create complex new task types
// simply by composing existing pipeline definitions.
func NewTaskType(pipeline *Pipeline) (Client, error) {
	// create a closure for the default name of the task that provides output
	var defaultOutput string
	if pipeline.pipelineConfig.DefaultOutput == nil {
		logrus.Warnf("task type based on pipeline '%s' does not declare a "+
			"default output; calling tasks must set one, or their creation will "+
			"fail; consider setting 'defaultOutput' to one of the pipeline's task names",
			pipeline.pipelineConfig.Name)
	} else {
		defaultOutput = *pipeline.pipelineConfig.DefaultOutput
		if _, ok := pipeline.pipelineConfig.Tasks[defaultOutput]; !ok {
			return nil, errors.Errorf("task type based on pipeline '%s' "+
				"declares default output comes from a task named '%s', but no "+
				"task in the pipeline has that name",
				pipeline.pipelineConfig.Name, defaultOutput)
		}
	}

	// return a closure that generates new Pipes with the values found above
	return PipeFunc(func(task *Task) (Pipe, error) {
		plTask := &PipelineTask{
			pipeline:   pipeline,
			OutputTask: defaultOutput,
		}

		if task.Raw != nil {
			if err := json.Unmarshal(task.Raw, plTask); err != nil {
				return nil, errors.Wrap(err, "unable to unmarshal task")
			}
		}

		// make sure the Task isn't missing any required inputs
		var missing []string
		for _, pTask := range pipeline.pts {
			// all of the Input tasks must either have a default value...
			iTask, ok := pTask.pipe.(*InputTask)
			if !ok || iTask.Default != nil {
				continue
			}
			// ...or the calling task needs to set a value via inputs or links
			// TODO: come up with better names... for references:
			//    task - incoming/calling Task configuration/definition that
			//      we're evaluating
			//    pTask - one of the parsed tasks in the "sub" pipeline
			//    plTask - the new task/pipe we're generating which will
			//      satisfy callers by executing the "sub" pipeline
			inputName := pTask.task.name
			_, isProvided := plTask.Inputs[inputName]
			_, isLinked := task.Links[inputName]
			if !(isProvided || isLinked) {
				missing = append(missing, inputName)
			}
		}
		if len(missing) > 0 {
			return nil, errors.Errorf("task is missing required inputs: %s",
				strings.Join(missing, ", "))
		}

		return plTask, nil
	}), nil
}

// Execute runs the attached Pipeline with data from other Pipes.
//
// It sets up a new pipeline context and prepares it with the data from the
// Task's raw input values. It creates the initial job contexts, and for any
// Input tasks, it marks the job successfully completed and assigns it its value:
// either data linked data from a Pipe, or its default value if nothing was
// piped in.
//
// After preparing the context, it executes the underlying Pipeline. When it
// completes, it finds the job associated with the output Task, checks its status,
// and sends the job's output to the writer.
//
// Note that while the underlying pipeline runs in a different PipelineContext,
// they share the same Golang context.Context, so the "calling" pipeline's context
// controls timeouts -- not the pipeline on which this Pipe is based.
func (pt *PipelineTask) Execute(ctx context.Context, w io.Writer, linkedValues linkMap) error {
	pipelineName := pt.pipeline.pipelineConfig.Name
	logrus.Debugf("Starting task based on pipeline '%s'", pipelineName)

	if pt.OutputTask == "" {
		return errors.New("pipeline-based task does not specify which " +
			"of the pipeline's tasks to use as output")
	}

	pctx := newPipeContext(pt.pipeline)
	defer pctx.returnBuffers()

	var outputJob *job
	for idx, parsedTask := range pt.pipeline.pts {
		if parsedTask.task.name == pt.OutputTask {
			outputJob = pctx.jobs[idx]
		}

		_, isInputTask := parsedTask.pipe.(*InputTask)
		if !isInputTask {
			continue
		}

		taskName := parsedTask.task.name
		v, ok := pt.Inputs[taskName]
		if !ok {
			v, ok = linkedValues[taskName]
		}
		if !ok {
			continue
		}

		pctx.baseEntry.Debugf("Assigning input '%s'.", taskName)
		if len(v) == 0 {
			pctx.baseEntry.Warnf("Input '%s' has empty value.", taskName)
		}

		pctx.jobs[idx].result = v
		pctx.jobs[idx].status = &Status{
			State:       Success,
			StartedAt:   time.Now().UTC(),
			CompletedAt: time.Now().UTC(),
		}
	}
	if outputJob == nil {
		return errors.Errorf("pipeline-based task is missing task '%s', "+
			"which is the desired output", pt.OutputTask)
	}
	outputJob.dependants += 2

	// try running the Pipeline
	if err := execute(ctx, pctx); err != nil {
		return errors.WithMessagef(err, "failed pipeline-based task execution "+
			"using pipeline '%s'", pipelineName)
	}

	// the Pipe's output comes from one of the Pipeline's tasks.
	status := outputJob.status
	taskResult := outputJob.result
	if status == nil || status.Err != nil {
		pctx.baseEntry.Warnf("pipeline-based task's output task did not complete successfully")
	}
	if len(taskResult) == 0 {
		pctx.baseEntry.Debugf("pipeline-based task '%s' output task '%s' had no content",
			pipelineName, pt.OutputTask)
		return nil
	}
	_, err := w.Write(taskResult)
	return err
}

// Execute a Pipeline by running each task in dependency order until either the
// Pipeline is complete, or a Task indicates that the Pipeline should not continue.
//
// Execution may also be canceled via the Context, but it is up to tasks to play
// nice and respect cancellation requests; at the very least, the pipeline will
// stop as soon as the current task completes.
func (pline *Pipeline) Execute(ctx context.Context) Status {
	pctx := newPipeContext(pline)
	defer pctx.returnBuffers()

	pctx.status.start()
	err := execute(ctx, pctx)
	pctx.status.complete(err)
	return *pctx.status
}

// execute takes a pipeline context so that we can preset values as needed.
func execute(ctx context.Context, pctx *PipelineContext) error {
	for jobIdx, job := range pctx.jobs {
		pipe := job.pipe
		pctx.baseEntry.Data["jobIdx"] = jobIdx
		pctx.baseEntry.Data["taskName"] = job.task.name

		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}

		if _, ok := pipe.(*InputTask); ok {
			// 'input' tasks should only run if their value isn't overridden
			if job.result != nil {
				pctx.baseEntry.Debug("Skipping input task because its value is set.")
				continue
			}
		}

		job.status = &Status{}
		if shouldRun, err := pctx.shouldExecute(job); !shouldRun {
			if err != nil {
				return err
			}
			pctx.baseEntry.Debugf("Skipping task.")
			continue
		}

		pctx.baseEntry.Debugf("Preparing input map.")
		var inputMap map[string][]byte
		if len(job.dependsOn) == 0 {
			pctx.baseEntry.Debugf("Task has no links.")
			inputMap = emptyDataMap
		} else {
			inputMap = make(map[string][]byte, len(job.dependsOn))
			if err := pctx.getLinkedInput(job, inputMap); err != nil {
				return errors.WithMessagef(err,
					"failed while linking input for task '%s'",
					job.task.name)
			}
		}

		var err error
		job.status.start()
		pctx.baseEntry.Debugf("Starting task.")
		shouldLogOutput := logrus.IsLevelEnabled(logrus.DebugLevel) && !job.task.DisableResultLog
		if shouldLogOutput || job.dependants > 0 {
			// only assign a buffer if we will use the result
			job.buffer = bufferPool.Get().(*bytes.Buffer)
			err = pipe.Execute(ctx, job.buffer, inputMap)
		} else {
			pctx.baseEntry.Debugf("Task has no dependents")
			err = pipe.Execute(ctx, ioutil.Discard, inputMap)
		}

		if err != nil {
			pctx.baseEntry.
				WithError(err).
				Debugf("Task failed.")

			if !isPermanentPipeError(err) {
				// pctx.baseEntry.Debugf("Retrying.")
				// note: because linking drops the dependants count, we can't relink (with this design)
				// job.status.start()
			}

			job.status.State = Failed
			job.status.CompletedAt = time.Now().UTC()
			job.status.Err = err

			if !job.task.ContinueOnError {
				return err
			}

			shouldLogOutput = false
		} else {
			job.status.State = Success
			job.status.CompletedAt = time.Now().UTC()
			job.result = job.buffer.Bytes()
			pctx.baseEntry.Debugf("Task succeeded.")
		}

		if shouldLogOutput {
			if len(job.result) == 0 {
				pctx.baseEntry.Debugf("Task returned no content.")
			} else if len(job.result) < truncateOutputAt {
				pctx.baseEntry.Debugf("Task result: %s", job.result)
			} else {
				pctx.baseEntry.Debugf("Task result (truncated from %d bytes): %s...",
					len(job.result), job.result[:truncateOutputAt])
			}
		}

		if len(job.result) == 0 {
			if job.task.ErrorIfEmpty {
				job.status.State = Failed
				return errors.Errorf("task '%s' returned no "+
					"content and is configured to consider this an error",
					job.task.name)
			}
			if job.task.StopIfEmpty {
				pctx.baseEntry.Debugf("Stopping pipeline due to empty task result.")
				break
			}
			if job.dependants == 0 {
				// return the buffer since we don't need it
				job.result = emptyResult
				bufferPool.Put(job.buffer)
				job.buffer = nil
			}
		}
	}
	return nil
}

// bufferPool maintains a pool of reusable byte buffers.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// returnBuffers to the buffer pool.
func (pctx *PipelineContext) returnBuffers() {
	for _, j := range pctx.jobs {
		if j == nil || j.buffer == nil {
			continue
		}
		j.buffer.Reset()
		bufferPool.Put(j.buffer)
	}
}

const truncateOutputAt = 100 // bytes
var emptyDataMap = map[string][]byte{}
var emptyResult = []byte(``)

// addLinkedInput passes the result of Linked tasks to this Task's Fill method.
func (pctx *PipelineContext) getLinkedInput(j *job, inputMap map[string][]byte) error {
	for _, d := range j.dependsOn {
		if d.dType == dState {
			continue
		}

		pctx.baseEntry.Debugf("Linking '%s' from %s to %s.",
			d.as, d.task.name, j.task.name)
		dJob, ok := pctx.jobsByTask[d.task]
		if !ok {
			// this shouldn't happen, but just to be safe...
			return errors.Errorf("missing job for link '%s'", d.task.name)
		}
		dJob.dependants--

		// TODO: the only thing that actually needs a map[string][]byte is the
		//   template task. Other than that, the only idea was allowing easier
		//   logic for unmarshaling, but the efficiency gain is considerable.
		//   This method needs to only be used for Template tasks; all other
		//   task types should end up just directly with the jobs, already linked
		//   during parsing. From there, it should be easy for them to set values.

		var val []byte
		var err error
		switch d.dType {
		case dLink:
			val = dJob.result
		case dStart:
			val, err = json.Marshal(dJob.status.StartedAt.UnixNano() / 1e6)
		case dComplete:
			val, err = json.Marshal(dJob.status.CompletedAt.UnixNano() / 1e6)
		case dErr:
			val, err = json.Marshal(dJob.status.Err.Error())
		}
		if err != nil {
			return err
		}
		inputMap[d.as] = val

	}

	return nil
}

// determine whether a task should execute based on its conditions.
func (pctx *PipelineContext) shouldExecute(job *job) (bool, error) {
	for _, d := range job.dependsOn {
		j, ok := pctx.jobsByTask[d.task]
		if !ok || j.status == nil {
			return false, errors.Errorf("task requires '%s', but "+
				"it has not yet completed", d.task.name)
		}
		if j.status.State != d.state {
			return false, errors.Errorf("task requires '%s' to have "+
				"state %s, but it is %s",
				d.task.name, d.state, j.status.State)
		}
	}
	return true, nil
}

func isPermanentPipeError(err error) bool {
	err = errors.Cause(err)
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
