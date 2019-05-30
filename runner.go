package goplumber

import (
	"context"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type runningPipeline struct {
	pipeline Pipeline
	cancel   context.CancelFunc
}

type PipelineRunner struct {
	pipeLock  sync.Mutex
	pipelines map[string]runningPipeline
}

func NewPipelineRunner() PipelineRunner {
	return PipelineRunner{pipelines: map[string]runningPipeline{}}
}

func (pr *PipelineRunner) withPipeLock(f func()) {
	pr.pipeLock.Lock()
	defer pr.pipeLock.Unlock()
	f()
}

func (pr *PipelineRunner) AddPipeline(ctx context.Context, p Pipeline) {
	pr.withPipeLock(func() {
		if ctx.Err() != nil {
			logrus.WithField("pipeline", p.Name).
				Debug("Skipping AddPipeline because context was canceled")
			return
		}

		if rp, ok := pr.pipelines[p.Name]; ok {
			rp.cancel()
			delete(pr.pipelines, p.Name)
		}

		rp := runningPipeline{pipeline: p}

		// Look at the pipeline's trigger(s):
		// If it's time-based, schedule it to run.
		if p.Trigger.Interval.Seconds != 0 {
			pCtx, pCancel := context.WithCancel(ctx)
			rp.cancel = pCancel
			go runPipelineForever(pCtx, p, logPipelineResult)
		}
		// todo If it's HTTP-based, set up a webhook.
		// todo If it's MQTT- or 0MQ-based, set up a Subscriber.

		pr.pipelines[p.Name] = rp
	})
}

func (pr *PipelineRunner) RunPipeline(ctx context.Context, name string) (err error) {
	pr.withPipeLock(func() {
		rp, ok := pr.pipelines[name]
		if !ok {
			err = errors.Errorf("no pipeline named %s", name)
			return
		}

		go func() {
			var result PipelineResult
			runNow(ctx, rp.pipeline, &result)
			logPipelineResult(result)
		}()
	})
	return
}

type PipelineResult struct {
	Pipeline *Pipeline
	Status   PipeStatus
}

type PipelineFinished func(result PipelineResult)

func logPipelineResult(r PipelineResult) {
	if r.Status.Err != nil {
		logrus.Errorf("Pipeline failed: %+v", r.Status.Err)
	} else {
		logrus.Debugf("Pipeline result: %+v", r.Status)
	}
}

// runPipelineForever repeatedly schedules the pipeline's execution until the
// context is canceled. Each time the pipeline finishes, the callback, if provided,
// is called synchronously.
func runPipelineForever(ctx context.Context, p Pipeline, callback PipelineFinished) {
	if ctx.Err() != nil {
		return
	}

	// run once right away
	var r PipelineResult
	runNow(ctx, p, &r)
	if callback != nil {
		callback(r)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r := schedule(ctx, p)
		if callback != nil {
			callback(r)
		}
	}
}

// schedule waits until the pipeline should start, runs it, and returns the result;
// if the context is canceled before the pipeline is started, it returns.
func schedule(ctx context.Context, pipeline Pipeline) PipelineResult {
	result := PipelineResult{Pipeline: &pipeline}
	waitDuration := time.Duration(pipeline.Trigger.Interval.Seconds) * time.Second

	select {
	case <-time.After(waitDuration):
		runNow(ctx, pipeline, &result)
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			result.Status.Err = err
		}
		result.Status.Err = errors.New("pipeline canceled before execution")
	}

	return result
}

func runNow(ctx context.Context, pipeline Pipeline, result *PipelineResult) {
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			asErr, isErr := panicErr.(error)
			if isErr {
				result.Status.Err = asErr
			} else {
				logrus.Error(panicErr)
				result.Status.Err = errors.Errorf("pipeline panicked; see logs")
			}
		}

		result.Status.CompletedAt = time.Now().UTC()
		if result.Status.Err != nil {
			result.Status.State = Failed
		} else {
			result.Status.State = Success
		}
	}()

	result.Status.StartedAt = time.Now().UTC()
	result.Status.Err = pipeline.Execute(ctx)
	return
}
