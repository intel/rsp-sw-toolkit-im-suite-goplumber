/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package goplumber

import (
	"context"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"time"
)

func RunNow(ctx context.Context, p *Pipeline) {
	runNow(ctx, p)
}

// RunPipelineForever repeatedly schedules the Pipeline's execution until the
// context is canceled.
//
// The Pipeline will be executed once immediately. The given duration is waited
// after an execution completed; it is _NOT_ the amount of time between starts,
// but instead the time from one end to the next start.
//
// If the duration is zero or negative, the Pipeline will execute as often as
// possible.
func RunPipelineForever(ctx context.Context, p *Pipeline, d time.Duration) {
	if ctx.Err() != nil {
		return
	}

	var tick <-chan time.Time = nil
	if d > 0 {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		r := runNow(ctx, p)
		r.logResult(p.pipelineConfig.Name)
		select {
		case <-tick:
		case <-ctx.Done():
			return
		}
	}
}

// setResult recovers from pipeline panics, yielding a consistent result.
func setResult(result *Status) {
	panicErr := recover()

	if panicErr != nil {
		err, isErr := panicErr.(error)
		if isErr {
			err = errors.Wrap(err, "pipeline panicked")
		} else {
			err = errors.New("pipeline panicked")
		}
		result.Err = err
	}

	result.CompletedAt = time.Now().UTC()
	if result.Err != nil {
		result.State = Failed
	} else {
		result.State = Success
	}
}

// runNow executes a pipeline and records the result, handling panics if necessary.
func runNow(ctx context.Context, pipeline *Pipeline) (result Status) {
	defer setResult(&result)

	// nil Timeout or Timeout of zero is set to default
	// negative Timeout implies Pipeline should not auto-cancel
	timeout := time.Duration(defaultTimeoutSecs) * time.Second
	pTimeout := pipeline.pipelineConfig.TimeoutSecs
	if pTimeout != nil && *pTimeout > 0 {
		timeout = time.Duration(*pTimeout) * time.Second
	}

	var cancelFunc context.CancelFunc
	if timeout > 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, timeout)
		logrus.Debugf("Starting pipeline %s with timeout %d seconds.",
			pipeline.pipelineConfig.Name, int(timeout.Seconds()))
	} else {
		ctx, cancelFunc = context.WithCancel(ctx)
		logrus.Debugf("Starting pipeline %s without a timeout; it must "+
			"either finish on its own or be canceled manually.",
			pipeline.pipelineConfig.Name)
	}
	defer cancelFunc()

	result = pipeline.Execute(ctx)
	return
}
