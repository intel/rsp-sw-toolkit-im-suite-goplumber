/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package goplumber

import (
	"context"
	"errors"
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestPipelineRunner_interval(t *testing.T) {
	// setup the ETL pipeline
	w := expect.WrapT(t)
	p := getTestPipeline(w, getTestPlumber(), "ETL.json")
	w.ShouldHaveLength(p.pts, 5)

	// record how many times the destination server is called
	mtx := sync.Mutex{}
	var timesCalled []time.Time
	destServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mtx.Lock()
		timesCalled = append(timesCalled, time.Now())
		mtx.Unlock()
		w.ShouldBeEqual(r.Method, "POST")
		incoming := w.ShouldHaveResult(ioutil.ReadAll(r.Body)).([]byte)
		w.Logf("dest server called with content: %s", incoming)
		w.ShouldBeEqual([]byte("1988-06-30T11:00:54.321Z"), incoming)
	}))
	defer destServer.Close()
	memoryStore.kvStore["storeURL"] = []byte(`"` + destServer.URL + `/store"`)

	withDataServer(w, map[string][]byte{"/load": []byte(`583671654321`)}, func(url string) {
		memoryStore.kvStore["loadURL"] = []byte(`"` + url + `/load"`)

		// set the pipeline to trigger every two seconds
		// start the runner & add the pipeline
		ctx, cancel := context.WithCancel(context.Background())
		started := time.Now()
		go RunPipelineForever(ctx, p, time.Duration(1)*time.Second)

		// wait two seconds, then cancel the context
		<-time.After(2 * time.Second)
		cancel()

		// record the number of times called; make sure it was once or twice
		saveTimeCalled := timesCalled
		copy(saveTimeCalled, timesCalled)
		w.StopOnMismatch().ShouldBeEqual(len(timesCalled), 2)

		// the first call should have been right away
		delta := timesCalled[0].Sub(started)
		w.As(delta).ShouldBeTrue(delta.Seconds() < 0.05)

		// wait another couple of seconds to confirm it's not still running
		<-time.After(2 * time.Second)
		w.ShouldBeEqual(saveTimeCalled, timesCalled)
	})
}

func TestPipelineRunner_runMultiple(t *testing.T) {
	w := expect.WrapT(t)

	plumber := getTestPlumber()
	p := getTestPipeline(w, plumber, "ETL.json")

	destReqs := withDataServer(w, map[string][]byte{
		"/load":  []byte(`583671654321`),
		"/store": []byte(``),
	}, func(url string) {
		memoryStore.kvStore["loadURL"] = []byte(`"` + url + `/load"`)
		memoryStore.kvStore["storeURL"] = []byte(`"` + url + `/store"`)

		ctx, cancel := context.WithCancel(context.Background())

		wg := sync.WaitGroup{}
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func() {
				result := runNow(ctx, p)
				w.ShouldSucceed(result.Err)
				wg.Done()
			}()
		}
		wg.Wait()

		cancel()
	})

	w.ShouldHaveLength(destReqs, 2)
	w.ShouldHaveLength(destReqs["/load"], 100)
	w.ShouldHaveLength(destReqs["/store"], 100)
}

func TestSetResult(t *testing.T) {
	w := expect.WrapT(t).StopOnMismatch()

	panics := func() (r Status) {
		defer setResult(&r)
		panic(errors.New("function panic"))
		return
	}

	r := panics()
	w.ShouldNotBeNil(r)
	w.ShouldBeEqual(r.State, Failed)
	w.As("completion time recorded").ShouldBeFalse(r.CompletedAt.After(time.Now().UTC()))
	w.ShouldNotBeNil(r.Err)
	w.ShouldContainStr(r.Err.Error(), "function panic")
	w.ShouldContainStr(r.Err.Error(), "pipeline panicked")
	w.Log(r)

	noPanics := func() (r Status) {
		defer setResult(&r)
		return
	}

	r = noPanics()
	w.ShouldNotBeNil(r)
	w.ShouldBeEqual(r.State, Success)
	w.As("completion time recorded").ShouldBeFalse(r.CompletedAt.After(time.Now().UTC()))
	w.ShouldBeNil(r.Err)
	w.Log(r)
}
