/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package goplumber

import (
	"context"
	"encoding/json"
	"github.com/intel/rsp-sw-toolkit-im-suite-expect"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

var dataLoader = NewFileSystem("testdata")
var memoryStore = NewMemoryStore()

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func getTestData(w *expect.TWrapper, filename string) []byte {
	w.Helper()
	return w.ShouldHaveResult(dataLoader.GetFile(filename)).([]byte)
}

func getTestPlumber() Plumber {
	p := NewPlumber()
	p.SetClient("template", NewTemplateClient(dataLoader))
	p.SetSource("secret", dataLoader)
	p.SetSource("get", memoryStore)
	p.SetSource("put", memoryStore)

	return p
}

func getTestPipeline(w *expect.TWrapper, p Plumber, name string) *Pipeline {
	var pipelineConfig PipelineConfig
	w.ShouldSucceed(json.Unmarshal(getTestData(w, name), &pipelineConfig))
	return w.ShouldHaveResult(p.NewPipeline(&pipelineConfig)).(*Pipeline)
}

// withDataServer starts a server, executes a function, and then cleans up.
//
// The server only lives during the function's execution. The result of calling
// this function is a map of request paths to their corresponding request bodies,
// which may be used after execution to validate certain calls were made.
//
// The function is called with the URL of the test server, which then may be used
// for making requests. The server responds to requests by finding a match in the
// dataMap for the path of the request. If there is no match, it returns 404 and
// logs the missing endpoint.
func withDataServer(w *expect.TWrapper, dataMap map[string][]byte, f func(url string)) map[string][][]byte {
	w.Helper()
	callMap := map[string][][]byte{}
	mtx := sync.Mutex{}
	s := httptest.NewServer(
		http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			mtx.Lock()
			defer w.ShouldSucceedLater(r.Body.Close)
			body := w.ShouldHaveResult(ioutil.ReadAll(r.Body)).([]byte)
			callMap[r.URL.Path] = append(callMap[r.URL.Path], body)
			mtx.Unlock()

			data, ok := dataMap[r.URL.Path]
			if !ok {
				w.Errorf("missing endpoint for %s", r.URL.Path)
				rw.WriteHeader(404)
				return
			}

			w.ShouldHaveResult(rw.Write(data))
		}))
	f(s.URL)
	s.Close()
	return callMap
}

func TestPipeline_simpleETL(t *testing.T) {
	w := expect.WrapT(t).StopOnMismatch()
	plumber := getTestPlumber()
	p := getTestPipeline(w, plumber, "ETL.json")

	w.ShouldHaveLength(p.pts, 5)

	destRequests := withDataServer(w, map[string][]byte{
		"/load":  []byte(`583671654321`),
		"/store": []byte(``),
	}, func(srcURL string) {
		memoryStore.kvStore["loadURL"] = []byte(`"` + srcURL + `/load"`)
		memoryStore.kvStore["storeURL"] = []byte(`"` + srcURL + `/store"`)
		w.ShouldSucceed(p.Execute(context.Background()).Err)
	})

	w.ShouldContain(destRequests, []string{"/store"})
	w.ShouldHaveLength(destRequests["/store"], 1)
	w.ShouldBeEqual(destRequests["/store"][0], []byte(`1988-06-30T11:00:54.321Z`))
}

func TestPipeline_simpleETL_lowLogLevel(t *testing.T) {
	// When the logging level is high, the executor avoids creating buffers
	// for results that will just be thrown away, so verify everything works
	// even when the level is increased.

	lvl := logrus.GetLevel()
	defer logrus.SetLevel(lvl)
	logrus.SetLevel(logrus.ErrorLevel)

	w := expect.WrapT(t).StopOnMismatch()
	plumber := getTestPlumber()
	p := getTestPipeline(w, plumber, "ETL.json")

	w.ShouldHaveLength(p.pts, 5)

	destRequests := withDataServer(w, map[string][]byte{
		"/load":  []byte(`583671654321`),
		"/store": []byte(``),
	}, func(srcURL string) {
		memoryStore.kvStore["loadURL"] = []byte(`"` + srcURL + `/load"`)
		memoryStore.kvStore["storeURL"] = []byte(`"` + srcURL + `/store"`)
		w.ShouldSucceed(p.Execute(context.Background()).Err)
	})

	w.ShouldContain(destRequests, []string{"/store"})
	w.ShouldHaveLength(destRequests["/store"], 1)
	w.ShouldBeEqual(destRequests["/store"][0], []byte(`1988-06-30T11:00:54.321Z`))
}

func TestPipeline_customType(t *testing.T) {
	w := expect.WrapT(t).StopOnMismatch()
	plumber := getTestPlumber()

	etPipeline := getTestPipeline(w, plumber, "etTaskType.json")

	// add a new task type, 'ET', which runs the etPipeline and returns the
	// result of the 'transform' task
	plumber.Clients["ET"] = w.ShouldHaveResult(NewTaskType(etPipeline)).(Client)

	// now load the 'loadOnly' pipeline, which declares an ET task
	loadPipeline := getTestPipeline(w, plumber, "loadOnly.json")

	w.ShouldBeTrue(len(etPipeline.pts) > 1)
	w.ShouldBeTrue(len(loadPipeline.pts) > 1)

	destRequests := withDataServer(w, map[string][]byte{
		"/load":  []byte(`583671654321`),
		"/store": []byte(``),
	}, func(srcURL string) {
		memoryStore.kvStore["loadURL"] = []byte(`"` + srcURL + `/load"`)
		memoryStore.kvStore["storeURL"] = []byte(`"` + srcURL + `/store"`)
		w.ShouldSucceed(loadPipeline.Execute(context.Background()).Err)
	})

	w.ShouldContain(destRequests, []string{"/store"})
	w.ShouldHaveLength(destRequests["/store"], 1)
	w.ShouldBeEqual(destRequests["/store"][0], []byte(`1988-06-30T11:00:54.321Z`))
}
