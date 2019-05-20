package goplumber

import (
	"context"
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

var testDataLoader = NewFSLoader("testdata")
var testMemStore = NewMemoryStore()
var testDockerSecretsStore = NewDockerSecretsStore("testdata")

func getTestData(w *expect.TWrapper, filename string) []byte {
	w.Helper()
	return w.ShouldHaveResult(testDataLoader.GetFile(filename)).([]byte)
}

func getTestPipeline(w *expect.TWrapper, name string) Pipeline {
	conf := getTestData(w, name)
	pcon := PipelineConnector{
		TemplateLoader: testDataLoader,
		KVData:         testMemStore,
		Secrets:        testDockerSecretsStore,
	}
	return w.ShouldHaveResult(NewPipeline(conf, pcon)).(Pipeline)
}

func contentServer(w *expect.TWrapper, content []byte, url *string) (cleanup func()) {
	serv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.Logf("content server called; sending: %s", content)
		w.ShouldHaveResult(rw.Write(content))
	}))
	if url != nil {
		*url = serv.URL
	}
	return func() {
		serv.Close()
	}
}

func destServer(w *expect.TWrapper, expected []byte, url *string) (cleanup func()) {
	serv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.Logf("dest server called with method %s", r.Method)
		w.ShouldBeEqual(r.Method, "POST")
		incoming := w.ShouldHaveResult(ioutil.ReadAll(r.Body)).([]byte)
		w.ShouldBeEqual(expected, incoming)
	}))
	if url != nil {
		*url = serv.URL
	}
	return func() {
		serv.Close()
	}
}

func TestPipeline_simpleETL(t *testing.T) {
	w := expect.WrapT(t)
	p := getTestPipeline(w, "ETL.json")
	w.ShouldHaveLength(p.TaskMap, 3)
	w.ShouldContain(p.taskOrder, p.TaskMap)

	content := []byte(`583671654321`)
	defer contentServer(w, content, &(p.TaskMap["extract"].pipe.(*HTTPTask).URL))()
	expected := []byte("1988-06-30T11:00:54.321Z")
	defer destServer(w, expected, &(p.TaskMap["load"].pipe.(*HTTPTask).URL))()

	w.ShouldSucceed(p.Execute(context.Background()))
}

func TestPipeline_scheduleETL(t *testing.T) {
	// setup the ETL pipeline
	w := expect.WrapT(t)
	p := getTestPipeline(w, "ETL.json")
	w.ShouldHaveLength(p.TaskMap, 3)
	w.ShouldContain(p.taskOrder, p.TaskMap)

	content := []byte(`583671654321`)
	defer contentServer(w, content, &(p.TaskMap["extract"].pipe.(*HTTPTask).URL))()

	// record how many times the destination server is called
	mtx := sync.Mutex{}
	timesCalled := []time.Time{}
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

	// overwrite the URL
	p.TaskMap["load"].pipe.(*HTTPTask).URL = destServer.URL

	// set the pipeline to trigger every two seconds
	p.Trigger.Interval = Interval{Seconds: 1}

	// start the runner & add the pipeline
	ctx, cancel := context.WithCancel(context.Background())
	runner := PipelineRunner{pipelines: map[string]runningPipeline{}}
	started := time.Now()
	runner.AddPipeline(ctx, p)

	// wait two seconds, then cancel the context
	<-time.After(2 * time.Second)
	cancel()

	// record the number of times called; make sure it was once or twice
	saveTimeCalled := timesCalled
	copy(saveTimeCalled, timesCalled)
	w.ShouldContain([]int{1, 2}, len(timesCalled))

	// the first call should have been right away
	delta := timesCalled[0].Sub(started)
	w.As(delta).ShouldBeTrue(delta.Seconds() < 0.5)

	// wait another couple of seconds to confirm it's not still running
	<-time.After(2 * time.Second)
	w.ShouldBeEqual(saveTimeCalled, timesCalled)
}
