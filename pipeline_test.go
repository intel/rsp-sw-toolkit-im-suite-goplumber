package goplumber

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

var dataLoader = NewFSLoader("testdata")
var memoryStore = NewMemoryStore()

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func getTestData(w *expect.TWrapper, filename string) []byte {
	w.Helper()
	return w.ShouldHaveResult(dataLoader.GetFile(filename)).([]byte)
}

func getTestPlumber() Plumber {
	p := NewPlumber(dataLoader)
	p.TaskGenerators["secret"] = NewLoadTaskGenerator(dataLoader)
	p.TaskGenerators["get"] = NewLoadTaskGenerator(memoryStore)
	p.TaskGenerators["put"] = NewStoreTaskGenerator(memoryStore)

	return p
}

func getTestPipeline(w *expect.TWrapper, p Plumber, name string) Pipeline {
	return w.ShouldHaveResult(p.NewPipeline(getTestData(w, name))).(Pipeline)
}

// contentServer starts serving "content" at a URL. It returns a cleanup function
// that should be executed after the test completes, allowing syntax like:
//     defer contentServer(w, content, url)
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
func withDataServer(w *expect.TWrapper, dataMap map[string][]byte, f func(url string)) map[string][]byte {
	w.Helper()
	callMap := map[string][]byte{}
	s := httptest.NewServer(
		http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			defer w.ShouldSucceedLater(r.Body.Close)
			callMap[r.URL.Path] = w.ShouldHaveResult(ioutil.ReadAll(r.Body)).([]byte)

			data, ok := dataMap[r.URL.Path]
			if !ok {
				w.Errorf("missing endpoint for %s", r.URL.Path)
				rw.WriteHeader(404)
				return
			}

			w.ShouldHaveResult(rw.Write(data))
			rw.WriteHeader(200)
		}))
	f(s.URL)
	s.Close()
	return callMap
}

func TestPipeline_simpleETL(t *testing.T) {
	w := expect.WrapT(t)
	plumber := getTestPlumber()
	p := getTestPipeline(w, plumber, "ETL.json")
	w.ShouldHaveLength(p.Tasks, 3)
	w.ShouldContain(p.taskOrder, p.Tasks)

	var destRequests map[string][]byte
	withDataServer(w, map[string][]byte{"/": []byte(`583671654321`)}, func(srcURL string) {
		destRequests = withDataServer(w, map[string][]byte{"/": []byte(``)}, func(dstURL string) {
			p.Tasks["extract"].pipe.(*HTTPTask).URL = srcURL
			p.Tasks["load"].pipe.(*HTTPTask).URL = dstURL
			w.ShouldSucceed(p.Execute(context.Background()))
		})
	})

	w.ShouldContain(destRequests, []string{"/"})
	w.ShouldBeEqual(destRequests["/"], []byte(`1988-06-30T11:00:54.321Z`))
}

func TestPipeline_scheduleETL(t *testing.T) {
	// setup the ETL pipeline
	w := expect.WrapT(t)
	p := getTestPipeline(w, getTestPlumber(), "ETL.json")
	w.ShouldHaveLength(p.Tasks, 3)
	w.ShouldContain(p.taskOrder, p.Tasks)

	content := []byte(`583671654321`)
	defer contentServer(w, content, &(p.Tasks["extract"].pipe.(*HTTPTask).URL))()

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

	// overwrite the URL
	p.Tasks["load"].pipe.(*HTTPTask).URL = destServer.URL

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
