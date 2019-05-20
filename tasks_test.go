package goplumber

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestUnmarshalPartial(t *testing.T) {
	w := expect.WrapT(t).StopOnMismatch()

	type Struct struct {
		S string
	}
	type Struct2 struct {
		S string
	}

	starter := struct {
		A          string `json:"A"`
		a, AA, Aaa string
		B          int `json:"B"`
		b, BB      int
		C          float64 `json:"C"`
		c, CC      float64
		D          Struct `json:"D"`
		d, DD      Struct
		E          Struct `json:"tag"`
		EE         Struct `json:"tag2"`
		F          Struct `json:"-"`
		Struct
		Struct2 `json:"tag3"`
	}{
		A: "A",
		B: 1,
		C: 3.14,
		D: Struct{S: "D"},
		E: Struct{S: "E"},
	}

	partial := map[string][]byte{
		"AA":     []byte(`"AA"`),
		"aAa":    []byte(`"AAA"`),
		"BB":     []byte("22"),
		"CC":     []byte("0.00159"),
		"DD":     []byte(`{"s":"DD"}`),
		"tag2":   []byte(`{"s":"EE"}`),
		"a":      []byte(`"a"`),
		"b":      []byte(`33`),
		"c":      []byte(`0.4321`),
		"d":      []byte(`{"s":"d"}`),
		"e":      []byte(`{"s":"e"}`),
		"F":      []byte(`"F"`),
		"struct": []byte(`{"s":"struct"}`),
		"tag3":   []byte(`{"s":"t3"}`),
	}
	w.ShouldSucceed(unmarshalPartial(partial, &starter))

	// unmentioned values should be unaffected
	w.ShouldBeEqual(starter.A, "A")
	w.ShouldBeEqual(starter.B, 1)
	w.ShouldBeEqual(starter.C, 3.14)
	w.ShouldBeEqual(starter.D, Struct{S: "D"})
	w.ShouldBeEqual(starter.E, Struct{S: "E"})

	// partial values should be loaded
	w.ShouldBeEqual(starter.AA, "AA")
	w.ShouldBeEqual(starter.Aaa, "AAA")
	w.ShouldBeEqual(starter.BB, 22)
	w.ShouldBeEqual(starter.CC, 0.00159)
	w.ShouldBeEqual(starter.DD, Struct{S: "DD"})
	w.ShouldBeEqual(starter.EE, Struct{S: "EE"})
	w.ShouldBeEqual(starter.Struct, Struct{S: "struct"})
	w.ShouldBeEqual(starter.Struct2, Struct2{S: "t3"})

	// unexported fields should be left alone
	w.ShouldBeEmptyStr(starter.a)
	w.ShouldBeEqual(starter.b, 0)
	w.ShouldBeEqual(starter.c, 0.0)
	w.ShouldBeEqual(starter.d, Struct{})
	w.ShouldBeEqual(starter.F, Struct{})
}

// Test downloading from one HTTP server and sending it to another.
func TestDownloadAndSend(t *testing.T) {
	w := expect.WrapT(t).StopOnMismatch()

	content := []byte(`hello, world`)
	contentServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.Logf("content server called; sending: %s", content)
		w.ShouldHaveResult(rw.Write(content))
	}))
	defer contentServer.Close()

	destMethod := "POST"
	destServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.ShouldBeEqual(r.Method, destMethod)
		incoming := w.ShouldHaveResult(ioutil.ReadAll(r.Body)).([]byte)
		w.ShouldBeEqual(incoming, content)
	}))
	defer destServer.Close()

	pipeline := &Pipeline{
		TaskMap: map[string]*Task{
			"getContent": {
				TaskType: "http",
				Raw: []byte(fmt.Sprintf(`{"url":"%s", "method": "GET"}`,
					contentServer.URL)),
			},
			"sendContent": {
				TaskType: "http",
				Links:    map[string]Link{"body": {Source: "getContent"}},
				Raw: []byte(fmt.Sprintf(`{"url":"%s", "method":"%s"}`,
					destServer.URL, destMethod)),
			},
		},
	}

	w.ShouldSucceed(pipeline.initTasks(PipelineConnector{
		TemplateLoader: NewFSLoader("testdata"),
		KVData:         NewMemoryStore(),
	}))
	pipeline.taskOrder = []string{"getContent", "sendContent"}

	w.ShouldSucceed(pipeline.Execute(context.Background()))
}
