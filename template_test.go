package goplumber

import (
	"bytes"
	"context"
	"encoding/json"
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"testing"
)

type SingleTemplateTest struct {
	Tmpl, Expected string
	Data           map[string]json.RawMessage
	StageInput     map[string][]byte
}

var tmplLoader = NewFSLoader("testdata")
var tmplClient = NewTemplateClient(memoryStore).(*templateClient)

func (tt SingleTemplateTest) test(t *testing.T) {
	w := expect.WrapT(t).StopOnMismatch()

	name := "test-template"
	memoryStore.kvStore[name] = []byte(tt.Tmpl)

	raw := w.ShouldHaveResult(json.Marshal(struct {
		Template    string
		InitialData map[string]json.RawMessage
		Namespaces  []string
	}{Template: name, InitialData: tt.Data, Namespaces: []string{name}})).([]byte)

	task := &Task{Raw: raw}
	transform := w.ShouldHaveResult(tmplClient.GetPipe(task)).(*TemplatePipe)

	w.ShouldBeEqual(transform.template.Name(), name)

	buff := &bytes.Buffer{}
	w.ShouldSucceed(transform.Execute(context.Background(), buff, tt.StageInput))
	r := buff.String()
	w.Log(r)
	w.ShouldBeEqual(r, tt.Expected)
}

func TestTemplateTask_json(t *testing.T) {
	tt := SingleTemplateTest{
		Tmpl: `
{{- $s1 := .stage1 | json -}}
{{printf "hello, %s!" $s1.hello}}
{{.stage2 | printf "%s.00"}} + {{.addVal | int}} = 
{{- .stage2 | add $.addVal | printf " %d" -}}
`,
		Data:     map[string]json.RawMessage{"addVal": []byte(`36`)},
		Expected: "hello, world!\n1234.00 + 36 = 1270",
		StageInput: map[string][]byte{
			"stage1": []byte(`{"hello": "world"}`),
			"stage2": []byte(`1234`),
		},
	}
	tt.test(t)

	tt.Data = map[string]json.RawMessage{"addVal": []byte(`-10`)}
	tt.Expected = "hello, world!\n1234.00 + -10 = 1224"
	tt.test(t)

	tt.StageInput["stage1"] = []byte(`{"hello": "there"}`)
	tt.Expected = "hello, there!\n1234.00 + -10 = 1224"
	tt.test(t)
}

func TestTemplateTask_emptyTemplate(t *testing.T) {
	tt := SingleTemplateTest{
		StageInput: nil,
		Tmpl:       ``,
		Expected:   "",
	}
	tt.test(t)
}

func TestTemplateTask_emptyInitial(t *testing.T) {
	tt := SingleTemplateTest{
		StageInput: map[string][]byte{"stage1": []byte("lorem ipsum")},
		Tmpl:       `{{.stage1 | str}}`,
		Expected:   "lorem ipsum",
	}
	tt.test(t)
}

func TestTemplateTask_noStageInput(t *testing.T) {
	tt := SingleTemplateTest{
		StageInput: nil,
		Tmpl:       `{{printf "%10s" "text"}}`,
		Expected:   `      text`,
	}
	tt.test(t)
}
