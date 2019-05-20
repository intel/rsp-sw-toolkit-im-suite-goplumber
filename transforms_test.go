package goplumber

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"text/template"

	"github.impcloud.net/RSP-Inventory-Suite/expect"
)

type SingleTemplateTest struct {
	Tmpl, Expected string
	Data           map[string]json.RawMessage
	StageInput     map[string][]byte
}

var tmplLoader = NewFSLoader("testdata")

func (tt SingleTemplateTest) test(t *testing.T) {
	w := expect.WrapT(t).StopOnMismatch()

	tmpl := w.ShouldHaveResult(baseTmpl.Clone()).(*template.Template)
	name := "test-template"
	w.ShouldHaveResult(tmpl.New(name).Parse(tt.Tmpl))

	raw := w.ShouldHaveResult(json.Marshal(struct {
		Template    string
		InitialData map[string]json.RawMessage
	}{Template: name, InitialData: tt.Data})).([]byte)

	transform := w.ShouldHaveResult(NewTemplateTask(raw, tmpl, tmplLoader)).(*TemplateTask)
	w.ShouldSucceed(transform.Fill(tt.StageInput))
	transform.TemplateName = name

	w.Logf("%s", w.ShouldHaveResult(json.Marshal(transform)).([]byte))
	buff := &bytes.Buffer{}
	w.ShouldSucceed(transform.Execute(context.Background(), buff))
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
{{- .stage2 | json | add $.addVal | printf " %.02f" -}}
`,
		Data:     map[string]json.RawMessage{"addVal": []byte(`36`)},
		Expected: "hello, world!\n1234.00 + 36 = 1270.00",
		StageInput: map[string][]byte{
			"stage1": []byte(`{"hello": "world"}`),
			"stage2": []byte(`1234`),
		},
	}
	tt.test(t)

	tt.Data = map[string]json.RawMessage{"addVal": []byte(`-10`)}
	tt.Expected = "hello, world!\n1234.00 + -10 = 1224.00"
	tt.test(t)

	tt.StageInput["stage1"] = []byte(`{"hello": "there"}`)
	tt.Expected = "hello, there!\n1234.00 + -10 = 1224.00"
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
