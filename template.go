package goplumber

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"time"
)

var (
	baseTmpl = template.New("base").
		Funcs(template.FuncMap{
			"timestamp":         Timestamp,
			"outboundIP":        GetOutboundIP,
			"formatTime":        FormatTime,
			"formatCurrentTime": FormatCurrentTime,
			"int":               ToInt,        // number, string, byte, or byte array to int
			"add":               Add,          // sum of all args
			"str":               ToString,     // byte array to Go string
			"bytes":             json.Marshal, // object to JSON-formatted bytes
			"json":              ToJSON,       // bytes to a JSON object
			"dec64":             base64.StdEncoding.DecodeString,
			"enc64":             base64.StdEncoding.EncodeToString,
			"join":              strings.Join,
			"split":             strings.Split,
			"splitN":            strings.SplitN,
			"strIdx":            strings.Index,
			"trimSpace":         strings.TrimSpace,
			"err":               TemplateError, // force a template error
		})
)

func TemplateError(format string, args ...interface{}) (string, error) {
	return "", errors.Errorf(format, args...)
}

// ToInt attempts to convert the given interface to an integral type.
func ToInt(i interface{}) (int, error) {
	switch v := i.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	case string:
		return strToInt(v)
	case []byte:
		return strToInt(string(v))
	default:
		vt := reflect.ValueOf(v)
		for vt.IsValid() && (vt.Kind() == reflect.Ptr || vt.Kind() == reflect.Interface) {
			vt = vt.Elem()
		}
		if !vt.IsValid() {
			return 0, errors.New("underlying value is nil")
		}
		if vt.Type().ConvertibleTo(intType) {
			return int(vt.Convert(intType).Int()), nil
		}
		if vt.Type().ConvertibleTo(floatType) {
			return int(vt.Convert(floatType).Float()), nil
		}
		return 0, errors.Errorf("can't convert type %T to number", v)
	}
}

var intType = reflect.TypeOf(int(0))
var floatType = reflect.TypeOf(float64(0))

func strToInt(s string) (int, error) {
	r, err := strconv.Atoi(s)
	if err == nil {
		return r, nil
	}
	i, err := strconv.ParseInt(s, 0, 0)
	if err == nil {
		return int(i), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	return int(f), err
}

func strToNumber(s string) (interface{}, error) {
	var r interface{}
	r, err := strconv.Atoi(s)
	if err != nil {
		r, err = strconv.ParseInt(s, 0, 0)
	}
	if err != nil {
		r, err = strconv.ParseFloat(s, 64)
	}
	return r, err
}

// Timestamp returns the number of milliseconds since Jan 1, 1970 UTC.
func Timestamp() int64 {
	return time.Now().UTC().UnixNano() / 1e6
}

// FormatTime formats the given time according to the given format.
//
// The time must either be a go time.Time, or an int, which is interpreted as
// the number of milliseconds since Jan 1, 1970, in UTC. Other types will return
// an error.
func FormatTime(fmt string, t interface{}) (string, error) {
	switch v := t.(type) {
	case time.Time:
		return v.Format(fmt), nil
	case int:
		return time.Unix(0, int64(v)*1e6).UTC().Format(fmt), nil
	default:
		return "", errors.Errorf("can't interpret type %T as time", v)
	}
}

// FormatCurrentTime returns the current UTC time formatted by the given string.
func FormatCurrentTime(fmt string) string {
	s, _ := FormatTime(fmt, time.Now().UTC())
	return s
}

// ToString converts the given item to its string representation.
func ToString(x interface{}) string {
	return fmt.Sprintf("%s", x)
}

// ToJSON unmarshals the byte array into an interface, which it returns.
func ToJSON(src []byte) (v interface{}, err error) {
	err = json.Unmarshal(src, &v)
	return
}

// Add returns the sum of integer values.
func Add(values ...interface{}) (int, error) {
	sum := int(0)
	for _, val := range values {
		v, err := ToInt(val)
		if err != nil {
			return 0, err
		}
		sum += v
	}
	return sum, nil
}

func GetOutboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "1.1.1.1:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP, nil
}

type TemplateTask struct {
	Data         map[string]json.RawMessage `json:"initialData,omitempty"`
	Namespaces   []string                   `json:"namespaces"`
	TemplateName string                     `json:"template"`
}

type TemplatePipe struct {
	template *template.Template
	data     map[string][]byte
}

// LoadNamespace loads a template namespace from a given source.
//
// If the source is a FileSystem and id does not have an extension, .gotmpl is
// automatically appended to the id.
func LoadNamespace(source DataSource, namespaces []string) (*template.Template, error) {
	tmpl, err := baseTmpl.Clone()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to clone root template namespace")
	}
	tmpl = tmpl.Option("missingkey=error")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, ns := range namespaces {
		logrus.Debugf("Loading namespace %s.", ns)
		if _, ok := source.(FileSystem); ok && filepath.Ext(ns) == "" {
			ns += ".gotmpl"
		}

		body, _, err := source.Get(ctx, ns)
		if ctx.Err() != nil {
			return nil, err
		}

		if err != nil {
			return nil, errors.WithMessagef(err, "failed loading namespace '%s'", ns)
		}
		if _, err := tmpl.New(ns).Parse(string(body)); err != nil {
			return nil, errors.Wrapf(err, "failed parsing namespace '%s'", ns)
		}
	}

	return tmpl, err
}

// TemplateClient provides TemplatePipes by loading templates from a DataSource.
type templateClient struct {
	src DataSource
}

// NewTemplateClient returns a new Client that returns TemplatePipes.
func NewTemplateClient(src DataSource) Client {
	return &templateClient{src: src}
}

func (tc *templateClient) GetPipe(task *Task) (Pipe, error) {
	ts := TemplateTask{}
	if err := json.Unmarshal(task.Raw, &ts); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal template task")
	}
	if ts.TemplateName == "" {
		return nil, errors.New("template task is missing template name")
	}

	tmpl, err := LoadNamespace(tc.src, ts.Namespaces)
	if err != nil {
		return nil, err
	}
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		var allTmpls []string
		for _, tmpl := range tmpl.Templates() {
			allTmpls = append(allTmpls, tmpl.Name())
		}
		logrus.WithField("defined templates", allTmpls).
			Debug("Parsing complete.")
	}

	tp := &TemplatePipe{
		template: tmpl.Lookup(ts.TemplateName),
	}
	if tp.template == nil {
		return nil, errors.Errorf("no template named '%s' in this namespace",
			ts.TemplateName)
	}

	tp.data = map[string][]byte{}
	for k, v := range ts.Data {
		tp.data[k] = v
	}
	for l := range task.Links {
		if _, present := tp.data[l]; present {
			return nil, errors.Errorf("task both links and supplies"+
				" initial data for '%s'", l)
		}
	}
	return tp, nil
}

func (ts *TemplatePipe) Execute(ctx context.Context, w io.Writer, links linkMap) error {
	if ts.template == nil {
		panic("initial template is nil")
	}

	for k, v := range ts.data {
		links[k] = v
	}

	if err := ts.template.Execute(w, links); err != nil {
		return errors.Wrap(err, "template task failed")
	}
	return nil
}
