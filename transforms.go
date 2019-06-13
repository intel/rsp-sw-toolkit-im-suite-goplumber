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
func ToInt(v interface{}) (int, error) {
	switch v := v.(type) {
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

// ToNumber converts to a number.
func ToNumber(x interface{}) (interface{}, error) {
	switch v := x.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return v, nil
	case string:
		return strToNumber(v)
	case []byte:
		return strToNumber(string(v))
	default:
		vt := reflect.ValueOf(v)
		for vt.IsValid() && (vt.Kind() == reflect.Ptr || vt.Kind() == reflect.Interface) {
			vt = vt.Elem()
		}
		if !vt.IsValid() {
			return 0, errors.New("underlying value is nil")
		}
		if vt.Type().ConvertibleTo(intType) {
			return vt.Convert(intType).Int(), nil
		}
		if vt.Type().ConvertibleTo(floatType) {
			return vt.Convert(floatType).Float(), nil
		}
		return 0, errors.Errorf("can't convert type %T to number", v)
	}
}

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

// Add returns the sum of its values.
func Add(sum float64, rest ...float64) (float64, error) {
	for _, val := range rest {
		sum += val
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
	Data         map[string]interface{} `json:"initialData,omitempty"`
	Namespaces   []string               `json:"namespaces"`
	TemplateName string                 `json:"template"`
	template     *template.Template
}

// LoadTemplateNamespace loads a template namespace from a given source.
//
// If the source is a FileSystem and id does not have an extension, .gotmpl is
// automatically appended to the id.
func LoadTemplateNamespace(source DataSource, id string) (string, error) {
	if _, ok := source.(FileSystem); ok && filepath.Ext(id) == "" {
		id += ".gotmpl"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, _, err := source.Get(ctx, id)
	if ctx.Err() != nil {
		return "", err
	}

	return string(body), err
}

func NewTemplateTask(conf []byte, root *template.Template, loader DataSource) (*TemplateTask, error) {
	ts := &TemplateTask{}
	if err := json.Unmarshal(conf, ts); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal template task")
	}
	if ts.TemplateName == "" {
		return nil, errors.New("template task is missing template name")
	}

	tmpl, err := root.Clone()
	if err != nil {
		return nil, err
	}
	ts.template = tmpl.Option("missingkey=error")

	// load other templates into this namespace
	for _, ns := range ts.Namespaces {
		logrus.Debugf("Loading namespace %s.", ns)
		data, err := LoadTemplateNamespace(loader, ns)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load template namespace '%s'", ns)
		}
		if _, err := ts.template.New(ns).Parse(data); err != nil {
			return nil, errors.Wrapf(err, "unable to parse namespace '%s'", ns)
		}
	}

	// make sure the named template exists in the namespace
	if ts.template.Lookup(ts.TemplateName) == nil {
		return nil, errors.Errorf("no template named '%s' in this namespace",
			ts.TemplateName)
	}
	return ts, nil
}

func (ts *TemplateTask) Fill(linkedInput map[string][]byte) error {
	if ts.template == nil {
		panic("initial template is nil")
	}

	if ts.Data == nil {
		ts.Data = map[string]interface{}{}
	}
	// Merge initial data with linked input.
	// This overwrites 'raw' keys, but maybe it should return an error instead.
	for k, v := range linkedInput {
		ts.Data[k] = v
	}
	return nil
}

func (ts *TemplateTask) Execute(ctx context.Context, w io.Writer) error {
	logrus.Debugf("Executing template '%s'.", ts.TemplateName)
	if err := ts.template.ExecuteTemplate(w, ts.TemplateName, ts.Data); err != nil {
		return errors.Wrap(err, "template task failed")
	}
	return nil
}
