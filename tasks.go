package goplumber

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
)

type HTTPTask struct {
	client         *http.Client
	bodyReader     io.Reader
	MaxRetries     int                 `json:"maxRetries"`
	Method         string              `json:"method,omitempty"`
	URL            string              `json:"url,omitempty"`
	Body           json.RawMessage     `json:"body,omitempty"`
	Headers        map[string][]string `json:"headers,omitempty"`
	SkipCertVerify bool                `json:"skipCertVerify"`
}

func (task *HTTPTask) Fill(linkedInput map[string][]byte) error {
	logrus.Debug("Filling HTTP task")
	if err := unmarshalPartial(linkedInput, task); err != nil {
		return err
	}

	if task.Method == "" {
		return errors.New("missing method for HTTP task")
	}
	if task.URL == "" {
		return errors.New("missing URL for HTTP task")
	}
	if task.Body != nil {
		task.bodyReader = bytes.NewReader(task.Body)
	}
	if task.MaxRetries < 1 {
		logrus.Debugf("Forcing HTTPTask to have MaxRetries of at least 1, "+
			"instead of value %d", task.MaxRetries)
		task.MaxRetries = 1
	}

	if task.SkipCertVerify {
		logrus.Debug("Using insecure HTTP client to skip SSL certificate verification")
		task.client = getInsecureClient()
	} else {
		task.client = http.DefaultClient
	}

	return nil
}

var createInsecureTransportOnce = sync.Once{}
var insecureClient *http.Client

func getInsecureClient() *http.Client {
	createInsecureTransportOnce.Do(createInsecureClient)
	return insecureClient
}

func createInsecureClient() {
	insecureClient = &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          10,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}}
}

func (task *HTTPTask) Execute(ctx context.Context, w io.Writer) error {
	request, err := http.NewRequest(task.Method, task.URL, task.bodyReader)
	if err != nil {
		return errors.Wrap(err, "unable to create http request")
	}

	// add headers
	if task.Headers != nil {
		for headerKey, headers := range task.Headers {
			for _, h := range headers {
				request.Header.Add(headerKey, h)
			}
		}
	}

	request = request.WithContext(ctx)

	logrus.
		WithField("method", task.Method).
		WithField("url", task.URL).
		WithField("body length", len(task.Body)).
		WithField("num headers", len(task.Headers)).
		WithField("client", task.client).
		Debug("Executing HTTP task")

	response, err := task.client.Do(request)
	if err != nil {
		return errors.Wrap(err, "http task failed")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, response.Body)
		_ = response.Body.Close()
	}()

	// todo: retries/backoff

	if response.StatusCode < 200 || response.StatusCode > 299 {
		data, _ := ioutil.ReadAll(response.Body)
		return errors.Errorf("non-2xx status from %s: %d; body: %s",
			request.URL, response.StatusCode, data)
	}
	if _, err = io.Copy(w, response.Body); err != nil {
		return errors.Wrap(err, "failed to copy response body")
	}
	return nil
}

type JSONTask struct {
	content map[string]json.RawMessage
}

func (jt *JSONTask) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &jt.content)
}

func (jt *JSONTask) Fill(d map[string][]byte) error {
	// merge with existing data
	for k, v := range d {
		jt.content[k] = v
	}
	return nil
}

func (jt *JSONTask) Execute(ctx context.Context, w io.Writer) error {
	e := json.NewEncoder(w)
	e.SetEscapeHTML(false)
	return e.Encode(jt.content)
}

type JSONValidationTask struct {
	Content     []byte `json:"content"`
	SchemaBytes []byte `json:"schema"`
	schema      *gojsonschema.Schema
}

func (jvt *JSONValidationTask) Fill(d map[string][]byte) error {
	if err := unmarshalPartial(d, jvt); err != nil {
		return err
	}
	if jvt.Content == nil {
		return errors.New("JSON validation task has no content to validate")
	}
	if len(jvt.SchemaBytes) == 0 {
		return errors.New("JSON validation task has no schema to validate against")
	}

	l := gojsonschema.NewBytesLoader(jvt.SchemaBytes)
	s, err := gojsonschema.NewSchema(l)
	if err != nil {
		return errors.Wrapf(err, "unable to load schema")
	}
	jvt.schema = s
	return nil
}

func (jvt *JSONValidationTask) Execute(ctx context.Context, w io.Writer) error {
	dataLoader := gojsonschema.NewBytesLoader(jvt.Content)

	result, err := jvt.schema.Validate(dataLoader)
	if err != nil {
		return errors.Wrapf(err, "unable to validate against JSON schema")
	}

	logrus.Debugf("Validation result: %+v", result)
	if result.Valid() {
		return nil
	}

	errStrs := make([]string, len(result.Errors()))
	for i, e := range result.Errors() {
		errStrs[i] = fmt.Sprintf("suberror %d: %+v", i+1, e)
	}
	return errors.Errorf("JSON validation failed:\n%s",
		strings.Join(errStrs, "\n"))
}

type LoadStoreTask struct {
	store   PipelineStore
	Source  string
	Name    string
	Value   []byte
	Default json.RawMessage
	isLoad  bool
}

func (lst *LoadStoreTask) Fill(d map[string][]byte) error {
	if err := unmarshalPartial(d, lst); err != nil {
		return err
	}
	if lst.Name == "" {
		return errors.New("missing key name")
	}
	return nil
}

func (lst *LoadStoreTask) get(ctx context.Context, w io.Writer) error {
	if lst.Name == "" {
		return errors.New("missing key name")
	}
	v, ok, err := lst.store.Get(ctx, lst.Name)
	if err != nil {
		return err
	}
	if !ok {
		logrus.Debugf("missing %s; using default %s", lst.Name, lst.Default)
		v = lst.Default
	}
	_, err = w.Write(v)
	return err
}

func (lst *LoadStoreTask) Execute(ctx context.Context, w io.Writer) error {
	if lst.isLoad {
		return lst.get(ctx, w)
	}
	return lst.store.Put(ctx, lst.Name, lst.Value)
}

type MQTTTask struct {
	client         mqtt.Client
	ClientID       string
	Topics         []string
	Endpoint       string
	Message        []byte
	Username       string
	Password       string
	TimeoutSecs    int
	Encrypt        bool
	SkipCertVerify bool
}

func NewMQTTTask() *MQTTTask {
	return &MQTTTask{
		Encrypt:     true,
		TimeoutSecs: 30,
	}
}

func (mqttd *MQTTTask) Fill(linkedInput map[string][]byte) error {
	if err := unmarshalPartial(linkedInput, mqttd); err != nil {
		return err
	}

	if mqttd.Endpoint == "" {
		return errors.New("missing endpoint for MQTT task")
	}
	endpoint := mqttd.Endpoint
	if !strings.Contains(mqttd.Endpoint, "://") {
		if mqttd.Encrypt {
			endpoint = "tls://" + endpoint
		} else {
			endpoint = "tcp://" + endpoint
		}
	}
	purl, err := url.Parse(endpoint)
	if err != nil {
		return errors.Wrap(err, "invalid URL")
	}
	endpoint = purl.String()

	if mqttd.TimeoutSecs < 1 {
		logrus.Debugf("Setting time out to 30s")
		mqttd.TimeoutSecs = 30
	}

	// todo: use resource connection pool
	options := mqtt.NewClientOptions().
		AddBroker(endpoint).
		SetOrderMatters(false).
		SetMaxReconnectInterval(time.Duration(mqttd.TimeoutSecs) * time.Second).
		SetOnConnectHandler(func(client mqtt.Client) {
			logrus.Infof("MQTT connected on %s", endpoint)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, e error) {
			logrus.Errorf("MQTT disconnected from %s: %+v", endpoint, e)
		}).
		SetTLSConfig(&tls.Config{
			InsecureSkipVerify: mqttd.SkipCertVerify,
		})

	if mqttd.Password != "" {
		if mqttd.Username == "" {
			return errors.New("mqtt has password, but no username")
		}
		options.SetUsername(mqttd.Username)
		options.SetPassword(mqttd.Password)
	}

	if mqttd.ClientID != "" {
		options.SetClientID(mqttd.ClientID)
	}

	mqttd.client = mqtt.NewClient(options)

	return nil
}

func (mqttd *MQTTTask) Execute(ctx context.Context, w io.Writer) error {
	if len(mqttd.Topics) == 0 {
		return nil
	}

	remaining := getRemaining(ctx)
	if remaining.Seconds() <= 0 {
		return errors.New("deadline has expired")
	}

	client := mqttd.client
	if !client.IsConnected() {
		or := mqttd.client.OptionsReader()
		logrus.Debugf("Connecting to MQTT at: %+v", or.Servers())
		if token := client.Connect(); token.WaitTimeout(remaining) && token.Error() != nil {
			return errors.Wrap(token.Error(), "check host and security settings")
		}
	}

	// update the time remaining
	remaining = getRemaining(ctx)
	if remaining.Seconds() <= 0 {
		return errors.New("deadline has expired")
	}

	msg := mqttd.Message
	logrus.Debugf("Publishing %d bytes to %d topic(s) %s",
		len(msg), len(mqttd.Topics), mqttd.Topics)

	errChan := make(chan error, len(mqttd.Topics))
	for i := range mqttd.Topics {
		go func(topic string) {
			token := client.Publish(topic, 0, false, msg)
			if !token.WaitTimeout(remaining) {
				errChan <- errors.Errorf("timed out publishing to %s", topic)
			} else {
				err := token.Error()
				if err != nil {
					errChan <- errors.WithStack(err)
				} else {
					errChan <- nil
				}
			}
		}(mqttd.Topics[i])
	}

	var errs []string
	count := 0
	for err := range errChan {
		count++
		if err != nil {
			errs = append(errs, fmt.Sprintf("%+v", err))
		}
		if count == len(mqttd.Topics) {
			close(errChan)
		}
	}

	if len(errs) != 0 {
		return errors.Errorf("failed to publish to some topics: %s",
			strings.Join(errs, "\n-----\n"))
	}

	return nil
}

// unmarshalPartial unmarshals a partially unmarshaled JSON object into a struct.
//
// The purpose of this function is letting a struct be "partially" filled with
// some initial data, then later, new data is presented as JSON and merged with
// the original struct. It's not the prettiest implementation.
func unmarshalPartial(partial map[string][]byte, s interface{}) error {
	v := reflect.ValueOf(s)
	if !v.IsValid() || v.Kind() != reflect.Ptr {
		return errors.New("destination must be a non-nil pointer to a struct")
	}
	v = v.Elem()
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return errors.New("destination must be a non-nil pointer to a struct")
	}
	t := v.Type()

	getData := func(name string) ([]byte, bool) {
		// try exact match first
		if d, ok := partial[name]; ok {
			return d, true
		}

		// look for a case-insensitive match
		for key, data := range partial {
			if strings.EqualFold(key, name) {
				return data, true
			}
		}

		// no luck
		return nil, false
	}

	for i := 0; i < t.NumField(); i++ {
		field := v.Field(i)
		tField := t.Field(i)

		// logrus.Debugf("unmarshal %d: %+v %+v", i, field, tField)

		jsonTag := tField.Tag.Get("json")
		if jsonTag == "-" || tField.PkgPath != "" {
			// logrus.Debugf("skipping unexported field %+v", tField)
			continue
		}
		if !field.CanSet() {
			// logrus.Debugf("skipping unsettable field %+v", tField)
			continue
		}

		var data []byte
		jsonOpts := strings.Split(jsonTag, ",")
		if len(jsonOpts) > 0 && jsonOpts[0] != "" {
			// logrus.Debugf("using exact JSON tag match: %s", jsonOpts[0])
			// requires exact match
			if d, ok := partial[jsonOpts[0]]; ok {
				data = d
			}
		} else {
			if d, ok := getData(tField.Name); ok {
				data = d
			}
		}

		if data == nil {
			// logrus.Debugf("no data for field %+v", tField)
			continue
		}

		if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.Uint8 {
			// logrus.Debugf("setting %s to raw bytes", tField.Name)
			field.SetBytes(data)
			continue
		}

		// try unmarshaling
		x := reflect.New(tField.Type).Interface()
		// logrus.Debugf("unmarshaling %s", data)
		if err := json.Unmarshal(data, x); err != nil {
			return errors.Wrapf(err, "failed to unmarshal field '%s'", tField.Name)
		}

		// assign the result
		xVal := reflect.ValueOf(x).Elem()
		xType := xVal.Type()
		if xType.AssignableTo(field.Type()) {
			// logrus.Debugf("setting %s to %+v", tField.Name, xVal)
			field.Set(xVal)
		} else if xType.ConvertibleTo(field.Type()) {
			// logrus.Debugf("converted %T to %T: %+[1]v to %+v", xVal, x)
			field.Set(xVal.Convert(field.Type()))
		} else {
			return errors.Errorf("cannot assign nor convert %s (type %s) "+
				"from type %s as '%s'", tField.Name, field.Kind(), xType.Kind(), data)
		}
	}

	return nil
}

// getRemaining returns the amount of time remaining before a context will expire.
func getRemaining(ctx context.Context) time.Duration {
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(time.Second * time.Duration(defaultTimeout))
	}

	remaining := time.Until(deadline)
	if remaining.Seconds() < 0 {
		return 0
	}
	return remaining
}
