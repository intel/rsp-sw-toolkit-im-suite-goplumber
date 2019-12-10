/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

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

// LoadTask loads data from a DataSource using a key. If the key isn't present,
// it'll return the Default, which may be nil.
type LoadTask struct {
	Key     string          `json:"name"`
	Default json.RawMessage `json:"default"` // default value, if not present
	source  DataSource
}

// StoreTask sends data to a Sink.
type StoreTask struct {
	Key   string `json:"name"`
	Value json.RawMessage
	sink  Sink
}

// NewSourceClient returns a Client to load data from the DataSource.
func NewSourceClient(source DataSource) Client {
	return JSONPipe(func(task *Task) (Pipe, error) {
		return &LoadTask{source: source}, nil
	})
}

// NewSinkClient returns a Client to store data in a given Sink.
func NewSinkClient(sink Sink) Client {
	return JSONPipe(func(task *Task) (Pipe, error) {
		return &StoreTask{sink: sink}, nil
	})
}

// Execute by loading data from a source.
func (task *LoadTask) Execute(ctx context.Context, w io.Writer, d linkMap) error {
	// create a new load task to unmarshal the input
	lt := *task
	if err := unmarshalMap(d, &lt); err != nil {
		return err
	}

	if lt.Key == "" {
		return errors.New("missing key name")
	}
	v, ok, err := task.source.Get(ctx, lt.Key)
	if err != nil {
		return err
	}
	if !ok {
		logrus.Debugf("missing %s; using default %s", lt.Key, lt.Default)
		v = lt.Default
	}
	if v != nil {
		_, err = w.Write(v)
	}
	return err
}

func (task *StoreTask) Execute(ctx context.Context, w io.Writer, d linkMap) error {
	st := *task
	if err := unmarshalMap(d, &st); err != nil {
		return err
	}
	if st.Key == "" {
		return errors.New("missing key name")
	}
	return task.sink.Put(ctx, st.Key, st.Value)
}

// HTTPTask executes an HTTP request.
type HTTPTask struct {
	MaxRetries     int                 `json:"maxRetries"`
	Method         string              `json:"method,omitempty"`
	URL            string              `json:"url,omitempty"`
	Body           json.RawMessage     `json:"body,omitempty"`
	Headers        map[string][]string `json:"headers,omitempty"`
	SkipCertVerify bool                `json:"skipCertVerify"`
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

func (task *HTTPTask) Execute(ctx context.Context, w io.Writer, d linkMap) error {
	httpPipe := *task
	if err := unmarshalMap(d, &httpPipe); err != nil {
		return err
	}

	if httpPipe.Method == "" {
		return errors.New("missing method for HTTP task")
	}
	if httpPipe.URL == "" {
		return errors.New("missing URL for HTTP task")
	}
	if httpPipe.MaxRetries < 1 {
		logrus.Debugf("Forcing HTTPTask to have MaxRetries of at least 1, "+
			"instead of value %d", httpPipe.MaxRetries)
		httpPipe.MaxRetries = 1
	}

	var client *http.Client
	if httpPipe.SkipCertVerify {
		logrus.Debug("Using insecure HTTP client to skip SSL certificate verification")
		client = getInsecureClient()
	} else {
		client = http.DefaultClient
	}

	var bodyReader io.Reader
	if httpPipe.Body != nil {
		bodyReader = bytes.NewReader(httpPipe.Body)
	}

	request, err := http.NewRequest(httpPipe.Method, httpPipe.URL, bodyReader)
	if err != nil {
		return errors.Wrap(err, "unable to create http request")
	}

	// add headers
	if httpPipe.Headers != nil {
		for headerKey, headers := range httpPipe.Headers {
			for _, h := range headers {
				request.Header.Add(headerKey, h)
			}
		}
	}

	request = request.WithContext(ctx)

	logrus.
		WithField("method", httpPipe.Method).
		WithField("url", httpPipe.URL).
		WithField("bodyLength", len(httpPipe.Body)).
		WithField("numHeaders", len(httpPipe.Headers)).
		Debug("Executing HTTP task")

	response, err := client.Do(request)
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

type JSONValidationTask struct {
	Content     []byte `json:"content"`
	SchemaBytes []byte `json:"schema"`
}

func (jvt *JSONValidationTask) Execute(ctx context.Context, w io.Writer, d linkMap) error {
	pipe := *jvt
	if err := unmarshalMap(d, &pipe); err != nil {
		return err
	}
	if pipe.Content == nil {
		return errors.New("JSON validation task has no content to validate")
	}
	if len(pipe.SchemaBytes) == 0 {
		return errors.New("JSON validation task has no schema to validate against")
	}

	l := gojsonschema.NewBytesLoader(pipe.SchemaBytes)
	schema, err := gojsonschema.NewSchema(l)
	if err != nil {
		return errors.Wrapf(err, "unable to load schema")
	}

	dataLoader := gojsonschema.NewBytesLoader(pipe.Content)

	result, err := schema.Validate(dataLoader)
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

type MQTTClient struct {
	client         mqtt.Client
	ClientID       string
	Username       string
	Password       string
	Endpoint       string
	TimeoutSecs    int
	SkipCertVerify bool
}

func (mqttClient *MQTTClient) UnmarshalJSON(data []byte) error {
	logrus.Debug("Unmarshaling client")

	type mc_ MQTTClient
	var mc mc_
	if err := json.Unmarshal(data, &mc); err != nil {
		return err
	}
	*mqttClient = (MQTTClient)(mc)

	if mqttClient.Endpoint == "" {
		return errors.New("missing endpoint for MQTT task")
	}
	endpoint := mqttClient.Endpoint
	if !strings.Contains(mqttClient.Endpoint, "://") {
		endpoint = "tcp://" + endpoint
	}

	purl, err := url.Parse(endpoint)
	if err != nil {
		return errors.Wrap(err, "invalid URL")
	}
	endpoint = purl.String()

	if mqttClient.TimeoutSecs == 0 {
		logrus.Debugf("Setting timeout to 30s")
		mqttClient.TimeoutSecs = 30
	}

	options := mqtt.NewClientOptions().
		AddBroker(endpoint).
		SetOnConnectHandler(func(client mqtt.Client) {
			logrus.Infof("MQTT connected on %s", endpoint)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, e error) {
			logrus.Warningf("MQTT disconnected from %s: %+v", endpoint, e)
		}).
		SetTLSConfig(&tls.Config{
			InsecureSkipVerify: mqttClient.SkipCertVerify,
		})

	if mqttClient.Password != "" {
		if mqttClient.Username == "" {
			return errors.New("mqtt has password, but no username")
		}
		options.SetUsername(mqttClient.Username)
		options.SetPassword(mqttClient.Password)
	} else if mqttClient.Username != "" {
		return errors.New("mqtt has username, but no password")
	}

	if mqttClient.ClientID != "" {
		options.SetClientID(mqttClient.ClientID)
	}

	mqttClient.client = mqtt.NewClient(options)
	return nil
}

func (mqttClient *MQTTClient) ensureConnected() error {
	client := mqttClient.client
	if client.IsConnected() {
		return nil
	}

	logrus.Debugf("Connecting MQTT on %s", mqttClient.Endpoint)
	token := client.Connect()
	timedOut := !token.WaitTimeout(time.Duration(mqttClient.TimeoutSecs) * time.Second)
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "error trying to connect to mqtt")
	}
	if timedOut {
		return errors.New("timed out connecting mqtt")
	}
	if !client.IsConnected() {
		return errors.New("client not connected")
	}
	ct := token.(*mqtt.ConnectToken)
	logrus.Infof("Return code: %v; has session: %v",
		ct.ReturnCode(), ct.SessionPresent())
	return nil
}

type MQTTTask struct {
	Message []json.RawMessage `json:"message"`
}

func (mqttClient *MQTTClient) Put(ctx context.Context, topic string, msg []byte) error {
	remaining := getRemaining(ctx)
	if remaining.Seconds() <= 0 {
		return errors.New("deadline has expired")
	}

	client := mqttClient.client
	if err := mqttClient.ensureConnected(); err != nil {
		return err
	}

	// update the time remaining
	remaining = getRemaining(ctx)
	if remaining.Seconds() <= 0 {
		return errors.New("deadline has expired")
	}

	logrus.Debugf("Publishing %d bytes to topic '%s'", len(msg), topic)

	token := client.Publish(topic, 0, false, msg)
	if !token.WaitTimeout(remaining) {
		return errors.Errorf("timed out publishing to %s", topic)
	}
	token.Wait()
	return token.Error()
}

// unmarshalMap unmarshals a map of bytes, presumably JSON values, into a struct,
// without first remarshaling the entire thing to bytes.
func unmarshalMap(partial map[string][]byte, s interface{}) error {
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
			if field.Kind() == reflect.String && len(data) > 0 &&
				(data[0] != byte('"') || data[len(data)-1] != byte('"')) {
				return errors.Wrapf(err, "failed to unmarshal field '%s'; "+
					"note: this field is a string type -- does it need \"quotes\"? ",
					tField.Name)
			}
			return errors.Wrapf(err, "failed to unmarshal field '%s' of "+
				"kind %v", field.Kind(), tField.Name)
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
		deadline = time.Now().Add(time.Second * time.Duration(defaultTimeoutSecs))
	}

	remaining := time.Until(deadline)
	if remaining.Seconds() < 0 {
		return 0
	}
	return remaining
}
