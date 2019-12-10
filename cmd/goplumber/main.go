/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.impcloud.net/RSP-Inventory-Suite/goplumber"
	"io/ioutil"
	"os"
	"os/signal"
	"time"
)

func main() {
	conf := flag.String("config", "plumber.json", "configuration file")
	flag.Parse()

	data, err := ioutil.ReadFile(*conf)
	if err != nil {
		log.Fatal(err)
	}

	pc := PlumberConfig{}
	if err := json.Unmarshal(data, &pc); err != nil {
		log.Fatal(err)
	}

	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	if err := loadPipelines(ctx, pc); err != nil {
		log.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Debug("Received shutdown; canceling pipelines.")
}

type PlumberConfig struct {
	ConfigDir      string
	MQTTConfigFile string
	CustomTasks    []string
	PipelineNames  []string
}

func loadPipelines(ctx context.Context, config PlumberConfig) error {
	log.Debug("Starting pipelines.")
	plumber := goplumber.NewPlumber()

	loader := goplumber.NewFileSystem(config.ConfigDir)
	plumber.SetTemplateSource("template", loader)
	plumber.SetSource("secret", loader)
	plumber.SetSink("saveFile", loader)

	kvData := goplumber.NewMemoryStore()
	plumber.SetSource("get", kvData)
	plumber.SetSink("put", kvData)

	mqttData, err := loader.GetFile(config.MQTTConfigFile)
	if err != nil {
		return err
	}

	mqc := goplumber.MQTTClient{}
	if err := json.Unmarshal(mqttData, &mqc); err != nil {
		return errors.Wrap(err, "failed to unmarshal MQTT client")
	}
	plumber.SetSink("mqtt", &mqc)

	log.Debug("Loading custom task types from pipelines.")
	for _, name := range config.CustomTasks {
		data, err := loader.GetFile(name)
		if err != nil {
			return err
		}

		var pConf goplumber.PipelineConfig
		if err := json.Unmarshal(data, &pConf); err != nil {
			return errors.Wrapf(err, "failed to unmarshal %s", name)
		}

		taskType, err := plumber.NewPipeline(&pConf)
		if err != nil {
			return errors.Wrapf(err, "failed to load %s", name)
		}
		client, err := goplumber.NewTaskType(taskType)
		if err != nil {
			return errors.Wrapf(err, "failed to create client for %s", name)
		}
		plumber.SetClient(pConf.Name, client)
	}

	// only load the configured names
	log.Debug("Loading pipelines.")
	pipelines := map[*goplumber.Pipeline]time.Duration{}
	for _, name := range config.PipelineNames {
		data, err := loader.GetFile(name)
		if err != nil {
			return errors.Wrapf(err, "failed to pipeline %s", name)
		}

		var pipelineConf goplumber.PipelineConfig
		if err := json.Unmarshal(data, &pipelineConf); err != nil {
			return errors.Wrapf(err, "failed to unmarshal %s", name)
		}

		p, err := plumber.NewPipeline(&pipelineConf)
		if err != nil {
			return errors.Wrapf(err, "failed to load %s", name)
		}
		pipelines[p] = pipelineConf.Trigger.Interval.Duration()
	}

	log.Debug("Running pipelines.")
	for p, d := range pipelines {
		if d > 0 {
			goplumber.RunPipelineForever(ctx, p, d)
		} else {
			go goplumber.RunNow(ctx, p)
		}
	}

	return nil
}
