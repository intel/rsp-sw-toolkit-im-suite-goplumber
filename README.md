# Goplumber
_Go plumb your data_

Goplumber is an [ETL framework](https://en.wikipedia.org/wiki/Extract,_transform,_load) 
based around [Go's templates](https://golang.org/pkg/text/template/).

## Quickstart
Create a pipeline runner and add a pipeline to it:

```go
// PipelineConnector abstracts load & store operations
connections := goplumber.PipelineConnector{
	TemplateLoader: goplumber.NewFSLoader(config.TemplateDirectory),    // file system loader
	KVData:         goplumber.NewMemoryStore(),                         // memory cache
	Secrets:        goplumber.NewDockerSecretsStore(config.SecretsPath) // FSLoader that rejects Put
}

// create a pipeline from some JSON configuration data
p, err := goplumber.NewPipeline(somePipelineJSON, connections)
if err != nil {
	log.Fatal(err)
}

// a runner automates pipeline triggers; it uses a Context for cancellation.
runner := goplumber.NewPipelineRunner()
runner.AddPipeline(context.TODO(), p)
```

## Concepts
Like many ETL frameworks, `goplumber` is based around _pipelines_ of _tasks_,
which currently are specified via `JSON` configuration. Unlike many other ETL
frameworks, transformations are primarily defined as `template` tasks. The input
to a task is a `map[string][]byte`, which works very naturally with Go's templates. 
The output of a task is `[]byte` data, which may then serve as input to other tasks.

Most task's configuration will look something like this `HTTP` task:

```json
{
  "myTask": {
    "type": "http",
    "raw": {
      "method": "POST",
      "url": "http://example.com/"
    },
    "links": {
      "body": {"from": "someOtherTask"}
    }
  }
}
```

The task's name is `myTask`, which will appear in log message and can be used to 
link other tasks that depend on its output. The `raw` and `links` keys give the
input parameters: the specific keys/parameters valid for a task depend on its
_type_. In this example, `method`, `url`, and `body` are parameters for an `http` 
task; the `method` and `url` are `raw` values, while its `body` is linked from a
task named `someOtherTask`.

Task input is specified in the configuration as either `raw` values or `links`. 
`goplumber` builds a dependency graph of the tasks from the config. When the 
pipeline is _triggered_, it executes tasks in dependency order. It merges a task's
`raw` and `links` parameters into a map which is then passed to the task.

Most tasks (see info about `template` tasks below) allow all parameters to be 
either `raw` or `links`. A key should not appear in both the `raw` and `links`
section. All `links` must reference the name of a valid task in the pipeline.

`links` support some other syntax to handle some common cases. For example,
it's common for tasks to declare `links` to tasks that output `json`-formatted 
data. Rather than requiring an intermediary `template` task to extract properties 
from `json` objects, `links` can include an `elem` property, specifying a key
to extract, e.g.: `links: {"url": {"from": "pipelineConfig", "elem": "myendpoint"} }`.
This expects the output of the `pipelineConfig` task to be a `json` object with
a key named `myendpoint`. Currently, this syntax does not support nested keys.

While a pipeline is executing, some metadata is stored about its tasks -- things
like when it started or stopped, or whether it contained an error. This data is
stored as its `status`, which may be accessed in `links` via `"using": "status"`.
Here's how it may be combined with `elem` to extract the completion time for a
task: `{"value": {"from": "publish", "using": "status", "elem": "CompletedAt"}}`.

### Template Tasks
Since template tasks are the main transform in `goplumber`, they have some special
behavior. In Go, templates are defined within a _namespace_ of templates, within
which templates may call each other by name. When Go parses template data, it
creates a map of template names in the namespace to their definitions; if it
encounters a new template definition with the same name as one already in the
namespace, the new definition overwrites the old one. This allows a user to create
reusable, composable templates definitions. 

A template task's namespace is formed by loading all of the names given in its
`namespaces` list. Each namespace defines one or more templates, loaded in order. 
Although how these are loaded is determined by a pipeline's `loader`s (discussed
elsewhere), you can think of these definitions as existing in regular files.
Additionally, the pipeline's `templateRefs` parameter can specify a list of 
namespaces to prepend _every_ template task, making it easy to store common
definitions in only one or a few places. 

The task's `template` parameter gives the name of the entrypoint template that 
run when the task executes. Naturally, this name must be defined in at least one
of the task's namespaces. You **must** define a template task's `template` name 
and `namespaces` as `raw` parameters. 

#### Example:
```json
{
  "myTemplateTask": {
    "type": "template",
    "raw": {
      "template": "myURLBuilderTemplate",
      "namespaces": ["urlTemplates"],
      "initialData": {
        "baseURL": "http://example.com/endpoint"
      }
    },
    "links": {
      "someParam": {"from": "anotherTask"}
    }    
  }
}
```

The input map for the template consists of the `raw.initialData` merged with 
the data stored in `links`. As for all tasks, the input is `map[string][]byte`,
and the output is `[]byte`. This allows, for instance, the output of a template
to be the raw data for an `http` task, but it leads to a notable gotcha: if you
wish to use the output of a template in a `json` context, the template must
produce valid `json` data. Additionally, if the template wishes to use linked 
`json` object, it must first convert the `json` data to extract its properties.

For example, using the above task definition, let's say a template is used to 
construct the `url` parameter for an `http` task. The `someParam` link from
`anotherTask` is expected to be a `json` with a `param` key. We'll need the 
template to unmarshal `someParam` and to produce a valid `json` string (surrounded
with `"`s):

```gotemplate
{{define "myURLBuilderTemplate"}}
{{- /* constructs a URL for a request */ -}}
"{{.baseURL}}?qp={{with json .someParam}}{{.param | urlquery}}{{end}}"
{{- end -}}
```

#### Template Functions
The following template functions are available (see code docs for more details):

- `timestamp`: current Unix time in ms as an int64.
- `formatTime`: convert int64 Unix ms timestamp to string.
- `formatCurrentTime`: like above, but uses the current time.
- `outboundIP`: returns a `net.IP` that the service uses for outbound requests.
- `int`: converts a number, string, byte, or byte array to an int.
- `add`: returns the sum of multiple `float64`s.
- `str`: converts a `[]byte` to a Go `string`.
- `bytes`: converts a Go `string` to a `[]byte`.
- `json`: unmarshals `[]byte` (or `json.RawMessage`) into a Go `map[string]interface{}`.
- `dec64`: decodes a base-64 encoded `string` into a `[]byte`
- `enc64`: encodes a `[]byte` to a base-64 encoded `string`.
- `join`: same as `strings.Join`.
- `split`: same as `strings.Split`.
- `splitN`: same as `strings.SplitN`.
- `strIdx`: same as `strings.Index`.
- `trimSpace`: same as `strings.TrimSpace`.
- `err`: immediately forces a template to stop execution and return an error.

### List of Tasks
Currently `goplumber` supports the following tasks; for a list of their parameters,
see the code's documentation:

- `template`: as described above, the main transformation mechanism.
- `http`: make an HTTP request to a URL with a given body, headers, and method.
- `mqtt`: publish byte data to a list of topics on an MQTT broker.
- `secret`: load a Docker secret.
- `get`: load data from a key/value store.
- `put`: store data in a key/value store.
- `validation`: validate a task's input map against a JSON schema.
- `json`: convenience task for data defined directly in a pipeline definition; 
    it's probably better to load such data from a key/value store in most cases.

## Triggers
A pipeline executes when it is _triggered_. A pipeline's definition specifies
which triggers, if any, it should use (in principle, it's reasonable to use this
library only with manually triggered pipelines). A pipeline doesn't do anything
with the trigger definition -- it serves only as metadata about the pipeline.
Instead, a _pipeline runner_ interprets triggers to handle their execution. 

Currently, the only defined trigger is the `interval` trigger, which specifies 
that a task should execute on a regular interval. When a pipeline is added to the
runner, it executes the pipeline immediately, then waits for its `interval`, then
executes the pipeline again. This repeats until the runner is canceled, which is
handled via a Go `context`.

## Error Handling
Currently, `goplumber` has very basic error handling: it tracks pipeline task
execution, and if a task fails, the entire pipeline is canceled immediately.
Future work will track transient versus permanent errors, retry tasks that fail
for transient reasons, and allow pipelines to execute tasks conditionally based
on other task's statuses. 

If a pipeline panics during execution, it halts and errors are logged for it, but
it won't (shouldn't) bring down the service.

### Declaring Order Dependencies
Sometimes, one task should only execute after another task, even though it doesn't
require any of that second task's output. For example, if you're validating that
a task's output matches a JSON schema, you likely only care that the validation
task completed successfully. You can declare such a dependency in the task via
the `ifSuccessful` key, which defines a list of task names that must first complete
(successfully) before the task will start. In future work, this key will change
to `conditions` and provide more options -- for instance, the ability to run
tasks if other tasks _fail_. 

## Future Work
As described in other sections above, the following "todo" work remains:

- Error Handling
  - Distinguish transient versus permanent errors.
  - Retry tasks with transient errors using exponential backoff.
  - Allow configurable number of retries.
  - Improve initial configuration error detection.
- Conditional Task Execution
  - Allow tasks to declare conditions which must be satisfied before execution.
  - Incorporate this data into the dependency graph to ensure tasks run in the
  correct order.
- Triggers
  - HTTP 
    - When configured, construct an endpoint for POST requests to execute the pipeline.
    - Allow initial data to come from this POST request.
  - MQTT/0-MQ
    - When configured, listen to a broker/topic to trigger the pipeline.
    - Allow initial data to come from this publication.
- Template 
  - Allow loading templates from loaders other than the filesystem.
  - Provide endpoints to modify templates without restarting the service.
  - Provide an endpoint that accepts input data and a list of template namespaces
  to make it easy to test a template configuration.
- Tasks
  - Abstract `get`/`store` slightly more to make it easier to add new loaders.
  - Possibly replace `secrets` using the above logic.
  - Add `consul` and `mongo` stores.
   
