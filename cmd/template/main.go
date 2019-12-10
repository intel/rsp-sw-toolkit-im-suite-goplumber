package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.impcloud.net/RSP-Inventory-Suite/goplumber"
	"log"
	"strings"
)

func main() {
	tmplDir := flag.String("tdir", ".", "directory containing templates")
	namespaces := flag.String("namespaces", "","comma separated list of namespaces to load from template dir")
	tmplName := flag.String("tname", "","name of the template to execute")
	inputFile := flag.String("input", "", "filename containing JSON input for template")
	flag.Parse()

	if len(*namespaces) == 0 {
		log.Fatal("namespaces must be set")
	}
	needNS := strings.Split(*namespaces, ",")
	if len(needNS) == 0 {
		log.Fatal("need at least one namespace to load")
	}
	if *tmplName == "" {
		*tmplName = needNS[0]
	}

	fsLoader := goplumber.NewFileSystem(*tmplDir)

	ns, err := goplumber.LoadNamespace(fsLoader, needNS)
	if err != nil {
		log.Fatal(err)
	}

	var input map[string]json.RawMessage
	if *inputFile != "" {
		inputData, err := fsLoader.GetFile(*inputFile)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(inputData, &input); err != nil {
			log.Fatal(err)
		}
	}

	result := &bytes.Buffer{}
	if err := ns.ExecuteTemplate(result, *tmplName, input); err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}
