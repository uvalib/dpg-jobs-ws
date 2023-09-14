package main

import (
	"fmt"
	"path"
)

func (svc *ServiceContext) createBondLocations(js *jobStatus, params map[string]interface{}) {
	svc.logInfo(js, fmt.Sprintf("start script to create locations for bond papers"))
	csvFileName, found := params["fileName"].(string)
	if found == false {
		svc.logFatal(js, "missing required fileName param")
		return
	}
	csvPath := path.Join(svc.ProcessingDir, "bondpapers", csvFileName)
	svc.logInfo(js, fmt.Sprintf("read locations from %s", csvPath))

	svc.logFatal(js, "Not implemented")
}

func (svc *ServiceContext) createBondUnits(js *jobStatus, params map[string]interface{}) {
	svc.logInfo(js, fmt.Sprintf("start script to create units for bond papers"))

	svc.logFatal(js, "Not implemented")
}

func (svc *ServiceContext) ingestBondFolder(js *jobStatus, params map[string]interface{}) {
	svc.logInfo(js, fmt.Sprintf("start script to ingest data from one bond folder"))

	svc.logFatal(js, "Not implemented")
}
