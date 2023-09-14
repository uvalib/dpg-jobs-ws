package main

import (
	"encoding/csv"
	"fmt"
	"os"
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
	recs, err := readCSV(csvPath)
	if err != nil {
		svc.logFatal(js, err.Error())
	}

	// col 1: title, col 8: BOX/FOLDER, col 9: num pages, col 10: filenames with | sep
	// Box format: "Box # Folder #"
	for i, line := range recs {
		if i == 0 {
			continue
		}
		boxFolder := line[8]
		svc.logInfo(js, fmt.Sprintf("add location %s", boxFolder))
	}

	svc.logFatal(js, "Not implemented")
}

func (svc *ServiceContext) createBondUnits(js *jobStatus, params map[string]interface{}) {
	svc.logInfo(js, fmt.Sprintf("start script to create units for bond papers"))
	csvFileName, found := params["fileName"].(string)
	if found == false {
		svc.logFatal(js, "missing required fileName param")
		return
	}
	csvPath := path.Join(svc.ProcessingDir, "bondpapers", csvFileName)
	svc.logInfo(js, fmt.Sprintf("read locations from %s", csvPath))
	recs, err := readCSV(csvPath)
	if err != nil {
		svc.logFatal(js, err.Error())
	}

	// col 1: title, col 8: BOX/FOLDER, col 9: num pages, col 10: filenames with | sep
	// Box format: "Box # Folder #"
	for i, line := range recs {
		if i == 0 {
			continue
		}
		boxFolder := line[8]
		title := line[1]
		svc.logInfo(js, fmt.Sprintf("add unit for %s: %s", boxFolder, title))
	}

	svc.logFatal(js, "Not implemented")
}

func (svc *ServiceContext) ingestBondFolder(js *jobStatus, params map[string]interface{}) {
	svc.logInfo(js, fmt.Sprintf("start script to ingest data from one bond folder"))

	svc.logFatal(js, "Not implemented")
}

func readCSV(filePath string) ([][]string, error) {
	csvFile, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open %s: %s", filePath, err.Error())
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvRecs, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("unable to read csv: %s", err.Error())
	}

	return csvRecs, nil
}
