package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strings"
)

func (svc *ServiceContext) createBondLocations(js *jobStatus, params map[string]interface{}) {
	svc.logInfo(js, fmt.Sprintf("start script to create locations for bond papers"))
	csvFileName, found := params["fileName"].(string)
	if found == false {
		svc.logFatal(js, "missing required fileName param")
		return
	}

	svc.logInfo(js, "lookup container type box")
	var boxContainer containerType
	err := svc.GDB.Where("name=?", "Box").First(&boxContainer).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("unable to load box container type: %s", err.Error()))
		return
	}

	csvPath := path.Join(svc.ProcessingDir, "bondpapers", csvFileName)
	svc.logInfo(js, fmt.Sprintf("read locations from %s", csvPath))
	recs, err := readCSV(csvPath)
	if err != nil {
		svc.logFatal(js, err.Error())
	}

	cnt := 0
	for i, line := range recs {
		if i == 0 {
			// the first row is the csv header... skip it
			continue
		}

		// col 8 contains box folder info that is formatted: Box 3 Folder 28
		boxFolder := line[8]
		svc.logInfo(js, fmt.Sprintf("add location %s", boxFolder))
		bits := strings.Split(boxFolder, " ")
		boxNum := bits[1]
		folderNum := bits[3]
		callNum := fmt.Sprintf("MSS 13347 Box %s", boxNum)
		var tgtMD metadata
		err = svc.GDB.Preload("Locations").Where("call_number=?", callNum).First(&tgtMD).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("unable to find metadata %s", callNum))
			continue
		}

		hasLocation := false
		for _, loc := range tgtMD.Locations {
			if loc.FolderID == folderNum {
				hasLocation = true
				break
			}
		}

		if hasLocation == false {
			svc.logInfo(js, fmt.Sprintf("add location %s to metadata %s", boxFolder, tgtMD.PID))
			newLoc := location{MetadataID: tgtMD.ID, ContainerTypeID: boxContainer.ID, FolderID: folderNum, ContainerID: boxNum}
			err = svc.GDB.Create(&newLoc).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("unable to create %s: %s", boxFolder, js.Error))
				continue
			}
			cnt++
		} else {
			svc.logInfo(js, fmt.Sprintf("metadata %s already has location %s", tgtMD.PID, boxFolder))
		}
	}

	svc.logInfo(js, fmt.Sprintf("%d locations created", cnt))
	svc.jobDone(js)
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
