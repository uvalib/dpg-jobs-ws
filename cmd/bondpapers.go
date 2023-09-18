package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"golang.org/x/exp/slices"
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
				svc.logError(js, fmt.Sprintf("unable to create %s: %s", boxFolder, err.Error()))
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

	rawOrderID, found := params["orderID"].(float64)
	if found == false {
		svc.logFatal(js, "missing required orderID param")
		return
	}
	tgtOrder := order{ID: int64(rawOrderID)}
	err = svc.GDB.First(&tgtOrder).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("order %d not found: %s", tgtOrder.ID, err.Error()))
		return
	}

	tgtBox, found := params["box"].(string)
	if found {
		svc.logInfo(js, fmt.Sprintf("process files from box %s", tgtBox))
	}
	tgtFolder, found := params["folder"].(string)
	if found {
		svc.logInfo(js, fmt.Sprintf("process files from folder %s", tgtFolder))
	}

	cnt := 0
	updated := 0
	indendedUseID := int64(110) // digital collection building
	chunkRegex := regexp.MustCompile(`\s\((Doc\s)?[1-9]{1}\sof\s[1-9]{1}\)$`)
	for i, line := range recs {
		if i == 0 {
			continue
		}

		// title may appear multiple times with different prefix / suffix
		// suffix looks like (Doc # of #) or (# of #). This happens when a folder was scanned in multiple chunks; strip and ignore
		title := line[1]
		title = chunkRegex.ReplaceAllString(title, "")

		// extract box/folder info
		boxFolder := line[8]
		bits := strings.Split(boxFolder, " ")
		boxNum := bits[1]
		folderNum := bits[3]
		callNum := fmt.Sprintf("MSS 13347 Box %s", boxNum)
		ingestFolder := fmt.Sprintf("mss13347-b%s-f%s", boxNum, folderNum) // directory name for src images

		if tgtBox != "" && tgtBox != boxNum {
			continue
		}
		if tgtFolder != "" && tgtFolder != folderNum {
			continue
		}

		// get the image list, clean it up and sort
		var images []string
		for _, img := range strings.Split(line[10], "|") {
			img = strings.TrimSpace(img)
			images = append(images, img)
		}

		// get parent metadata record...
		var tgtMD metadata
		err = svc.GDB.Where("call_number=?", callNum).First(&tgtMD).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("unable to find metadata %s", callNum))
			continue
		}

		// see if a unit for this record already exists
		var tgtUnit unit
		svc.GDB.Where("metadata_id=? and staff_notes=?", tgtMD.ID, title).Limit(1).Find(&tgtUnit)
		if tgtUnit.ID == 0 {
			svc.logInfo(js, fmt.Sprintf("create unit for %s", ingestFolder))
			si := fmt.Sprintf("Ingest from: %s\nImages: %s", ingestFolder, sortImages(strings.Join(images, ",")))
			newUnit := unit{OrderID: tgtOrder.ID, MetadataID: &tgtMD.ID, UnitStatus: "approved", IntendedUseID: &indendedUseID,
				CompleteScan: true, StaffNotes: title, SpecialInstructions: si}
			err = svc.GDB.Create(&newUnit).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("unable to create unit for %s: %s", ingestFolder, err.Error()))
				continue
			}
			cnt++
		} else {
			if strings.Contains(tgtUnit.SpecialInstructions, "Images:") == false {
				svc.logInfo(js, fmt.Sprintf("add first batch of images to unit %d", tgtUnit.ID))
				si := tgtUnit.SpecialInstructions + "\nImages: " + sortImages(strings.Join(images, ","))
				err = svc.GDB.Model(&tgtUnit).Update("special_instructions", si).Error
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to add image list to unit %d: %s", tgtMD.ID, err.Error()))
					continue
				}
				updated++
			} else {
				// images are present, see if it contains the current batch
				if strings.Contains(tgtUnit.SpecialInstructions, images[0]) == false {
					svc.logInfo(js, fmt.Sprintf("add batch of images to unit %d", tgtUnit.ID))
					bits := strings.Split(tgtUnit.SpecialInstructions, ":")
					siImages := bits[len(bits)-1]
					siImages = siImages + "," + strings.Join(images, ",")
					siImages = sortImages(siImages)
					si := strings.Split(tgtUnit.SpecialInstructions, "\n")[0] + "\nImages: " + siImages
					err = svc.GDB.Model(&tgtUnit).Update("special_instructions", si).Error
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to add image list to unit %d: %s", tgtMD.ID, err.Error()))
						continue
					}
					updated++
				}
			}
		}
	}

	svc.logInfo(js, fmt.Sprintf("%d units created", cnt))
	svc.jobDone(js)
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

func sortImages(images string) string {
	y := strings.Split(images, ",")
	slices.SortFunc(y,
		func(a, b string) int {
			aSeq := strings.TrimSuffix(a, ".tif")
			bits := strings.Split(a, "_")
			aSeq = bits[len(bits)-1]
			bSeq := strings.TrimSuffix(b, ".tif")
			bits = strings.Split(b, "_")
			bSeq = bits[len(bits)-1]
			if aSeq > bSeq {
				return 1
			}
			return -1
		})
	return strings.Join(y, ",")
}
