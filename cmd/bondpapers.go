package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/exp/slices"
)

// SAMPLE LOCAL CALL
// curl -X POST http://localhost:8180/script -H "Content-Type: application/json" --data '{"computeID": "lf6f", "name": "createBondLocations", "params": {"fileName": "boxes9-12.csv"}}'
func (svc *ServiceContext) createBondLocations(c *gin.Context, js *jobStatus, params map[string]any) error {
	svc.logInfo(js, "start script to create locations for bond papers")
	csvFileName, found := params["fileName"].(string)
	if found == false {
		return fmt.Errorf("missing required fileName param")
	}

	svc.logInfo(js, "lookup container type box")
	var boxContainer containerType
	err := svc.GDB.Where("name=?", "Box").First(&boxContainer).Error
	if err != nil {
		return fmt.Errorf("unable to load box container type: %s", err.Error())
	}

	csvPath := path.Join(svc.ProcessingDir, "bondpapers", csvFileName)
	svc.logInfo(js, fmt.Sprintf("read locations from %s", csvPath))
	recs, err := readCSV(csvPath)
	if err != nil {
		return err
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
		} else {
			svc.logInfo(js, fmt.Sprintf("location %s for call num %s will be attached to metadata %s", boxFolder, callNum, tgtMD.PID))
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
	c.String(http.StatusOK, fmt.Sprintf("%d locations created; check tracksys job %d status for details\n", cnt, js.ID))
	return nil
}

// createBondUnits will generate TrackSys units based on a bond papers inventory CSV. NOTE: each row of the spreadsheet
// is to be considered a unique document and should be added to a unique unit. This will resut in some units for documents with
// the same title, but that is OK as they are just different revisions
// params:
//   - src : the source directory for the bond image mount (ex: /bondpapers/New from Bond Project)
//   - fileName : the file name of the CSV inventory
//   - orderID: the TS order ID for the destination
//   - box: (OPTIONAL) which box of images to igest.  If omitted, all boxes will be processed
//   - folder: (OPTIONAL) folder to ingest. If omitted, the entire box will be ingested
//
// EXAMPLE: curl -X POST https://dpg-jobs.lib.virginia.edu/script -H "Content-Type: application/json" --data '{"computeID": "lf6f", "name": "createBondUnits", "params": {"orderID": 12288, "src": "/mnt/work/bondpapers", "fileName": "boxes9-12.csv", "box": "9"}}'
func (svc *ServiceContext) createBondUnits(c *gin.Context, js *jobStatus, params map[string]any) error {
	svc.logInfo(js, "start script to create units for bond papers")
	bondRoot, found := params["src"].(string)
	if found == false {
		return fmt.Errorf("missing required src param")
	}
	csvFileName, found := params["fileName"].(string)
	if found == false {
		return fmt.Errorf("missing required fileName param")
	}

	csvPath := path.Join(bondRoot, csvFileName)
	svc.logInfo(js, fmt.Sprintf("read unit data from %s", csvPath))
	recs, err := readCSV(csvPath)
	if err != nil {
		return err
	}

	rawOrderID, found := params["orderID"].(float64)
	if found == false {
		return fmt.Errorf("missing required orderID param")
	}
	tgtOrder := order{ID: int64(rawOrderID)}
	err = svc.GDB.First(&tgtOrder).Error
	if err != nil {
		return fmt.Errorf("order %d not found: %s", tgtOrder.ID, err.Error())
	}
	if strings.Contains(tgtOrder.OrderTitle, "Julian Bond Papers") == false {
		return fmt.Errorf("order %d is not for Julian Bond Papers; title [%s]", tgtOrder.ID, tgtOrder.OrderTitle)
	}

	tgtBox, found := params["box"].(string)
	if found {
		svc.logInfo(js, fmt.Sprintf("process units from box %s", tgtBox))
	}
	tgtFolder, found := params["folder"].(string)
	if found {
		svc.logInfo(js, fmt.Sprintf("process units from folder %s", tgtFolder))
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: panic recovered during createBondUnits: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered during createBondUnits: %v", r))
			}
		}()

		cnt := 0
		indendedUseID := int64(110) // digital collection building
		chunkRegex := regexp.MustCompile(`\s\((Doc\s)?[1-9]{1}\sof\s[1-9]{1}\)$`)
		for i, line := range recs {
			if i == 0 {
				continue
			}

			// title may appear multiple times with different prefix / suffix
			// suffix looks like (Doc # of #) or (# of #). Strip it and ignore
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

			svc.logInfo(js, fmt.Sprintf("processing csv line %d", i))

			// get the image list, clean it up and sort
			var images []string
			for _, img := range strings.Split(line[10], "|") {
				img = strings.TrimSpace(img)
				if img != "" {
					images = append(images, img)
				}
			}
			if len(images) == 0 || images == nil {
				svc.logInfo(js, fmt.Sprintf("%s has no images and is being skipped", boxFolder))
				continue
			}
			images = sortImages(images)
			svc.logInfo(js, fmt.Sprintf("first page in new record [%s]", images[0]))

			// get parent metadata record...
			var tgtMD metadata
			err = svc.GDB.Where("call_number=?", callNum).First(&tgtMD).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("unable to find metadata %s", callNum))
				continue
			}

			// see if a unit for this record already exists: IMPORTANT; titles are held in staff_notes and may be duplicated
			// need to pull all that match metadata and title, then check page info to see if its already been processed.
			var tgtUnits []unit
			svc.GDB.Where("metadata_id=? and staff_notes=?", tgtMD.ID, title).Find(&tgtUnits)
			if len(tgtUnits) == 0 {
				svc.logInfo(js, fmt.Sprintf("create unit for %s", ingestFolder))
				si := fmt.Sprintf("Ingest from: %s\nImages: %s", ingestFolder, strings.Join(images, ","))
				newUnit := unit{OrderID: tgtOrder.ID, MetadataID: &tgtMD.ID, UnitStatus: "approved", IntendedUseID: &indendedUseID,
					CompleteScan: true, StaffNotes: title, SpecialInstructions: si}
				err = svc.GDB.Create(&newUnit).Error
				if err != nil {
					svc.logError(js, fmt.Sprintf("unable to create unit for %s: %s", ingestFolder, err.Error()))
					continue
				}
				svc.logInfo(js, fmt.Sprintf("created unit %d for %s", newUnit.ID, ingestFolder))
				cnt++
			} else {
				svc.logInfo(js, fmt.Sprintf("%d units exist for metadata %d title [%s]", len(tgtUnits), tgtMD.ID, title))
			}
		}

		svc.logInfo(js, fmt.Sprintf("%d units created", cnt))
		svc.jobDone(js)
	}()
	c.String(http.StatusOK, fmt.Sprintf("createBondUnits started; check tracksys job %d status for details\n", js.ID))
	return nil
}

// ingestBond images will ingest a number of images from the bond papers image mount
// params:
//   - src : the source directory for the bond image mount (ex: /bondpapers/New from Bond Project)
//   - orderID: the TS order ID for the destination
//   - box: which box of images to igest
//   - folder: (OPTIONAL) folder to ingest. If omitted, the entire box will be ingested
//
// EXAMPLE: curl -X POST https://dpg-jobs.lib.virginia.edu/script -H "Content-Type: application/json" --data '{"computeID": "lf6f", "name": "ingestBondImages", "params": {"orderID": 12288, "src": "/mnt/work/bondpapers/Jan 2024 Delivery", "box": "9", "folder": "21"}}'
func (svc *ServiceContext) ingestBondImages(c *gin.Context, js *jobStatus, params map[string]any) error {
	svc.logInfo(js, "start script to ingest bond images")
	bondRoot, found := params["src"].(string)
	if found == false {
		return fmt.Errorf("missing required src param")
	}
	if pathExists(bondRoot) == false {
		return fmt.Errorf("source path %s does not exist", bondRoot)
	}
	svc.logInfo(js, fmt.Sprintf("ingest root directory: %s", bondRoot))

	rawOrderID, found := params["orderID"].(float64)
	if found == false {
		return fmt.Errorf("missing required orderID param")
	}
	tgtOrder := order{ID: int64(rawOrderID)}
	err := svc.GDB.First(&tgtOrder).Error
	if err != nil {
		return fmt.Errorf("order %d not found: %s", tgtOrder.ID, err.Error())
	}
	if strings.Contains(tgtOrder.OrderTitle, "Julian Bond Papers") == false {
		return fmt.Errorf("order %d is not for Julian Bond Papers; title [%s]", tgtOrder.ID, tgtOrder.OrderTitle)
	}

	tgtBox, found := params["box"].(string)
	if found == false {
		return fmt.Errorf("missing required box param")
	}

	callNum := fmt.Sprintf("MSS 13347 Box %s", tgtBox)
	svc.logInfo(js, fmt.Sprintf("lookup metadata from order %d with call number %s", tgtOrder.ID, callNum))
	var tgtMD metadata
	err = svc.GDB.Preload("Locations").Where("call_number=?", callNum).First(&tgtMD).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("unable to find metadata %s", callNum))
	}

	tgtFolder, found := params["folder"].(string)
	if found {
		svc.logInfo(js, fmt.Sprintf("ingest images from folder %s", tgtFolder))
	} else {
		svc.logInfo(js, "ingest images from all available folders")
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: panic recovered during bond image ingest: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered during ingest: %v", r))
			}
		}()

		//get all of the unis associated with the target order / meetadata record (box)
		var boxUnits []unit
		err = svc.GDB.Where("order_id=? and metadata_id=?", tgtOrder.ID, tgtMD.ID).Find(&boxUnits).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("unable to load units for order %d, metadata %d: %s", tgtOrder.ID, tgtMD.ID, err.Error()))
			return
		}

		cnt := 0
		for _, tgtUnit := range boxUnits {
			if tgtUnit.UnitStatus != "approved" {
				svc.logInfo(js, fmt.Sprintf("skipping unit %d with status [%s]", tgtUnit.ID, tgtUnit.UnitStatus))
				continue
			}
			unitIngestFrom := extractBondImageFolder(&tgtUnit)
			if unitIngestFrom == "" {
				svc.logInfo(js, fmt.Sprintf("skipping unit %d that does not have source image folder in special instructions", tgtUnit.ID))
				continue
			}

			if tgtFolder != "" {
				tgtIngestFrom := fmt.Sprintf("mss13347-b%s-f%s", tgtBox, tgtFolder)
				if unitIngestFrom != tgtIngestFrom {
					continue
				}
			}

			srcDir := path.Join(bondRoot, fmt.Sprintf("Box %s", tgtBox), unitIngestFrom, "TIFF")
			svc.logInfo(js, fmt.Sprintf("ingest folder %s into unit %d", srcDir, tgtUnit.ID))
			if pathExists(srcDir) == false {
				svc.logError(js, fmt.Sprintf("image source dir %s does not exist", srcDir))
				continue
			}

			srcImages := extractBondUnitImageList(&tgtUnit)
			mfPageNum := 0
			for _, imgFN := range srcImages {
				mfPageNum++
				srcImg := path.Join(srcDir, strings.TrimSpace(imgFN))
				svc.logInfo(js, fmt.Sprintf("ingest %s", srcImg))
				if pathExists(srcImg) == false {
					svc.logError(js, fmt.Sprintf("image %s does not exist", srcImg))
					continue
				}

				tsMasterFileName := fmt.Sprintf("%09d_%04d.tif", tgtUnit.ID, mfPageNum)
				newMF, err := svc.loadMasterFile(tsMasterFileName)
				if err != nil {
					svc.logError(js, fmt.Sprintf("error loading masterfile %s: %s", tsMasterFileName, err.Error()))
					continue
				}
				if newMF.ID == 0 {
					svc.logInfo(js, fmt.Sprintf("Create new master file %s", tsMasterFileName))
					newMD5 := md5Checksum(srcImg)
					newFileSize := getFileSize(srcImg)
					desc := ""
					if mfPageNum == 1 {
						desc = tgtUnit.StaffNotes
					}
					newMF = &masterFile{UnitID: tgtUnit.ID, MetadataID: tgtUnit.MetadataID, Filename: tsMasterFileName,
						Filesize: newFileSize, MD5: newMD5, Title: fmt.Sprintf("%d", mfPageNum), Description: desc}
					err = svc.GDB.Create(&newMF).Error
					if err != nil {
						svc.logError(js, fmt.Sprintf("unable to create masterfile %s: %s", tsMasterFileName, err.Error()))
						continue
					}
				} else {
					svc.logInfo(js, fmt.Sprintf("master file %s already exists", tsMasterFileName))
				}

				if newMF.ImageTechMeta.ID == 0 {
					svc.logInfo(js, "Create image tech metadata")
					err = svc.createImageTechMetadata(newMF, srcImg)
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to create image tech metadata: %s", err.Error()))
					}
				} else {
					svc.logInfo(js, "Image tech metadata already exists")
				}

				if len(newMF.Locations) == 0 {
					var tgtLoc *location
					bits := strings.Split(unitIngestFrom, "-")
					unitFolder := strings.Replace(bits[len(bits)-1], "f", "", 1)
					for _, loc := range tgtMD.Locations {
						if loc.ContainerID == tgtBox && loc.FolderID == unitFolder {
							tgtLoc = &loc
							break
						}
					}
					if tgtLoc == nil {
						svc.logError(js, fmt.Sprintf("location record not found for %s", unitIngestFrom))
					} else {
						err = svc.GDB.Exec("INSERT into master_file_locations (master_file_id, location_id) values (?,?)", newMF.ID, tgtLoc.ID).Error
						if err != nil {
							svc.logError(js, fmt.Sprintf("Unable to link location %d to masterfile %d: %s", tgtLoc.ID, newMF.ID, err.Error()))
						}
					}
				} else {
					svc.logInfo(js, "Masterfile already has location info")
				}

				err = svc.publishToIIIF(js, newMF, srcImg, false)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to publish masterfile %d to IIIF: %s", newMF.ID, err.Error()))
				}

				archiveMD5, err := svc.archiveFile(js, srcImg, tgtUnit.ID, newMF)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to archive masterfile %d %s: %s", newMF.ID, srcImg, err.Error()))
				} else {
					if archiveMD5 != newMF.MD5 {
						svc.logError(js, fmt.Sprintf("Archive MD5 does not match source MD5 for masterfile %d", newMF.ID))
					}
				}
			}
			if len(srcImages) != mfPageNum {
				svc.logError(js, fmt.Sprintf("Masterfile count mismatch for unit %d: %d images vs %d masterfiles", tgtUnit.ID, len(srcImages), mfPageNum))
			} else {
				svc.logInfo(js, fmt.Sprintf("Unit %d ingested; marking complete", tgtMD.ID))
				now := time.Now()
				tgtUnit.DateArchived = &now
				tgtUnit.UnitStatus = "done"
				err = svc.GDB.Model(&tgtUnit).Select("DateArchived", "UnitStatus").Updates(tgtUnit).Error
				if err != nil {
					svc.logError(js, fmt.Sprintf("unable to update status of completed unit %d: %s", tgtUnit.ID, err.Error()))
				}
			}
			cnt++
		}

		svc.logInfo(js, fmt.Sprintf("%d units ingested", cnt))
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("ingestBondImages started; check tracksys job %d status for details\n", js.ID))
	return nil
}

// generateBondMapping generate a CVS mapping for the bon project. Columns: original file, tracksys pid
// params:
//   - orderID: the target TS order ID. By default, all units will be exported
//   - box: (OPTIONAL) which box of images use
//   - folder: (OPTIONAL) which folder to export
//
// EXAMPLE: curl -X POST https://dpg-jobs.lib.virginia.edu/script -H "Content-Type: application/json" --data '{"computeID": "lf6f", "name": "generateBondMapping", "params": {"orderID": 12288, "box": "9"}}'
func (svc *ServiceContext) generateBondMapping(c *gin.Context, js *jobStatus, params map[string]any) error {
	svc.logInfo(js, "start script to export a bond to tracksys mapping csv")
	rawOrderID, found := params["orderID"].(float64)
	if found == false {
		return fmt.Errorf("missing required orderID param")
	}
	tgtOrder := order{ID: int64(rawOrderID)}
	err := svc.GDB.First(&tgtOrder).Error
	if err != nil {
		return fmt.Errorf("order %d not found: %s", tgtOrder.ID, err.Error())
	}
	if strings.Contains(tgtOrder.OrderTitle, "Julian Bond Papers") == false {
		return fmt.Errorf("order %d is not for Julian Bond Papers; title [%s]", tgtOrder.ID, tgtOrder.OrderTitle)
	}

	tgtFolder := ""
	tgtBox, found := params["box"].(string)
	if found {
		svc.logInfo(js, fmt.Sprintf("export box %s", tgtBox))
		tgtFolder, found = params["folder"].(string)
		if found {
			svc.logInfo(js, fmt.Sprintf("export folder %s", tgtFolder))
		}
	} else {
		svc.logInfo(js, "export mapping for entire order")
	}

	var tgtUnits []unit
	if tgtBox != "" {
		err := svc.GDB.Debug().
			Joins("inner join metadata m on m.id=units.metadata_id").Preload("MasterFiles").
			Where("order_id=? and call_number LIKE ?", tgtOrder.ID, fmt.Sprintf("%%Box %s", tgtBox)).
			Find(&tgtUnits).Error
		if err != nil {
			return fmt.Errorf("unable to get box units: %s", err.Error())
		}
	} else {
		err := svc.GDB.Where("order_id=?", tgtOrder.ID).Preload("MasterFiles").Find(&tgtUnits).Error
		if err != nil {
			return fmt.Errorf("unable to get units: %s", err.Error())
		}
	}

	c.Header("Content-Type", "text/csv")
	cw := csv.NewWriter(c.Writer)
	csvHead := []string{"original file", "tracksys pid"}
	cw.Write(csvHead)
	cnt := 0
	for _, tgtUnit := range tgtUnits {
		if len(tgtUnit.MasterFiles) == 0 {
			svc.logInfo(js, fmt.Sprintf("skipping unit %d that does not have master files", tgtUnit.ID))
			continue
		}
		imgDir := extractBondImageFolder(&tgtUnit)
		if imgDir == "" {
			svc.logInfo(js, fmt.Sprintf("skipping unit %d that does not have source image folder in special instructions", tgtUnit.ID))
			continue
		}
		if tgtFolder != "" {
			bits := strings.Split(imgDir, "-")
			folder := bits[len(bits)-1]
			folder = strings.Replace(folder, "f", "", 1)
			if folder != tgtFolder {
				continue
			}
		}
		svc.logInfo(js, fmt.Sprintf("export unit %d:[%s] from [%s]", tgtUnit.ID, tgtUnit.StaffNotes, imgDir))
		images := extractBondUnitImageList(&tgtUnit)
		for srcIdx, srcImg := range images {
			tgtMF := tgtUnit.MasterFiles[srcIdx]
			line := []string{strings.TrimSpace(srcImg), tgtMF.PID}
			cw.Write(line)
		}
		cnt++
	}
	svc.logInfo(js, fmt.Sprintf("%d unit mappings exported", cnt))
	svc.jobDone(js)
	cw.Flush()

	return nil
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

func extractBondImageFolder(tgtUnit *unit) string {
	ingestFrom := strings.Split(tgtUnit.SpecialInstructions, "\n")[0]
	bits := strings.Split(ingestFrom, ":")
	if len(bits) == 0 {
		return ""
	}
	ingestFrom = bits[1]
	ingestFrom = strings.TrimSpace(ingestFrom)
	return ingestFrom
}

func extractBondUnitImageList(tgtUnit *unit) []string {
	images := make([]string, 0)
	bits := strings.Split(tgtUnit.SpecialInstructions, "\n")
	if len(bits) == 0 {
		return images
	}
	imagesLine := bits[1]
	bits = strings.Split(imagesLine, ":")
	if len(bits) == 0 {
		return images
	}
	imagesStr := bits[1]
	for _, img := range strings.Split(imagesStr, ",") {
		img = strings.TrimSpace(img)
		images = append(images, img)
	}
	return sortImages(images)
}

func sortImages(images []string) []string {
	// y := strings.Split(images, ",")
	slices.SortFunc(images,
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
	return images
	// return strings.Join(y, ",")
}
