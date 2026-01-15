package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type tifMetadata struct {
	Title       string
	Description string
	ComponentID int64
	Location    string
}

const maxJP2Batches = 5

type jp2Source struct {
	MasterFile *masterFile
	Path       string
}

// Import a directory full of guest images from the archive or guest dropoff into a new unit owned by the target ID
// curl -X POST https://dpg-jobs.lib.virginia.edu/orders/11864/import -H "Content-Type: application/json" --data '{"from": "from_fineArts", "metadataID": 105320, "target": "20090902ARCH"}'
// curl -X POST https://dpg-jobs.lib.virginia.edu/orders/11864/import -H "Content-Type: application/json" --data '{"from": "archive", "metadataID": 105320, "target": "20090902ARCH"}'
func (svc *ServiceContext) importOrderImages(c *gin.Context) {
	orderID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	var tgtOrder order
	err := svc.GDB.First(&tgtOrder, orderID).Error
	if err != nil {
		log.Printf("ERROR: unable to get order %d: %s", orderID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	type orderImportReq struct {
		From       string `json:"from"`
		Target     string `json:"target"`
		MetadataID int64  `json:"metadataID"`
	}
	type orderImportResp struct {
		JobID  int64 `json:"job"`
		UnitID int64 `json:"unit"`
	}

	var req orderImportReq
	err = c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse import order item request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	srcDir := ""
	switch req.From {
	case "archive":
		srcDir = path.Join(svc.ArchiveDir, req.Target)
	case "from_fineArts":
		srcDir = path.Join(svc.ProcessingDir, "guest_dropoff", req.From, req.Target)
	default:
		log.Printf("INFO: invalid from param in import request: %s", req.From)
		c.String(http.StatusBadRequest, fmt.Sprintf("from %s is not valid", req.From))
		return
	}

	var mdRec metadata
	err = svc.GDB.First(&mdRec, req.MetadataID).Error
	if err != nil {
		log.Printf("ERROR: unable to get metadata record %d: %s", req.MetadataID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	js, err := svc.createJobStatus("ImportOrderImages", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create ImportOrderImages job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("import images from %s to a new unit in order %d with metadata %s", srcDir, orderID, mdRec.PID))
	var digitalCollectionBuildingID int64
	digitalCollectionBuildingID = 110
	staffNotes := fmt.Sprintf("Archive: %s", req.Target)
	var tgtUnit unit
	err = svc.GDB.Where("staff_notes=?", staffNotes).Find(&tgtUnit).Limit(1).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("unable to load unit with staff notes [%s]: %s", staffNotes, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if tgtUnit.ID == 0 {
		svc.logInfo(js, fmt.Sprintf("Create new unit with staff_notes [%s]", staffNotes))
		tgtUnit = unit{OrderID: tgtOrder.ID, MetadataID: &mdRec.ID, UnitStatus: "approved", IntendedUseID: &digitalCollectionBuildingID,
			CompleteScan: true, StaffNotes: staffNotes}
		err = svc.GDB.Create(&tgtUnit).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("unable to create unit for %s: %s", req.Target, err.Error()))
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		if tgtUnit.UnitStatus == "done" {
			svc.logInfo(js, fmt.Sprintf("%s has already been imoorted to unit %d", req.Target, tgtUnit.ID))
			svc.jobDone(js)

			out := orderImportResp{
				JobID:  0,
				UnitID: tgtUnit.ID,
			}
			c.JSON(http.StatusOK, out)
			return
		}

		if tgtUnit.UnitStatus != "approved" {
			svc.logFatal(js, fmt.Sprintf("Existing unit %d for %s has an incompatible status %s", tgtUnit.ID, req.Target, tgtUnit.UnitStatus))
			c.String(http.StatusBadRequest, fmt.Sprintf("exiating unit %d has incompatible status %s", tgtUnit.ID, tgtUnit.UnitStatus))
			return
		}
		svc.logInfo(js, fmt.Sprintf("Unit with staff_notes [%s] already exists; using it", staffNotes))
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				svc.logFatal(js, fmt.Sprintf("fatal error while ingesting images: %v", r))
				log.Printf("ERROR: Panic recovered during guest image ingest: %v", r)
				debug.PrintStack()
			}
		}()

		cnt := 0
		err = filepath.Walk(srcDir, func(fullPath string, entry os.FileInfo, err error) error {
			if err != nil || entry.IsDir() {
				return nil
			}

			// Grab the file extension - including the dot
			ext := filepath.Ext(entry.Name())
			if strings.ToLower(ext) != ".tif" || strings.Index(entry.Name(), "._") == 0 {
				// skip non .tif files and macOS temp files
				return nil
			}

			tifFile := tifInfo{path: fullPath, filename: entry.Name(), size: entry.Size()}
			svc.logInfo(js, fmt.Sprintf("ingest %s", tifFile.path))

			newMF, err := svc.getOrCreateMasterFile(js, tifFile, &tgtUnit, false)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Create masterfile failed: %s", err.Error()))
				return nil
			}

			err = svc.publishToIIIF(nil, newMF, tifFile.path, false)
			if err != nil {
				svc.logError(js, fmt.Sprintf("IIIF publish failed: %s", err.Error()))
				return nil
			}

			if req.From == "from_fineArts" {
				if newMF.DateArchived == nil {
					archiveMD5, err := svc.archiveFineArtsFile(tifFile.path, req.Target, newMF)
					if err != nil {
						svc.logError(js, fmt.Sprintf("archive failed: %s", err.Error()))
						return nil
					}
					if archiveMD5 != newMF.MD5 {
						svc.logError(js, fmt.Sprintf("archived MD5 does not match source MD5 for %s", newMF.Filename))
					}
				}
			}

			cnt++
			return nil
		})

		if err != nil {
			svc.logFatal(js, err.Error())
		} else {
			svc.logInfo(js, fmt.Sprintf("%d masterfiles processed", cnt))
			tgtUnit.UnitStatus = "done"
			if req.From == "from_fineArts" && tgtUnit.DateArchived == nil {
				now := time.Now()
				tgtUnit.DateArchived = &now
				svc.GDB.Model(&tgtUnit).Select("UnitStatus", "DateArchived").Updates(tgtUnit)
			} else {
				svc.GDB.Model(&tgtUnit).Select("UnitStatus").Updates(tgtUnit)
			}
		}

		svc.jobDone(js)
	}()

	out := orderImportResp{
		JobID:  js.ID,
		UnitID: tgtUnit.ID,
	}
	c.JSON(http.StatusOK, out)
}

func (svc *ServiceContext) importImages(js *jobStatus, tgtUnit *unit, srcDir string) error {
	svc.logInfo(js, fmt.Sprintf("Import images from %s", srcDir))
	if tgtUnit.ThrowAway {
		svc.logInfo(js, "This unit is a throw away and will not be archived.")
	}

	assembleDir := ""
	callNumber := ""
	location := ""
	if tgtUnit.Reorder == false && tgtUnit.IntendedUse.ID != 110 {
		svc.logInfo(js, "This unit requires patron deliverables. Setting up working directories.")
		assembleDir = path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", tgtUnit.ID))
		err := ensureDirExists(assembleDir, 0755)
		if err != nil {
			return err
		}
		svc.logInfo(js, fmt.Sprintf("Deliverables will be generated in %s", assembleDir))
		if tgtUnit.Metadata.Type == "SirsiMetadata" {
			callNumber = tgtUnit.Metadata.CallNumber
			location = svc.getMarcLocation(tgtUnit.Metadata)
		}
	}

	// FIXME THIS IS NOT NEEDED. -iptc:sub-location has somthing like [container type name] [container id], Folder [folder id]
	// Just use that.. nop need for this....

	// grab any project info for this unit
	unitProj, err := svc.getUnitProject(tgtUnit.ID)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to load project details: %s", err.Error()))
	} else {
		svc.logInfo(js, fmt.Sprintf("Workflow for unit is %s", unitProj.Workflow.Name))
	}

	// iterate through all of the .tif files in the unit directory
	tifFiles, err := svc.getTifFiles(js, srcDir, tgtUnit.ID)
	if err != nil {
		return err
	}

	// set up parallel jp2 batch processing
	filePerBatch := int(math.Ceil(float64(len(tifFiles)) / float64(maxJP2Batches)))
	batches := uint(math.Ceil(float64(len(tifFiles)) / float64(filePerBatch)))
	jp2Batch := make([]jp2Source, 0)
	var jp2WG sync.WaitGroup
	svc.logInfo(js, fmt.Sprintf("IIIF processing: %d masterfiles with a max of %d batches; %d files per batch, %d batches", len(tifFiles), maxJP2Batches, filePerBatch, batches))
	startTime := time.Now()

	for _, fi := range tifFiles {
		svc.logInfo(js, fmt.Sprintf("Import %s", fi.path))

		// grab metadata from exif headers
		tifMD, err := extractTifMetadata(fi.path)
		if err != nil {
			return err
		}
		svc.logInfo(js, fmt.Sprintf("Extracted the following TIF metadata: %+v", *tifMD))

		// See if this masterfile has already been created...
		newMF, err := svc.loadMasterFile(fi.filename)
		if err != nil {
			return err
		}
		if newMF.ID == 0 {
			svc.logInfo(js, fmt.Sprintf("Create new master file %s", fi.filename))
			newMD5 := md5Checksum(fi.path)
			newMF = &masterFile{UnitID: tgtUnit.ID, MetadataID: tgtUnit.MetadataID, Filename: fi.filename,
				Filesize: fi.size, MD5: newMD5, Title: tifMD.Title, Description: tifMD.Description}
			if tgtUnit.Metadata.IsManuscript && tifMD.ComponentID != 0 {
				var cnt int64
				svc.GDB.Table("components").Where("id=?", tifMD.ComponentID).Count(&cnt)
				if cnt == 0 {
					svc.logError(js, fmt.Sprintf("Could not find component %d to link to master file %s", tifMD.ComponentID, fi.filename))
				} else {
					svc.logInfo(js, fmt.Sprintf("Link to master file %s to component %d", fi.filename, tifMD.ComponentID))
					newMF.ComponentID = &tifMD.ComponentID
				}
			}

			err = svc.GDB.Create(&newMF).Error
			if err != nil {
				return err
			}
			svc.logInfo(js, fmt.Sprintf("Master file %s created", fi.filename))
		} else {
			svc.logInfo(js, fmt.Sprintf("Master file %s already exists", fi.filename))
		}

		if newMF.ImageTechMeta.ID == 0 {
			svc.logInfo(js, "Create image tech metadata")
			err = svc.createImageTechMetadata(newMF, fi.path)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to create image tech metadata: %s", err.Error()))
			}
		} else {
			svc.logInfo(js, "Image tech metadata already exists")
		}

		if tgtUnit.Reorder {
			continue
		}

		// FIXME get container type for project
		// if set, add location info to the master file
		if tifMD.Location != "" {
			svc.logInfo(js, fmt.Sprintf("Location metadata found: %s", tifMD.Location))
			if newMF.location() == nil {
				svc.logInfo(js, fmt.Sprintf("Create location %s for masterfile %s", tifMD.Location, newMF.Filename))
				// FIXME NO NEED FOR *unitProj.ContainerTypeID all info is in  tifMD.Location
				loc, err := svc.findOrCreateLocation(js, *tgtUnit.MetadataID, *unitProj.ContainerTypeID, srcDir, tifMD.Location)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to create location for %s: %s", newMF.Filename, err.Error()))
				} else {
					err = svc.GDB.Exec("INSERT into master_file_locations (master_file_id, location_id) values (?,?)", newMF.ID, loc.ID).Error
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to add location %d [%s] to %s: %s", loc.ID, tifMD.Location, newMF.Filename, err.Error()))
					} else {
						svc.logInfo(js, fmt.Sprintf("Master file location created for %s", fi.filename))
					}
				}
			}
		}

		jp2Batch = append(jp2Batch, jp2Source{MasterFile: newMF, Path: fi.path})
		if len(jp2Batch) == filePerBatch {
			svc.logInfo(js, fmt.Sprintf("Start JP2 processing for a batch of %d master files", len(jp2Batch)))
			jp2WG.Add(1)

			batchCopy := make([]jp2Source, len(jp2Batch))
			cnt := copy(batchCopy, jp2Batch)
			if cnt == 0 {
				return fmt.Errorf("unable to start iiif batch processing; source copy failed")
			}

			go func() {
				defer jp2WG.Done()
				svc.batchIIIFPublish(js, batchCopy, false)
			}()

			jp2Batch = make([]jp2Source, 0)
		}

		if tgtUnit.ThrowAway == false && tgtUnit.DateArchived == nil {
			archiveMD5, err := svc.archiveFile(js, fi.path, tgtUnit.ID, newMF)
			if err != nil {
				return fmt.Errorf("Archive failed: %s", err.Error())
			}
			if archiveMD5 != newMF.MD5 {
				svc.logError(js, "Archive MD5 does not match source MD5")
			}
		}

		if tgtUnit.IntendedUse.ID != 110 && tgtUnit.IntendedUse.DeliverableFormat != "pdf" {
			err = svc.createPatronDeliverable(js, tgtUnit, newMF, fi.path, assembleDir, callNumber, location)
			if err != nil {
				return fmt.Errorf("Create patron deliverable failed: %s", err.Error())
			}
		}

		// check for transcription text file
		baseFN := strings.TrimSuffix(newMF.Filename, filepath.Ext(newMF.Filename))
		textFilePath := path.Join(srcDir, fmt.Sprintf("%s.txt", baseFN))
		if pathExists(textFilePath) {
			svc.logInfo(js, fmt.Sprintf("Add transcription text for %s", fi.filename))
			bytes, err := os.ReadFile(textFilePath)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to read txt file %s: %s", textFilePath, err.Error()))
			} else {
				newMF.TranscriptionText = string(bytes)
				svc.GDB.Model(&newMF).Select("TranscriptionText").Updates(newMF)
			}
		}
	}

	if len(jp2Batch) > 0 {
		svc.logInfo(js, fmt.Sprintf("Start FINAL JP2 processing for a batch of %d master files", len(jp2Batch)))
		jp2WG.Add(1)
		go func() {
			defer jp2WG.Done()
			svc.batchIIIFPublish(js, jp2Batch, false)
		}()
	}

	svc.logInfo(js, fmt.Sprintf("%d master files processed; await completion of IIIF publish", len(tifFiles)))
	jp2WG.Wait()

	elapsed := time.Since(startTime)
	svc.logInfo(js, fmt.Sprintf("%d master files ingested; total time %.2f seconds", len(tifFiles), elapsed.Seconds()))
	now := time.Now()
	tgtUnit.DateArchived = &now
	svc.GDB.Model(tgtUnit).Select("DateArchived").Updates(*tgtUnit)
	svc.checkOrderArchiveComplete(js, tgtUnit.OrderID)

	svc.logInfo(js, "Images for Unit successfully imported.")
	return nil
}

func (svc *ServiceContext) getOrCreateMasterFile(js *jobStatus, srcTifInfo tifInfo, tgtUnit *unit, overwrite bool) (*masterFile, error) {
	newMF, err := svc.loadMasterFile(srcTifInfo.filename)
	if err != nil {
		return nil, fmt.Errorf("unable to load existing masterfile %s: %s", srcTifInfo.filename, err.Error())
	}

	if newMF.ID == 0 {
		svc.logInfo(js, fmt.Sprintf("create new masterfile for %s", srcTifInfo.filename))
		newMD5 := md5Checksum(srcTifInfo.path)
		newMF = &masterFile{UnitID: tgtUnit.ID, Filename: srcTifInfo.filename, Filesize: srcTifInfo.size, MD5: newMD5, MetadataID: tgtUnit.MetadataID}
		err = svc.GDB.Create(&newMF).Error
		if err != nil {
			return nil, fmt.Errorf("unable to create masterfile %s: %s", srcTifInfo.filename, err.Error())
		}
	} else {
		svc.logInfo(js, fmt.Sprintf("master file %s already exists", srcTifInfo.filename))
		if newMF.PID == "" {
			newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
			svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
		}
	}

	if newMF.ImageTechMeta.ID == 0 || overwrite {
		if newMF.ImageTechMeta.ID != 0 {
			svc.logInfo(js, "overwite existing image tech metadata")
			err = svc.GDB.Delete(&newMF.ImageTechMeta).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("unable to delete existing tech metadata record %d: %s", newMF.ImageTechMeta.ID, err.Error()))
			}
		}
		err = svc.createImageTechMetadata(newMF, srcTifInfo.path)
		if err != nil {
			svc.logError(js, fmt.Sprintf("unable to create image tech metadata: %s", err.Error()))
		}
	}

	if newMF.ImageTechMeta.Width == 0 || newMF.ImageTechMeta.Height == 0 {
		return nil, fmt.Errorf("%s has invalid tech metdata and is likely corrupt; skipping further processing", newMF.PID)
	}

	colorTest := strings.TrimSpace(newMF.ImageTechMeta.ColorSpace)
	if colorTest == "CMYK" {
		return nil, fmt.Errorf("%s has unsupported colorspace %s; skipping further processing", newMF.PID, colorTest)
	}
	return newMF, nil
}

func (svc *ServiceContext) loadMasterFile(filename string) (*masterFile, error) {
	var newMF masterFile
	err := svc.GDB.Preload("ImageTechMeta").Preload("Component").Preload("Locations").
		Where("filename=?", filename).Limit(1).Find(&newMF).Error
	if err != nil {
		return nil, err
	}
	return &newMF, nil
}

func (svc *ServiceContext) batchIIIFPublish(js *jobStatus, items []jp2Source, overwrite bool) {
	svc.logInfo(js, fmt.Sprintf("Process batch of %d master files", len(items)))
	startTime := time.Now()
	for _, item := range items {
		err := svc.publishToIIIF(js, item.MasterFile, item.Path, overwrite)
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to publish master file %s to IIIF: %s", item.MasterFile.PID, err.Error()))
		}
	}
	elapsed := time.Since(startTime)
	svc.logInfo(js, fmt.Sprintf("Finished IIIF processing for a batch of %d files; total time %.2f seconds", len(items), elapsed.Seconds()))
}

func extractTifMetadata(tifPath string) (*tifMetadata, error) {
	cmdArray := []string{"-json", "-iptc:OwnerID", "-iptc:headline", "-iptc:caption-abstract", "-iptc:sub-location", tifPath}
	stdout, err := exec.Command("exiftool", cmdArray...).Output()
	if err != nil {
		return nil, err
	}

	type exifData struct {
		Title       any `json:"Headline"`
		Description any `json:"Caption-Abstract"`
		OwnerID     any `json:"OwnerID"`
		Location    any `json:"sub-location"`
	}

	var parsedExif []exifData
	err = json.Unmarshal(stdout, &parsedExif)
	if err != nil {
		return nil, err
	}
	out := tifMetadata{}
	if parsedExif[0].OwnerID != nil {
		strID := fmt.Sprintf("%v", parsedExif[0].OwnerID)
		out.ComponentID, _ = strconv.ParseInt(strID, 10, 64)
	}
	if parsedExif[0].Title != nil {
		out.Title = fmt.Sprintf("%v", parsedExif[0].Title)
	} else {
		return nil, fmt.Errorf("missing required Headline in tif metadata for %s", tifPath)
	}
	if parsedExif[0].Description != nil {
		out.Description = fmt.Sprintf("%v", parsedExif[0].Description)
	}
	if parsedExif[0].Location != nil {
		out.Location = fmt.Sprintf("%v", parsedExif[0].Location)
	}
	return &out, nil
}

// FIXME ctID not needed. just look up based on the container type name in the first part of locationStr
func (svc *ServiceContext) findOrCreateLocation(js *jobStatus, mdID int64, ctID int64, baseDir, locationStr string) (*location, error) {
	svc.logInfo(js, fmt.Sprintf("Find or create location based on %s", locationStr))

	// parse containerID an folderID from location string
	//    format: [container type] [container id], Folder [folder id]
	bits := strings.Split(locationStr, ",")
	containerBits := strings.Split(bits[0], " ")
	box := strings.TrimSpace(containerBits[len(containerBits)-1])
	folder := ""
	if len(bits) > 1 {
		folderBits := strings.Split(bits[1], " ")
		folder = strings.TrimSpace(folderBits[len(folderBits)-1])
	}

	/* FULL PARSE EXAMPLE:
	locationStr := "Flat File Drawer 6, Folder 69"
	bits := strings.Split(locationStr, ",")
	containerBits := strings.Split(bits[0], " ")
	box := strings.TrimSpace(containerBits[len(containerBits)-1])
	containerType := strings.Join(containerBits[0:len(containerBits)-1], " ")
	folder := ""
	if len(bits) > 1 {
		folderBits := strings.Split(bits[1], " ")
		folder = strings.TrimSpace(folderBits[len(folderBits)-1])
	}
	fmt.Printf("%s %s folder %s", containerType, box, folder)
	*/

	var tgtLoc location
	err := svc.GDB.Where("metadata_id=?", mdID).Where("container_type_id=?", ctID).
		Where("container_id=?", box).Where("folder_id=?", folder).
		First(&tgtLoc).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) == false {
			return nil, err
		}
		tgtLoc = location{MetadataID: mdID, ContainerTypeID: ctID,
			ContainerID: box, FolderID: folder}
		notesFile := path.Join(baseDir, "notes.txt")
		if pathExists(notesFile) {
			bytes, err := os.ReadFile(notesFile)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to read location notes file: %s", err.Error()))
			} else {
				tgtLoc.Notes = string(bytes)
			}
		}
		err = svc.GDB.Create(&tgtLoc).Error
		if err != nil {
			return nil, err
		}
		svc.logInfo(js, fmt.Sprintf("Created location metadata for [%s/%s]", box, folder))
	} else {
		svc.logInfo(js, fmt.Sprintf("Found existing location metadata for [%s/%s]", box, folder))
	}
	return &tgtLoc, nil
}
