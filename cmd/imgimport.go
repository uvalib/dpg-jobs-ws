package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type tifMetadata struct {
	Title       string
	Description string
	ComponentID int64
	Box         string
	Folder      string
}

func (svc *ServiceContext) importGuestImages(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	type importReq struct {
		From   string `json:"from"`
		Target string `json:"target"`
	}
	var req importReq
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse import request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	overwrite := false
	overwriteStr := c.Query("overwrite")
	if overwriteStr == "1" || overwriteStr == "true" {
		overwrite = true
	}

	log.Printf("INFO: import %s from %s to unit %09d", req.Target, req.From, unitID)
	srcDir := path.Join(svc.ProcessingDir, "guest_dropoff", req.From, req.Target)
	if req.From == "archive" {
		srcDir = path.Join(svc.ArchiveDir, req.Target)
	} else if req.From == "download" {
		srcDir = req.Target
		req.Target = fmt.Sprintf("%09d", unitID)
	}
	if _, err := os.Stat(srcDir); os.IsNotExist(err) {
		log.Printf("ERROR: %s does not exist", srcDir)
		c.String(http.StatusBadRequest, fmt.Sprintf("%s does not exist", srcDir))
		return
	}

	// validate unit (and get data so archived date can be set)
	var tgtUnit unit
	err = svc.GDB.Find(&tgtUnit, unitID).Error
	if err != nil {
		log.Printf("ERROR: unable to load target unit: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	cnt := 0
	badSequenceNum := false
	err = filepath.Walk(srcDir, func(fullPath string, entry os.FileInfo, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".tif" {
			return nil
		}
		if strings.Index(entry.Name(), "._") == 0 {
			// some guest directories have macOS temp files that start with ._
			// the need to be skipped
			return nil
		}

		tifFile := tifInfo{path: fullPath, filename: entry.Name(), size: entry.Size()}
		log.Printf("INFO: import %s", tifFile.path)

		// be sure the filename is xxxx_sequence.tif.
		if req.From != "download" {
			test := strings.Split(strings.TrimSuffix(entry.Name(), ".tif"), "_")
			if len(test) == 1 {
				log.Printf("INFO: %s is missing sequence number, import and add staff note to unit", fullPath)
				badSequenceNum = true
			}
			seqStr := test[len(test)-1]
			seq, _ := strconv.Atoi(seqStr)
			if seq == 0 {
				log.Printf("INFO: %s has invalid sequence number %s, import and add staff note to unit", fullPath, seqStr)
				badSequenceNum = true
			}
		}

		newMF, err := svc.loadMasterFile(entry.Name())
		if err != nil {
			log.Printf("ERROR: unable to load masterfile %s: %s", entry.Name(), err.Error())
			return err
		}

		if newMF.ID == 0 {
			log.Printf("INFO: create guest masterfile %s", entry.Name())
			newMD5 := md5Checksum(tifFile.path)
			newMF = &masterFile{UnitID: unitID, Filename: tifFile.filename, Filesize: tifFile.size, MD5: newMD5}
			err = svc.GDB.Create(&newMF).Error
			if err != nil {
				log.Printf("ERROR: unable to create masterfile %s: %s", entry.Name(), err.Error())
				return err
			}
			newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
			svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
		} else {
			log.Printf("INFO: master file %s already exists", tifFile.filename)
			if newMF.PID == "" {
				newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
				svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
			}
		}

		if newMF.ImageTechMeta.ID == 0 || overwrite {
			if newMF.ImageTechMeta.ID != 0 {
				log.Printf("INFO: overwite existing image tech metadata")
				err = svc.GDB.Delete(&newMF.ImageTechMeta).Error
				if err != nil {
					log.Printf("WARNING: unable to delete existing tech metadata record %d: %s", newMF.ImageTechMeta.ID, err.Error())
				}
			}
			err = svc.createImageTechMetadata(newMF, tifFile.path)
			if err != nil {
				log.Printf("ERROR: unable to create image tech metadata: %s", err.Error())
			}
		}

		if newMF.ImageTechMeta.Width == 0 || newMF.ImageTechMeta.Height == 0 {
			log.Printf("ERROR: %s has invalid tech metdata and is likely corrupt; skipping further processing", newMF.PID)
			return nil
		}

		colorTest := strings.TrimSpace(newMF.ImageTechMeta.ColorSpace)
		if colorTest == "CMYK" {
			log.Printf("ERROR: %s has unsupported colorspace %s; skipping further processing", newMF.PID, colorTest)
			return nil
		}

		err = svc.publishToIIIF(nil, newMF, tifFile.path, overwrite)
		if err != nil {
			return fmt.Errorf("IIIF publish failed: %s", err.Error())
		}

		if req.From == "archive" {
			if newMF.DateArchived == nil {
				log.Printf("INFO: update date archived for %s", newMF.Filename)
				newMF.DateArchived = tgtUnit.DateArchived
				if newMF.DateArchived == nil {
					now := time.Now()
					newMF.DateArchived = &now
				}
				err = svc.GDB.Model(newMF).Select("DateArchived").Updates(*newMF).Error
				if err != nil {
					log.Printf("WARNING: unable to set date archived for master file %d:%s", newMF.ID, err.Error())
				}
			}
		} else if newMF.DateArchived == nil {
			archiveMD5, err := svc.archiveFineArtsFile(tifFile.path, req.Target, newMF)
			if err != nil {
				return fmt.Errorf("Archive failed: %s", err.Error())
			}
			if archiveMD5 != newMF.MD5 {
				log.Printf("WARNING: archived MD5 does not match source MD5 for %s", newMF.Filename)
			}
		}
		cnt++

		return nil
	})

	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	log.Printf("INFO: %d masterfiles processed", cnt)
	tgtUnit.UnitStatus = "done"
	tgtUnit.MasterFilesCount = uint(cnt)
	if badSequenceNum {
		tgtUnit.StaffNotes += fmt.Sprintf("Archive: %s", req.Target)
	}
	svc.GDB.Model(&tgtUnit).Select("UnitStatus", "MasterFilesCount", "StaffNotes").Updates(tgtUnit)

	c.String(http.StatusOK, fmt.Sprintf("%d masterfiles processed", cnt))
}

func (svc *ServiceContext) publishMasterFileToIIIF(c *gin.Context) {
	mfPID := c.Query("pid")
	if mfPID == "" {
		log.Printf("ERROR: publish masterfile to iiif request is missing required pid")
		c.String(http.StatusBadRequest, "missing required PID parameter")
		return
	}

	overwrite := false
	overwriteStr := c.Query("overwrite")
	if overwriteStr == "1" || overwriteStr == "true" {
		overwrite = true
	}

	log.Printf("INFO: publish masterfile %s to iiif, overwrite=%t", mfPID, overwrite)
	var tgtMF masterFile
	err := svc.GDB.Preload("ImageTechMeta").Preload("Component").Preload("Locations").Preload("Unit").
		Where("pid=?", mfPID).Limit(1).Find(&tgtMF).Error
	if err != nil {
		log.Printf("INFO: unable to load masterfile %s: %s", mfPID, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	srcPath := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", tgtMF.UnitID))
	if strings.Contains(tgtMF.Unit.StaffNotes, "Archive: ") {
		tgtDir := strings.Split(tgtMF.Unit.StaffNotes, "Archive: ")[1]
		srcPath = path.Join(svc.ArchiveDir, tgtDir, tgtMF.Filename)
	}

	if overwrite {
		if tgtMF.ImageTechMeta.ID != 0 {
			log.Printf("INFO: overwite existing image tech metadata")
			err = svc.GDB.Delete(&tgtMF.ImageTechMeta).Error
			if err != nil {
				log.Printf("WARNING: unable to delete existing tech metadata record %d: %s", tgtMF.ImageTechMeta.ID, err.Error())
			}
		}
		err = svc.createImageTechMetadata(&tgtMF, srcPath)
		if err != nil {
			log.Printf("ERROR: unable to create image tech metadata: %s", err.Error())
		}
	}

	if tgtMF.ImageTechMeta.Width == 0 || tgtMF.ImageTechMeta.Height == 0 {
		log.Printf("ERROR: %s has invalid tech metdata and is likely corrupt; skipping further processing", tgtMF.PID)
		c.String(http.StatusBadRequest, "invalid tech metadata; width and height are zero")
		return
	}

	colorTest := strings.TrimSpace(tgtMF.ImageTechMeta.ColorSpace)
	if colorTest == "CMYK" {
		log.Printf("ERROR: %s has unsupported colorspace %s; skipping further processing", tgtMF.PID, colorTest)
		c.String(http.StatusBadRequest, fmt.Sprintf("unsupported colorspace %s", colorTest))
		return
	}

	err = svc.publishToIIIF(nil, &tgtMF, srcPath, overwrite)
	if err != nil {
		log.Printf("ERROR: publish %s to iiif failed: %s", mfPID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "published")
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

	// iterate through all of the .tif files in the unit directory
	mfCount := 0
	tifFiles, err := svc.getTifFiles(js, srcDir, tgtUnit.ID)
	if err != nil {
		return err
	}
	for _, fi := range tifFiles {
		svc.logInfo(js, fmt.Sprintf("Import %s", fi.path))
		mfCount++

		// grab metadata from exif headers
		tifMD, err := extractTifMetadata(fi.path)
		if err != nil {
			return err
		}

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
			newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
			svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
			svc.logInfo(js, fmt.Sprintf("Master file %s created", fi.filename))
		} else {
			svc.logInfo(js, fmt.Sprintf("Master file %s already exists", fi.filename))
			if newMF.PID == "" {
				newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
				svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
			}
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

		err = svc.publishToIIIF(js, newMF, fi.path, false)
		if err != nil {
			return fmt.Errorf("IIIF publish failed: %s", err.Error())
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
			bytes, err := ioutil.ReadFile(textFilePath)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to read txt file %s: %s", textFilePath, err.Error()))
			} else {
				newMF.TranscriptionText = string(bytes)
				svc.GDB.Model(&newMF).Select("TranscriptionText").Updates(newMF)
			}
		}

		// if box/folder set, add location info to the master file
		if tifMD.Box != "" && tifMD.Folder != "" {
			unitProj, _ := svc.getUnitProject(tgtUnit.ID)
			if newMF.location() == nil && unitProj != nil {
				svc.logInfo(js, fmt.Sprintf("Location defined for this masterfile: %s/%s", tifMD.Box, tifMD.Folder))
				loc, err := svc.findOrCreateLocation(js, *tgtUnit.MetadataID, *unitProj.ContainerTypeID, srcDir, tifMD.Box, tifMD.Folder)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to create location for %s: %s", newMF.Filename, err.Error()))
				} else {
					err = svc.GDB.Exec("INSERT into master_file_locations (master_file_id, location_id) values (?,?)", newMF.ID, loc.ID).Error
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to add location %d [%s/%s] to %s: %s", loc.ID, tifMD.Box, tifMD.Folder, newMF.Filename, err.Error()))
					}
				}
			}
		}
	}

	svc.logInfo(js, fmt.Sprintf("%d master files ingested", mfCount))
	now := time.Now()
	tgtUnit.UnitExtentActual = uint(mfCount)
	tgtUnit.MasterFilesCount = uint(mfCount)
	tgtUnit.DateArchived = &now
	svc.GDB.Model(tgtUnit).Select("UnitExtentActual", "MasterFilesCount", "DateArchived").Updates(*tgtUnit)
	svc.checkOrderArchiveComplete(js, tgtUnit.OrderID)

	svc.logInfo(js, "Images for Unit successfully imported.")
	return nil
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

func extractTifMetadata(tifPath string) (*tifMetadata, error) {
	cmdArray := []string{"-json", "-iptc:OwnerID", "-iptc:headline", "-iptc:caption-abstract", "-iptc:ContentLocationName", "-iptc:Keywords", tifPath}
	stdout, err := exec.Command("exiftool", cmdArray...).Output()
	if err != nil {
		return nil, err
	}

	type exifData struct {
		Title       interface{} `json:"Headline"`
		Description interface{} `json:"Caption-Abstract"`
		OwnerID     interface{} `json:"OwnerID"`
		Box         interface{} `json:"Keywords"`
		Folder      interface{} `json:"ContentLocationName"`
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
	if parsedExif[0].Box != nil {
		out.Box = fmt.Sprintf("%v", parsedExif[0].Box)
	}
	if parsedExif[0].Folder != nil {
		out.Folder = fmt.Sprintf("%v", parsedExif[0].Folder)
	}
	return &out, nil
}
func (svc *ServiceContext) findOrCreateLocation(js *jobStatus, mdID int64, ctID int64, baseDir, box, folder string) (*location, error) {
	svc.logInfo(js, fmt.Sprintf("Find or create location based on %s/%s", box, folder))
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
			bytes, err := ioutil.ReadFile(notesFile)
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
