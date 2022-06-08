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

	log.Printf("INFO: import %s from %s to unit %09d", req.Target, req.From, unitID)
	srcDir := path.Join(svc.ProcessingDir, "guest_dropoff", req.From, req.Target)
	if req.From == "archive" {
		srcDir = path.Join(svc.ArchiveDir, req.Target)
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
	err = filepath.Walk(srcDir, func(fullPath string, entry os.FileInfo, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".tif" {
			return nil
		}

		tifFile := tifInfo{path: fullPath, filename: entry.Name(), size: entry.Size()}
		log.Printf("INFO: import %s", tifFile.path)
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

		if newMF.ImageTechMeta.ID == 0 {
			err = svc.createImageTechMetadata(newMF, tifFile.path)
			if err != nil {
				log.Printf("ERROR: unable to create image tech metadata: %s", err.Error())
			}
		}

		err = svc.publishToIIIF(nil, newMF, tifFile.path, false)
		if err != nil {
			return fmt.Errorf("IIIF publish failed: %s", err.Error())
		}

		if req.From == "archive" {
			if newMF.DateArchived == nil {
				log.Printf("INFO: update date archived for %s", newMF.Filename)
				newMF.DateArchived = tgtUnit.DateArchived
				svc.GDB.Model(newMF).Select("DateArchived").Updates(*newMF)
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
		log.Printf("ERROR: unable to get tif files from %s: %s", srcDir, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	log.Printf("INFO: %d masterfiles processed", cnt)
	tgtUnit.UnitStatus = "done"
	tgtUnit.MasterFilesCount = uint(cnt)
	svc.GDB.Model(&tgtUnit).Select("UnitStatus", "MasterFilesCount").Updates(tgtUnit)

	c.String(http.StatusOK, fmt.Sprintf("%d masterfiles processed", cnt))
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

		// See if this masterfile has already been created...
		newMF, err := svc.loadMasterFile(fi.filename)
		if err != nil {
			return err
		}
		if newMF.ID == 0 {
			svc.logInfo(js, fmt.Sprintf("Create new master file %s", fi.filename))
			newMD5 := md5Checksum(fi.path)
			tifMD, err := extractTifMetadata(fi.path)
			if err != nil {
				return err
			}
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

		// fi.path is the full path to the image Strip out the srcDir and filename.
		// The remaining bit will be the subdirectories or nothing.
		// use this info to know if there is box/folder info encoded in the filename
		// subdir structure: [box|oversize|tray].{box_name}/{folder_name} EXAMPLE: Created location metadata for [140/3]
		subDirs := filepath.Dir(fi.path) // strip filename
		if subDirs == srcDir {
			// there are no subdirs, so there is no box/folder info. continue.
			continue
		}
		subDirs = subDirs[len(srcDir)+1:]
		unitProj, _ := svc.getUnitProject(tgtUnit.ID)
		if newMF.location() == nil && unitProj != nil {
			svc.logInfo(js, fmt.Sprintf("Sub directories exist for this masterfile: %s", subDirs))
			if unitProj.ContainerTypeID == nil {
				svc.logInfo(js, "Location data available, but container type not set. Defaulting to box")
				firstContainerID := int64(1) // default to first; box
				unitProj.ContainerTypeID = &firstContainerID
				svc.GDB.Model(unitProj).Select("ContainerTypeID").Updates(*unitProj)
			}
			loc, err := svc.findOrCreateLocation(js, *tgtUnit.MetadataID, *unitProj.ContainerTypeID, srcDir, subDirs)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to create location ofr %s: %s", newMF.Filename, err.Error()))
			} else {
				err = svc.GDB.Exec("INSERT into master_file_locations (master_file_id, location_id) values (?,?)", newMF.ID, loc.ID).Error
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to add location %d [%s] to %s: %s", loc.ID, subDirs, newMF.Filename, err.Error()))
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
	cmdArray := []string{"-json", "-iptc:OwnerID", "-iptc:headline", "-iptc:caption-abstract", tifPath}
	stdout, err := exec.Command("exiftool", cmdArray...).Output()
	if err != nil {
		return nil, err
	}

	type exifData struct {
		Title       interface{} `json:"Headline"`
		Description interface{} `json:"Caption-Abstract"`
		OwnerID     interface{} `json:"OwnerID"`
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
	return &out, nil
}
func (svc *ServiceContext) findOrCreateLocation(js *jobStatus, mdID int64, ctID int64, baseDir, subDir string) (*location, error) {
	svc.logInfo(js, fmt.Sprintf("Find or create location based on %s", subDir))
	bits := strings.Split(subDir, "/")
	var tgtLoc location
	err := svc.GDB.Where("metadata_id=?", mdID).Where("container_type_id=?", ctID).
		Where("container_id=?", bits[0]).Where("folder_id=?", bits[1]).
		First(&tgtLoc).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) == false {
			return nil, err
		}
		tgtLoc = location{MetadataID: mdID, ContainerTypeID: ctID,
			ContainerID: bits[0], FolderID: bits[1]}
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
		svc.logInfo(js, fmt.Sprintf("Created location metadata for [%s]", subDir))
	} else {
		svc.logInfo(js, fmt.Sprintf("Found existing location metadata for [%s]", subDir))
	}
	return &tgtLoc, nil
}
