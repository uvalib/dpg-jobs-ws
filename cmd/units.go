package main

import (
	"fmt"
	"log"
	"net/http"
	"path"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) attachFile(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("AttachFile", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create AttachFile job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, "Receive attachment file and name...")
	name := c.PostForm("name")
	description := c.PostForm("description")
	formFile, err := c.FormFile("file")
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to get attachment: %s", err.Error()))
		c.String(http.StatusBadRequest, fmt.Sprintf("unable to get file: %s", err.Error()))
		return
	}
	if name == "" {
		svc.logFatal(js, "Missing attachment name")
		c.String(http.StatusBadRequest, "missing attachment name")
		return
	}

	destDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID), "attachments")
	ensureDirExists(destDir, 0775)
	destFile := path.Join(destDir, name)

	svc.logInfo(js, fmt.Sprintf("Saving attachment to %s", destFile))
	err = c.SaveUploadedFile(formFile, destFile)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to save %s: %s", name, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, "Creating attachment record")
	md5 := md5Checksum(destFile)
	now := time.Now()
	att := attachment{UnitID: unitID, MD5: md5, Filename: name, Description: description, CreatedAt: now, UpdatedAt: now}
	err = svc.GDB.Create(&att).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to create attachment record: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("File %s added as attachment", name))
	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) cloneMasterFiles(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("CloneMasterFiles", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create CloneMasterFiles job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	type cloneRequest struct {
		UnitID   int64 `json:"unitID"`
		AllFiles bool  `json:"all"`
		Files    []struct {
			ID    int64  `json:"id"`
			Title string `json:"title"`
		} `json:"masterfiles"`
	}
	var req []cloneRequest
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse clone request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Loading destination unit %d", unitID))
	var destUnit unit
	err = svc.GDB.First(&destUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load destination unit %d: %s", unitID, err.Error()))
		return
	}

	go func() {
		pageNum := 1
		failed := false
		for _, cr := range req {
			svc.logInfo(js, fmt.Sprintf("Loading clone source unit %d", cr.UnitID))
			var srcUnit unit
			err = svc.GDB.Preload("MasterFiles").
				Preload("MasterFiles.ImageTechMeta").
				Preload("MasterFiles.Locations").
				First(&srcUnit, cr.UnitID).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", cr.UnitID, err.Error()))
				failed = true
				return
			}

			if cr.AllFiles {
				cloneCnt, err := svc.cloneAllMasterFiles(js, &srcUnit, &destUnit, pageNum)
				if err != nil {
					svc.logFatal(js, err.Error())
					break
				}
				pageNum += cloneCnt
			} else {
				for _, mf := range cr.Files {
					mf := findMasterfile(&srcUnit, mf.ID)
					if mf == nil {
						svc.logError(js, fmt.Sprintf("Unable to find masterfile %d in source unit %d. Skipping.", mf.ID, srcUnit.ID))
					} else {
						err = svc.cloneMasterFile(js, &srcUnit, mf, &destUnit, mf.Title, pageNum)
						if err != nil {
							svc.logFatal(js, err.Error())
							failed = true
							break
						}
					}
					pageNum++
				}
			}
			if failed {
				break
			}
		}
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) cloneAllMasterFiles(js *jobStatus, srcUnit *unit, destUnit *unit, startPageNum int) (int, error) {
	svc.logInfo(js, fmt.Sprintf("Cloning all master files from unit %d. Statring page number: %d", srcUnit.ID, startPageNum))
	pageNum := startPageNum
	clonedCount := 0
	for _, srcMF := range srcUnit.MasterFiles {
		err := svc.cloneMasterFile(js, srcUnit, &srcMF, destUnit, srcMF.Title, pageNum)
		if err != nil {
			return 0, err
		}
		pageNum++
		clonedCount++
	}
	svc.logInfo(js, fmt.Sprintf("All master files from unit %d have been cloned. %d masterfiles added", srcUnit.ID, clonedCount))
	return clonedCount, nil
}

func (svc *ServiceContext) cloneMasterFile(js *jobStatus, srcUnit *unit, srcMF *masterFile, destUnit *unit, newTitle string, pageNum int) error {
	// Create new MF records and pull tiffs from archive into in_proc for the new unit
	// so they will be ready to be used to generate deliverables with CreatePatronDeliverables job
	destUnitDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", destUnit.ID))
	ensureDirExists(destUnitDir, 0775)

	srcArchiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", srcUnit.ID), srcMF.Filename)
	if pathExists(srcArchiveFile) == false {
		return fmt.Errorf("unable to find archived tif %s for master file with ID %d", srcArchiveFile, srcMF.ID)
	}
	svc.ensureMD5(js, srcMF, srcArchiveFile)

	newFN := fmt.Sprintf("%s_%04d.tif", fmt.Sprintf("%09d", destUnit.ID), pageNum)
	destFile := path.Join(destUnitDir, newFN)
	svc.logInfo(js, fmt.Sprintf("Cloning master file from %s to %s", srcArchiveFile, destFile))
	newMD5, err := copyFile(srcArchiveFile, destFile, 0664)
	if err != nil {
		return err
	}

	if newMD5 != srcMF.MD5 {
		svc.logError(js, fmt.Sprintf("WARNING: Checksum mismatch for clone of source master file %d", srcMF.ID))
	}

	now := time.Now()
	newMF := masterFile{
		UnitID:       destUnit.ID,
		Filename:     newFN,
		Filesize:     srcMF.Filesize,
		ComponentID:  srcMF.ComponentID,
		Title:        newTitle,
		Description:  srcMF.Description,
		MD5:          newMD5,
		MetadataID:   srcMF.MetadataID,
		OriginalMfID: &srcMF.ID,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	err = svc.GDB.Create(&newMF).Error
	if err != nil {
		return fmt.Errorf("Unable to create %s: %s", newFN, err.Error())
	}
	newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
	svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
	if srcMF.location() != nil {
		svc.GDB.Exec("INSERT into master_file_locations (master_file_id, location_id) values (?,?)", newMF.ID, srcMF.location().ID)
	}

	tm := srcMF.ImageTechMeta
	newTM := imageTechMeta{
		MasterFileID: newMF.ID,
		ImageFormat:  tm.ImageFormat, Width: tm.Width, Height: tm.Height,
		Resolution: tm.Resolution, ColorSpace: tm.ColorSpace, Depth: tm.Depth,
		Compression: tm.Compression, ColorProfile: tm.ColorProfile,
		Equipment: tm.Equipment, Software: tm.Software, Model: tm.Model,
		ExifVersion: tm.ExifVersion, CaptureDate: tm.CaptureDate, ISO: tm.ISO,
		ExposureBias: tm.ExposureBias, ExposureTime: tm.ExposureTime,
		Aperture: tm.Aperture, FocalLength: tm.FocalLength, CreatedAt: now, UpdatedAt: now,
	}
	err = svc.GDB.Create(&newTM).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to create tech metadata for masterfile %d", newMF.ID))
	}
	svc.logInfo(js, fmt.Sprintf("Master file cloned to %s", newMF.PID))
	return nil
}

func findMasterfile(tgtUnit *unit, mfID int64) *masterFile {
	var match *masterFile
	for _, mf := range tgtUnit.MasterFiles {
		if mf.ID == mfID {
			match = &mf
			break
		}
	}
	return match
}
