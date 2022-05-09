package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
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
	att := attachment{UnitID: unitID, MD5: md5, Filename: name, Description: description}
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
			err = svc.GDB.
				Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
					return db.Order("master_files.filename ASC")
				}).
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
		svc.logInfo(js, fmt.Sprintf("%d masterfiles cloned into unit. Flagging unit as cloned", (pageNum-1)))
		destUnit.Reorder = true
		destUnit.MasterFilesCount = uint(pageNum - 1)
		err = svc.GDB.Model(&destUnit).Select("Reorder", "MasterFilesCount").Updates(destUnit).Error
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) createPatronDeliverables(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("CreatePatronDeliverables", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create CreatePatronDeliverables job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	go func() {
		svc.logInfo(js, fmt.Sprintf("Loading target unit %d", unitID))
		var tgtUnit unit
		err = svc.GDB.
			Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
				return db.Order("master_files.filename ASC")
			}).Preload("MasterFiles.ImageTechMeta").
			Preload("IntendedUse").Preload("Metadata").First(&tgtUnit, unitID).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
			return
		}

		// this can be called as part of a re-order or after finalization. for re-orders, the images will already exist in unit_dir
		unitDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", unitID))
		assembleDir := path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", unitID))

		if svc.unitImagesAvailable(js, &tgtUnit, assembleDir) == false {
			if tgtUnit.Reorder {
				svc.logInfo(js, "Creating deliverables for a reorder")
				// in this case, each cloned masterfile will have a reference to the original.
				// use this to get to the original unit and recalculate directories
				svc.copyOriginalFiles(js, &tgtUnit, unitDir)
			} else {
				archiveDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID))
				svc.logInfo(js, fmt.Sprintf("Creating deliverables from the archive %s", archiveDir))
				copyAll(archiveDir, unitDir)
			}
		} else {
			svc.logInfo(js, fmt.Sprintf("All files needed to generate unit %d deliverables exist in %s", unitID, assembleDir))
		}

		if tgtUnit.IntendedUse.DeliverableFormat == "pdf" {
			err = svc.createPatronPDF(js, &tgtUnit)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to create patron PDF deliverable: %s", err.Error()))
				return
			}
		} else {
			svc.logInfo(js, "Unit requires the creation of zipped patron deliverables.")
			err = ensureDirExists(assembleDir, 0755)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to create %s: %s", assembleDir, err.Error()))
				return
			}
			for _, mf := range tgtUnit.MasterFiles {
				mfPath := path.Join(unitDir, mf.Filename)
				callNumber := ""
				location := ""
				if tgtUnit.Metadata.Type == "SirsiMetadata" {
					callNumber = tgtUnit.Metadata.CallNumber
					location = svc.getMarcLocation(tgtUnit.Metadata)
				}
				err = svc.createPatronDeliverable(js, &tgtUnit, &mf, mfPath, assembleDir, callNumber, location)
				if err != nil {
					svc.logFatal(js, fmt.Sprintf("Deliverable creation failed for %s: %s", mf.Filename, err.Error()))
					return
				}
			}

			err = svc.zipPatronDeliverables(js, &tgtUnit)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Zip creation failed: %s", err.Error()))
				return
			}

		}

		now := time.Now()
		tgtUnit.DatePatronDeliverablesReady = &now
		svc.GDB.Model(&tgtUnit).Select("DatePatronDeliverablesReady").Updates(tgtUnit)
		svc.logInfo(js, "Deliverables created. Date deliverables ready has been updated.")

		svc.logInfo(js, "Cleaning up working directories")
		os.RemoveAll(unitDir)
		os.RemoveAll(assembleDir)
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
		Aperture: tm.Aperture, FocalLength: tm.FocalLength,
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

func (svc *ServiceContext) copyOriginalFiles(js *jobStatus, tgtUnit *unit, unitDir string) error {
	svc.logInfo(js, fmt.Sprintf("Copy original unit masterfiles to %s", unitDir))
	err := ensureDirExists(unitDir, 0775)
	if err != nil {
		return err
	}

	for _, mf := range tgtUnit.MasterFiles {
		destFile := path.Join(unitDir, mf.Filename)
		if pathExists(destFile) {
			svc.logInfo(js, fmt.Sprintf("%s already exists at %s", mf.Filename, destFile))
			continue
		}

		// Cloned files can come from many src units. Get original unit for
		// the current master file and figure out where to find it in the archive
		var omf masterFile
		err = svc.GDB.Find(&omf, mf.OriginalMfID).Error
		if err != nil {
			return err
		}

		origArchiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", omf.UnitID), omf.Filename)
		svc.logInfo(js, fmt.Sprintf("Copy original master file from %s to %s", origArchiveFile, unitDir))
		md5, err := copyFile(origArchiveFile, destFile, 0664)
		if err != nil {
			return err
		}

		if md5 != omf.MD5 {
			svc.logError(js, fmt.Sprintf("Copied file %s does not match original MD5", destFile))
		}
	}
	return nil
}

func (svc *ServiceContext) unitImagesAvailable(js *jobStatus, tgtUnit *unit, unitDir string) bool {
	if _, err := os.Stat(unitDir); os.IsNotExist(err) {
		svc.logInfo(js, fmt.Sprintf("Directory %s does not exist, creating it", unitDir))
		ensureDirExists(unitDir, 0775)
		return false
	}
	files, err := svc.getTifFiles(js, unitDir, tgtUnit.ID)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to read tif files from %s: %s", unitDir, err.Error()))
		return false
	}
	return len(files) == len(tgtUnit.MasterFiles)
}

func (svc *ServiceContext) getUnitProject(unitID int64) (*project, error) {
	// use limit(1) and find to avoid errors when project does not exist
	var currProj project
	err := svc.GDB.Preload("Notes").Where("unit_id=?", unitID).Limit(1).Find(&currProj).Error
	if err != nil {
		return nil, err
	}
	if currProj.ID == 0 {
		// no project available
		return nil, nil
	}
	return &currProj, nil
}
