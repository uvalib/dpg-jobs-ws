package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func (svc *ServiceContext) replaceMasterFiles(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("ReplaceMasterFiles", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create ReplaceMasterFiles job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	go func() {
		unitDir := fmt.Sprintf("%09d", unitID)
		srcDir := path.Join(svc.ProcessingDir, "finalization", "unit_update", unitDir)
		svc.logInfo(js, fmt.Sprintf("Looking for new *.tif files in %s", srcDir))
		files, err := svc.getTifFiles(js, srcDir, unitID)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to get .tif files in %s: %s", srcDir, err.Error()))
			return
		}

		if len(files) == 0 {
			svc.logFatal(js, "No replacement .tif files found")
			return
		}

		for _, tifFile := range files {

			svc.logInfo(js, fmt.Sprintf("Replacing master file %s", tifFile.filename))
			var mf masterFile
			err := svc.GDB.Preload("ImageTechMeta").Where("filename=?", tifFile.filename).First(&mf).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Masterfile %s was not found in unit. Skipping.", tifFile.filename))
				continue
			}
			mf.Filesize = tifFile.size
			mf.MD5 = md5Checksum(tifFile.path)
			err = svc.GDB.Model(&mf).Select("Filesize", "MD5").Updates(mf).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to save updates to %s: %s", mf.Filename, err.Error()))
			}
			if mf.ImageTechMeta.ID > 0 {
				svc.GDB.Delete(&mf.ImageTechMeta)
			}
			svc.createImageTechMetadata(&mf, tifFile.path)
			svc.publishToIIIF(js, &mf, tifFile.path, true)
			archiveMD5, err := svc.archiveFile(js, tifFile.path, unitID, &mf)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to archive %s: %s", mf.Filename, err.Error()))
			}
			if archiveMD5 != mf.MD5 {
				svc.logError(js, fmt.Sprintf("Archived MD5 does not match for %s", mf.Filename))
			}
		}

		svc.logInfo(js, "Cleaning up working files")
		os.RemoveAll(srcDir)
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) assignMasterFileComponent(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("AssignMasterFileComponent", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create AssignMasterFileComponent job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	var req struct {
		IDs         []int64 `json:"ids"`
		ComponentID int64   `json:"componentID"`
	}
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Update masterfiles %v to component %d", req.IDs, req.ComponentID))
	err = svc.GDB.Table("master_files").Where("id IN ?", req.IDs).Updates(map[string]interface{}{"component_id": req.ComponentID}).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to update component: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) assignMasterFileMetadata(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("AssignMasterFileMetadata", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create AssignMasterFileMetadata job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	var req struct {
		IDs        []int64 `json:"ids"`
		MetadataID int64   `json:"metadataID"`
	}
	svc.logInfo(js, "Staring process to assign master file metadata...")
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Validate metadata %d", req.MetadataID))
	var md metadata
	err = svc.GDB.Preload("ExternalSystem").Find(&md, req.MetadataID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			svc.logFatal(js, fmt.Sprintf("Meadata %d not found", req.MetadataID))
			c.String(http.StatusBadRequest, err.Error())
		} else {
			svc.logFatal(js, fmt.Sprintf("Unable to get metadata %d: %s", req.MetadataID, err.Error()))
			c.String(http.StatusInternalServerError, err.Error())
		}
		return
	}

	if md.Type == "ExternalMetadata" && (md.ExternalSystemID == nil || (md.ExternalSystemID != nil && *md.ExternalSystemID == 0)) {
		svc.logFatal(js, fmt.Sprintf("External metadata %d is missinng the external system ID.", req.MetadataID))
		c.String(http.StatusInternalServerError, err.Error())
	}

	if md.Type == "SirsiMetadata" || (md.Type == "ExternalMetadata" && md.ExternalSystem.Name != "ArchivesSpace") {
		svc.logFatal(js, fmt.Sprintf("Metadata %d is type %s. Only XML and ArchivesSpace are supported", req.MetadataID, md.Type))
		c.String(http.StatusBadRequest, fmt.Sprintf("Metadata %d is %s. Only  XML and ArchivesSpace are supported", md.ID, md.Type))
		return
	}

	svc.logInfo(js, fmt.Sprintf("Update masterfiles %v to metadata %d", req.IDs, req.MetadataID))
	err = svc.GDB.Table("master_files").Where("id IN ?", req.IDs).Updates(map[string]interface{}{"metadata_id": md.ID}).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to update metadata: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) renumberMasterFiles(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("RenumberMasterFiles", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create RenumberMasterFiles job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	type renumberReq struct {
		Filenames []string `json:"filenames"`
		StartNum  int      `json:"startNum"`
	}
	svc.logInfo(js, "Staring process to renumber master files...")
	var req renumberReq
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	sort.Strings(req.Filenames)
	if len(req.Filenames) == 0 {
		svc.logFatal(js, "No filenames in request")
		c.String(http.StatusBadRequest, "no filenames")
		return
	}
	svc.logInfo(js, fmt.Sprintf("These masterfiles will be renamed %v starting at page %d", req.Filenames, req.StartNum))

	svc.logInfo(js, "Load unit and masterfiles")
	var tgtUnit unit
	err = svc.GDB.Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
		return db.Order("master_files.filename ASC")
	}).First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	tgtFN := req.Filenames[0]
	req.Filenames = req.Filenames[1:]
	newNum := req.StartNum
	for _, mf := range tgtUnit.MasterFiles {
		if mf.Filename != tgtFN {
			continue
		}

		svc.logInfo(js, fmt.Sprintf("MasterFile %s renumber from %s to %d", tgtFN, mf.Title, newNum))
		mf.Title = fmt.Sprintf("%d", newNum)
		svc.GDB.Model(&mf).Select("Title").Updates(mf)

		if len(req.Filenames) > 0 {
			tgtFN = req.Filenames[0]
			req.Filenames = req.Filenames[1:]
			newNum++
		} else {
			break
		}
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) deleteMasterFiles(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("DeleteMasterFiles", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create DeleteMasterFiles job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	svc.logInfo(js, "Staring process to delete master files...")

	type delReq struct {
		Filenames []string `json:"filenames"`
	}
	var req delReq
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	sort.Strings(req.Filenames)
	if len(req.Filenames) == 0 {
		svc.logFatal(js, "No filenames in request")
		c.String(http.StatusBadRequest, "no filenames")
		return
	}
	svc.logInfo(js, fmt.Sprintf("These masterfiles will be removed %v", req.Filenames))

	go func() {
		svc.logInfo(js, "Load unit and masterfiles")
		var tgtUnit unit
		err = svc.GDB.Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
			return db.Order("master_files.filename ASC")
		}).First(&tgtUnit, unitID).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
			return
		}

		if tgtUnit.DateDLDeliverablesReady != nil {
			svc.logFatal(js, "Cannot delete from units that have been published")
			return
		}

		unitDir := fmt.Sprintf("%09d", unitID)
		tgtFN := req.Filenames[0]
		req.Filenames = req.Filenames[1:]
		for _, mf := range tgtUnit.MasterFiles {
			if mf.Filename != tgtFN {
				continue
			}

			svc.logInfo(js, fmt.Sprintf("Delete %s", mf.Filename))
			if mf.OriginalMfID == nil && mf.DeaccessionedAt == nil {
				svc.removeArchive(js, unitID, mf.Filename)
				svc.unpublishIIIF(js, &mf)
			} else {
				// clone
				clonedFile := path.Join(svc.ProcessingDir, "finalization", unitDir, mf.Filename)
				if pathExists(clonedFile) {
					svc.logInfo(js, fmt.Sprintf("Removing cloned tif from in_process dir: %s", clonedFile))
					os.Remove(clonedFile)
				}
			}

			svc.logInfo(js, fmt.Sprintf("Removing master file and image tech metadata for %s", tgtFN))
			svc.GDB.Where("master_file_id=?", mf.ID).Delete(&imageTechMeta{})
			svc.GDB.Delete(&masterFile{}, mf.ID)

			if len(req.Filenames) > 0 {
				tgtFN = req.Filenames[0]
				req.Filenames = req.Filenames[1:]
			} else {
				break
			}
		}

		svc.GDB.Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
			return db.Order("master_files.filename ASC")
		}).First(&tgtUnit, unitID) // reload masterfiles list

		svc.logInfo(js, "Updating remaining master files to correct page number gaps")
		prevPage := -1
		currPage := 1
		changeTitle := true
		for _, mf := range tgtUnit.MasterFiles {
			// if page titles are not a number, can't consider them to be sequential
			titleInt, _ := strconv.Atoi(mf.Title)
			if fmt.Sprintf("%d", titleInt) != mf.Title {
				changeTitle = false
			}
			if prevPage > -1 && prevPage+1 != currPage {
				changeTitle = false
			}

			mfPg := getMasterFilePageNum(mf.Filename)
			if mfPg > currPage {
				origFN := mf.Filename
				pageStr := fmt.Sprintf("%04d", currPage)
				newFN := fmt.Sprintf("%s_%s.tif", unitDir, pageStr)
				svc.logInfo(js, fmt.Sprintf("Update MF filename from %s to %s", origFN, newFN))
				mf.Filename = newFN

				// see if the title is a number and that it is the different
				// from the new page number portion. If so, update it
				if titleInt != currPage && changeTitle {
					mf.Title = fmt.Sprintf("%d", currPage)
				}
				err = svc.GDB.Model(&mf).Select("Filename", "Title").Updates(mf).Error
				if err != nil {
					log.Printf("ERR: %s", err.Error())
				}

				if mf.OriginalMfID == nil && mf.DeaccessionedAt == nil {
					svc.renameArchive(js, unitID, origFN, mf.MD5, newFN)
				} else {
					origClonedFile := path.Join(svc.ProcessingDir, "finalization", unitDir, origFN)
					newClonedFile := path.Join(svc.ProcessingDir, "finalization", unitDir, newFN)
					svc.logInfo(js, fmt.Sprintf("Rename cloned file %s -> %s", origClonedFile, newClonedFile))
					os.Rename(newClonedFile, newClonedFile)
				}
			}

			prevPage = currPage
			currPage++
		}
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) addMasterFiles(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("AddMasterFiles", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create AddMasterFiles job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	go func() {
		svc.logInfo(js, "Load unit and masterfiles")
		var tgtUnit unit
		err = svc.GDB.
			Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
				return db.Order("master_files.filename ASC")
			}).
			Preload("MasterFiles.ImageTechMeta").
			Preload("MasterFiles.Locations").
			Preload("MasterFiles.Locations.ContainerType").
			First(&tgtUnit, unitID).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
			return
		}

		srcDir := path.Join(svc.ProcessingDir, "finalization", "unit_update", fmt.Sprintf("%09d", tgtUnit.ID))
		svc.logInfo(js, fmt.Sprintf("Looking for new *.tif files in %s", srcDir))
		files, err := svc.getTifFiles(js, srcDir, unitID)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to get .tif files in %s: %s", srcDir, err.Error()))
			return
		}

		if len(files) == 0 {
			svc.logFatal(js, "No tif files found")
			return
		}

		newPage := -1
		prevPage := -1
		for _, fi := range files {
			pageNum := getMasterFilePageNum(fi.filename)
			if newPage == -1 {
				newPage = pageNum
				prevPage = newPage
			} else {
				if pageNum > prevPage+1 {
					svc.logFatal(js, fmt.Sprintf("Gap in sequence number of new master files; %d to %d", pageNum, prevPage+1))
					return
				}
				prevPage = pageNum
			}
		}

		lastPageNum := getMasterFilePageNum(tgtUnit.MasterFiles[len(tgtUnit.MasterFiles)-1].Filename)
		if newPage > lastPageNum+1 {
			svc.logFatal(js, fmt.Sprintf("New master file sequence number gap (from %d to %d)", lastPageNum, newPage))
			return
		}

		if newPage <= lastPageNum {
			// rename/rearchive files to make room for new files to be inserted
			// If components are involved, return the ID of the component at the insertion point
			err := svc.makeGapForInsertion(js, &tgtUnit, files)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to create gap for ne image insertion: %s", err.Error()))
				return
			}
		}

		// grab the first existing master file and see if it has location data.
		// if it does, the cotainer type for all will be the same. pull it
		var existingLoc *location
		var componentID *int64
		if len(tgtUnit.MasterFiles) > 0 {
			existingLoc = tgtUnit.MasterFiles[0].location()
			componentID = tgtUnit.MasterFiles[0].ComponentID
		}

		// Create new master files for the tif file found in the src dir
		svc.logInfo(js, fmt.Sprintf("Adding %d new master files...", len(files)))
		for _, tf := range files {
			// create MF and tech metadata
			md5 := md5Checksum(tf.path)
			pgNum := getMasterFilePageNum(tf.filename)
			newMF := masterFile{Filename: tf.filename, Title: fmt.Sprintf("%d", pgNum), Filesize: tf.size,
				MD5: md5, UnitID: tgtUnit.ID, ComponentID: componentID, MetadataID: tgtUnit.MetadataID}
			err = svc.GDB.Create(&newMF).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to create %s: %s", tf.filename, err.Error()))
				return
			}
			svc.logInfo(js, fmt.Sprintf("Created masterfile for %s, PID: %s", tf.filename, newMF.PID))
			if existingLoc != nil {
				svc.logInfo(js, fmt.Sprintf("Adding location %+v", *existingLoc))
				svc.GDB.Exec("INSERT into master_file_locations (master_file_id, location_id) values (?,?)", newMF.ID, existingLoc.ID)
			}

			svc.logInfo(js, fmt.Sprintf("Create image tech metadata for %s", tf.filename))
			err = svc.createImageTechMetadata(&newMF, tf.path)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to create %s tech metadata: %s", tf.filename, err.Error()))
			}

			err = svc.publishToIIIF(js, &newMF, tf.path, true)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to publish %s to IIIF: %s", tf.filename, err.Error()))
			}

			// archive file, validate checksum and set archived date
			newMD5, err := svc.archiveFile(js, tf.path, unitID, &newMF)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to archive %s: %s", tf.filename, err.Error()))
			}

			if newMD5 != newMF.MD5 {
				svc.logError(js, fmt.Sprintf("Archived MD5 does not match for %s: %s vs %s", tf.filename, newMD5, newMF.MD5))
			}

			now := time.Now()
			newMF.DateArchived = &now
			svc.GDB.Model(&newMF).Select("DateArchived").Updates(newMF)
		}

		svc.logInfo(js, "Cleaning up working files")
		os.RemoveAll(srcDir)

		svc.jobDone(js)
	}()

	log.Printf("INFO: return add master files job id: %d", js.ID)
	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) updateMasterFileTechMetadata(c *gin.Context) {
	mfID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("UpdateTechMetadata", "MasterFile", mfID)
	if err != nil {
		log.Printf("ERROR: unable to create UpdateTechMetadata job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var tgtMF masterFile
	err = svc.GDB.Preload("ImageTechMeta").First(&tgtMF, mfID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load master file %d: %s", mfID, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if tgtMF.ImageTechMeta.ID != 0 {
		svc.logInfo(js, fmt.Sprintf("Remove existing tech metadata for master file %s", tgtMF.PID))
		err = svc.GDB.Delete(&tgtMF.ImageTechMeta).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to remove existing master file %s tech metadata: %s", tgtMF.PID, err.Error()))
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
	}

	unitDir := fmt.Sprintf("%09d", tgtMF.UnitID)
	archiveFile := path.Join(svc.ArchiveDir, unitDir, tgtMF.Filename)
	svc.logInfo(js, fmt.Sprintf("Create tech metadata ffrom archived master file %s", archiveFile))
	if pathExists(archiveFile) == false {
		svc.logFatal(js, fmt.Sprintf("Master file %d archive %s does not exist", mfID, archiveFile))
		c.String(http.StatusBadRequest, "archive not found")
		return
	}

	err = svc.createImageTechMetadata(&tgtMF, archiveFile)
	if err != nil {
		log.Printf("ERROR: unable to create image tech metadata: %s", err.Error())
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "updated")
}

func (svc *ServiceContext) updateMasterFileIIIF(c *gin.Context) {
	mfID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("UpdateIIIF", "MasterFile", mfID)
	if err != nil {
		log.Printf("ERROR: unable to create UpdateIIIF job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var tgtMF masterFile
	err = svc.GDB.Preload("ImageTechMeta").First(&tgtMF, mfID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load master file %d: %s", mfID, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if tgtMF.DeaccessionedAt != nil {
		svc.logFatal(js, fmt.Sprintf("Master file %d:%s has been deaccessioned and cannot be updated", mfID, tgtMF.Filename))
		c.String(http.StatusBadRequest, "deaccessioned master files cannot be updated")
		return
	}
	if tgtMF.OriginalMfID != nil {
		svc.logFatal(js, fmt.Sprintf("Master file %d:%s is a clone and cannot be updated", mfID, tgtMF.Filename))
		c.String(http.StatusBadRequest, "cloned master files cannot be updated")
		return
	}

	unitDir := fmt.Sprintf("%09d", tgtMF.UnitID)
	archiveFile := path.Join(svc.ArchiveDir, unitDir, tgtMF.Filename)
	if pathExists(archiveFile) == false {
		svc.logFatal(js, fmt.Sprintf("Master file %d archive %s does not exist", mfID, archiveFile))
		c.String(http.StatusBadRequest, "archive not found")
		return
	}

	if tgtMF.ImageTechMeta.Width == 0 || tgtMF.ImageTechMeta.Height == 0 {
		svc.logFatal(js, fmt.Sprintf("%s has invalid tech metdata and is likely corrupt; skipping further processing", tgtMF.PID))
		c.String(http.StatusBadRequest, "invalid tech metadata; width and height are zero")
		return
	}

	colorTest := strings.TrimSpace(tgtMF.ImageTechMeta.ColorSpace)
	if colorTest == "CMYK" {
		svc.logFatal(js, fmt.Sprintf("%s has unsupported colorspace %s; skipping further processing", tgtMF.PID, colorTest))
		c.String(http.StatusBadRequest, fmt.Sprintf("unsupported colorspace %s", colorTest))
		return
	}

	go func() {
		err = svc.publishToIIIF(js, &tgtMF, archiveFile, true)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Update IIIF for master file %d from archive %s failed: %s", mfID, archiveFile, err.Error()))
			c.String(http.StatusInternalServerError, err.Error())
			return
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) deaccessionMasterFile(c *gin.Context) {
	mfID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("DeaccessionMasterFile", "MasterFile", mfID)
	if err != nil {
		log.Printf("ERROR: unable to create DeaccessionMasterFile job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	type deaccessinReq struct {
		ComputeID string `json:"computeID"`
		Note      string `json:"note"`
	}
	var req deaccessinReq
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	var mf masterFile
	err = svc.GDB.First(&mf, mfID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to find masterfile %d: %s", mfID, err.Error()))
		return
	}

	var staff staffMember
	err = svc.GDB.Where("computing_id=?", req.ComputeID).First(&staff).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to find staff member %s: %s", req.ComputeID, err.Error()))
		return
	}

	if mf.OriginalMfID != nil || (mf.OriginalMfID == nil && svc.hasReorders(&mf)) {
		svc.logFatal(js, "Cannot deaccession a cloned master file.")
		return
	}

	svc.logInfo(js, fmt.Sprintf("User %s begins to deaccession masterfile %s", req.ComputeID, mf.Filename))
	now := time.Now()
	mf.DeaccessionedAt = &now
	mf.DeaccessionNote = req.Note
	mf.DeaccessionedByID = &staff.ID
	err = svc.GDB.Model(&mf).Select("DeaccessionedAt", "DeaccessionedByID", "DeaccessionNote").Updates(mf).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to mark masterfile %s as deaccessioned: %s", mf.Filename, err.Error()))
		return
	}

	svc.removeArchive(js, mf.UnitID, mf.Filename)
	svc.unpublishIIIF(js, &mf)

	// If necessary, flag for publish to DL
	if mf.DateDlIngest != nil {
		svc.logInfo(js, "File was published to DL; flagging for removal")
		mf.DateDlUpdate = &now
		svc.GDB.Model(&mf).Select("DateDlUpdate").Updates(mf)
		svc.GDB.Model(&metadata{ID: *mf.MetadataID}).Updates(metadata{DateDlUpdate: &now})
	}

	svc.logInfo(js, fmt.Sprintf("masterfile %s deaccessioned by %s", mf.Filename, req.ComputeID))
	svc.jobDone(js)
}

func (svc *ServiceContext) hasReorders(mf *masterFile) bool {
	var count int64
	svc.GDB.Table("master_files").Where("original_mf_id=?", mf.ID).Count(&count)
	return count > 0
}

func (svc *ServiceContext) makeGapForInsertion(js *jobStatus, tgtUnit *unit, tifFiles []tifInfo) error {
	tgtFile := tifFiles[0].filename
	gapSize := len(tifFiles)
	svc.logInfo(js, fmt.Sprintf("Renaming/rearchiving all master files from %s to make room for insertion of %d new master files", tgtFile, gapSize))
	done := false
	for idx := len(tgtUnit.MasterFiles) - 1; idx >= 0; idx-- {
		mf := tgtUnit.MasterFiles[idx]
		if mf.Filename == tgtFile {
			done = true
		}

		// figure out new filename and rename/re-title
		origFN := mf.Filename
		origPageNum := getMasterFilePageNum(origFN)
		newPageNum := origPageNum + gapSize
		paddedPageNum := fmt.Sprintf("%04d", newPageNum)
		newFN := fmt.Sprintf("%s_%s.tif", strings.Split(origFN, "_")[0], paddedPageNum)
		newTitle := mf.Title
		titleInt, _ := strconv.Atoi(newTitle)
		if titleInt == origPageNum {
			// sometimes the title is not a number. only change title if it is a number
			newTitle = fmt.Sprintf("%d", titleInt+gapSize)
		}
		svc.logInfo(js, fmt.Sprintf("Rename %s to %s. Title %s", origFN, newFN, newTitle))
		mf.Filename = newFN
		mf.Title = newTitle
		err := svc.GDB.Model(&mf).Select("Filename", "Title").Updates(mf).Error
		if err != nil {
			return err
		}

		// copy archived file to new name and validate checksums
		svc.renameArchive(js, tgtUnit.ID, origFN, mf.MD5, newFN)

		if done == true {
			break
		}
	}
	return nil
}
