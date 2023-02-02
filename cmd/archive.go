package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func (svc *ServiceContext) archiveExists(c *gin.Context) {
	tgtDir := c.Query("dir")
	if tgtDir == "" {
		c.String(http.StatusBadRequest, "dir param is required")
		return
	}
	log.Printf("INFO: check if archive [%s] exists", tgtDir)
	archiveDir := path.Join(svc.ArchiveDir, tgtDir)
	if pathExists(archiveDir) == false {
		c.String(http.StatusNotFound, fmt.Sprintf("%s not found", tgtDir))
		return
	}
	fileCount := 0
	err := filepath.Walk(archiveDir, func(fullPath string, entry os.FileInfo, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".tif" {
			return nil
		}
		fileCount++
		return nil
	})
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, fmt.Sprintf("%s: %d tif files", tgtDir, fileCount))
}

func (svc *ServiceContext) downloadFromArchive(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("CopyArchivedFilesToProduction", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create CopyArchivedFilesToProduction job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	// get the source unit and masterfiles list. Fail if it can't be found
	var tgtUnit unit
	err = svc.GDB.Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
		return db.Order("master_files.filename ASC")
	}).First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit ID %d: %s", unitID, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	// parse the request
	type dlReq struct {
		Filename  string   `json:"filename"`
		Files     []string `json:"files"`
		ComputeID string   `json:"computeID"`
	}
	svc.logInfo(js, "Staring process to download master files from the archive...")
	var req dlReq
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	// Get the requesting user; this is used to determine the destination directory
	var cnt int64
	svc.GDB.Table("staff_members").Where("computing_id=?", req.ComputeID).Count(&cnt)
	if cnt != 1 {
		svc.logFatal(js, fmt.Sprintf("%s is not a valid computing ID", req.ComputeID))
		c.String(http.StatusBadRequest, "invalid compute id")
		return
	}

	// setup destination directory to receive downloaded files
	destPath := path.Join(svc.ProcessingDir, "from_archive", req.ComputeID, fmt.Sprintf("%09d", unitID))
	svc.logInfo(js, fmt.Sprintf("Ensure download destination directory %s exists", destPath))
	err = ensureDirExists(destPath, 0775)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to create download directory %s: %s", destPath, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if strings.ToLower(req.Filename) == "all" {
		svc.logInfo(js, fmt.Sprintf("%s requests to download all master files from unit %d", req.ComputeID, unitID))
		go svc.copyAllFromArchive(js, &tgtUnit, destPath)
		c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
		return
	}

	if req.Filename != "" {
		svc.logInfo(js, fmt.Sprintf("%s requests to download %s from unit %d", req.ComputeID, req.Filename, unitID))
		svc.copyFileFromArchive(js, &tgtUnit, req.Filename, destPath)
		svc.jobDone(js)
		c.String(http.StatusOK, "done")
		return
	}

	if len(req.Files) > 0 {
		svc.logInfo(js, fmt.Sprintf("%s requests to download %d master files from unit %d", req.ComputeID, len(req.Files), unitID))
		go func() {
			for _, filename := range req.Files {
				svc.logInfo(js, fmt.Sprintf("Downloading %s from unit %d", filename, unitID))
				svc.copyFileFromArchive(js, &tgtUnit, filename, destPath)
			}
			svc.logInfo(js, fmt.Sprintf("%d Masterfiles from unit %d copied to %s", len(req.Files), unitID, destPath))
			svc.jobDone(js)
		}()
		c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
		return
	}

	svc.logFatal(js, "Missing required files or filename in request")
	c.String(http.StatusBadRequest, "missing files and filename in request")
}

func (svc *ServiceContext) copyFileFromArchive(js *jobStatus, tgtUnit *unit, fileName, destDir string) {
	srcDir := fmt.Sprintf("%09d", tgtUnit.ID)
	if strings.Contains(tgtUnit.StaffNotes, "Archive: ") {
		srcDir = strings.Split(tgtUnit.StaffNotes, "Archive: ")[1]
	}
	if tgtUnit.Reorder {
		svc.logInfo(js, fmt.Sprintf("Unit %d is a REORDER. Download original master files from archive.", tgtUnit.ID))
		srcDir = ""
	} else {
		svc.logInfo(js, fmt.Sprintf("Unit archive dir [%s]", srcDir))
	}

	found := false
	for _, mf := range tgtUnit.MasterFiles {
		if mf.Filename == fileName {
			if mf.DeaccessionedAt != nil {
				svc.logFatal(js, fmt.Sprintf("Master file %s has been deaccessioned and cannot be downloaded", mf.Filename))
				return
			}

			srcFilename := mf.Filename
			if tgtUnit.Reorder && mf.OriginalMfID != nil {
				var origMF masterFile
				err := svc.GDB.First(&origMF, *mf.OriginalMfID).Error
				if err != nil {
					svc.logFatal(js, fmt.Sprintf("Unable to find original master file %d for %s: %s", *mf.OriginalMfID, mf.Filename, err.Error()))
					return
				}
				srcFilename = origMF.Filename
				srcDir = fmt.Sprintf("%09d", origMF.UnitID)
			}

			svc.logInfo(js, fmt.Sprintf("Copying %s from archive directory %s", srcFilename, srcDir))
			err := svc.copyArchivedFile(js, srcDir, srcFilename, destDir)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to copy %s: %s", mf.Filename, err.Error()))
				return
			}

			found = true
			svc.logInfo(js, fmt.Sprintf("Masterfile %s copied to %s", fileName, destDir))
			return
		}
	}

	if found == false {
		svc.logFatal(js, fmt.Sprintf("Unable to find master file %s in unit %d", fileName, tgtUnit.ID))
	}
}

// called as goroutine to copy all from the archive. it may take a long time
func (svc *ServiceContext) copyAllFromArchive(js *jobStatus, tgtUnit *unit, destDir string) {
	// define preliminary archive directory for the images. This will change if the unit is
	// a reorder or if it is archived in a non-standard location. Non-standard locations are
	// noted in the staff_notes field after 'Archive: ' and reorders are on a file by file basis.
	srcDir := fmt.Sprintf("%09d", tgtUnit.ID)
	if strings.Contains(tgtUnit.StaffNotes, "Archive: ") {
		srcDir = strings.Split(tgtUnit.StaffNotes, "Archive: ")[1]
	}
	if tgtUnit.Reorder {
		svc.logInfo(js, fmt.Sprintf("Unit %d is a REORDER. Download original master files from archive.", tgtUnit.ID))
		srcDir = ""
	} else {
		svc.logInfo(js, fmt.Sprintf("Unit archive dir [%s]", srcDir))
	}

	for _, mf := range tgtUnit.MasterFiles {
		if mf.DeaccessionedAt != nil {
			svc.logInfo(js, fmt.Sprintf("Skipping deaccessioned file %s ", mf.Filename))
			continue
		}

		srcFilename := mf.Filename
		if tgtUnit.Reorder && mf.OriginalMfID != nil {
			var origMF masterFile
			err := svc.GDB.First(&origMF, *mf.OriginalMfID).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to find original master file %d for %s: %s", *mf.OriginalMfID, mf.Filename, err.Error()))
				return
			}
			srcFilename = origMF.Filename
			srcDir = fmt.Sprintf("%09d", origMF.UnitID)
		}

		svc.logInfo(js, fmt.Sprintf("Copying %s from archive directory %s", srcFilename, srcDir))
		err := svc.copyArchivedFile(js, srcDir, srcFilename, destDir)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to copy %s: %s", mf.Filename, err.Error()))
			return
		}
	}
	svc.logInfo(js, fmt.Sprintf("Masterfiles from unit %d copied to %s", tgtUnit.ID, destDir))
	svc.jobDone(js)
}

func (svc *ServiceContext) copyArchivedFile(js *jobStatus, unitDir, filename, destDir string) error {
	archiveFile := path.Join(svc.ArchiveDir, unitDir, filename)
	if strings.Contains(filename, "ARCH") || strings.Contains(filename, "AVRN") || strings.Contains(filename, "VRC") {
		if strings.Contains(filename, "_") {
			overrideDir := strings.Split(filename, "_")[0]
			archiveFile = path.Join(svc.ArchiveDir, overrideDir, filename)
		}
	}
	archiveMD5 := md5Checksum(archiveFile)
	destFile := path.Join(destDir, filename)
	copyMD5, err := copyFile(archiveFile, destFile, 0666)
	if err != nil {
		return err
	}
	if copyMD5 != archiveMD5 {
		svc.logError(js, fmt.Sprintf("MD5 checksum does not match on copied file %s", destFile))
	}
	return nil
}

// archiveFineArtsFile will archive items from finearts which use a different directory and masterfile naming scheme
// EX: 20160809ARCH/20160809ARCH_0001.tif,  20100725VRC
func (svc *ServiceContext) archiveFineArtsFile(srcPath string, fineArtsDir string, tgtMF *masterFile) (string, error) {
	archiveUnitDir := path.Join(svc.ArchiveDir, fineArtsDir)
	archiveFile := path.Join(archiveUnitDir, tgtMF.Filename)
	log.Printf("INFO: archive fine arts file %s to %s", srcPath, archiveFile)

	if pathExists(archiveFile) {
		log.Printf("INFO: %s is already archived", tgtMF.Filename)
		archivedMD5 := md5Checksum(archiveFile)
		if tgtMF.DateArchived == nil {
			now := time.Now()
			tgtMF.DateArchived = &now
			svc.GDB.Model(tgtMF).Select("DateArchived").Updates(*tgtMF)
		}
		return archivedMD5, nil
	}

	err := ensureDirExists(archiveUnitDir, 0777)
	if err != nil {
		return "", fmt.Errorf("unable to create %s: %s", archiveUnitDir, err.Error())
	}

	newMD5, err := copyFile(srcPath, archiveFile, 0664)
	if err != nil {
		return "", err
	}
	log.Printf("INFO: %s archived to %s. MD5 checksum [%s]", tgtMF.Filename, archiveFile, newMD5)

	now := time.Now()
	tgtMF.DateArchived = &now
	err = svc.GDB.Model(tgtMF).Select("DateArchived").Updates(*tgtMF).Error
	if err != nil {
		log.Printf("WARNING: unable to set date archived for master file %d:%s", tgtMF.ID, err.Error())
	}
	return newMD5, nil
}

// archiveFile will create the unit directory, copy the target file, set the archived date and return an MD5 checksum
func (svc *ServiceContext) archiveFile(js *jobStatus, srcPath string, unitID int64, tgtMF *masterFile) (string, error) {
	archiveUnitDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID))
	archiveFile := path.Join(archiveUnitDir, tgtMF.Filename)

	svc.logInfo(js, fmt.Sprintf("Archive %s to %s", srcPath, archiveFile))
	err := ensureDirExists(archiveUnitDir, 0777)
	if err != nil {
		return "", fmt.Errorf("unable to create %s: %s", archiveUnitDir, err.Error())
	}

	newMD5, err := copyFile(srcPath, archiveFile, 0664)
	if err != nil {
		return "", err
	}
	svc.logInfo(js, fmt.Sprintf("%s archived to %s. MD5 checksum [%s]", tgtMF.Filename, archiveFile, newMD5))

	svc.logInfo(js, fmt.Sprintf("Setting date archived for %s", tgtMF.Filename))
	now := time.Now()
	tgtMF.DateArchived = &now
	err = svc.GDB.Model(tgtMF).Select("DateArchived").Updates(*tgtMF).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to set date archived for master file %d:%s", tgtMF.ID, err.Error()))
	}

	svc.logInfo(js, fmt.Sprintf("Masterfile %d : %s successfully archived to %s", tgtMF.ID, tgtMF.Filename, archiveFile))
	return newMD5, nil
}

func (svc *ServiceContext) removeArchive(js *jobStatus, unitID int64, filename string) {
	archiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID), filename)
	svc.logInfo(js, fmt.Sprintf("Remove archived file %s", archiveFile))
	if pathExists(archiveFile) == false {
		svc.logError(js, fmt.Sprintf("No archive found for %s", filename))
		return
	}
	err := os.Remove(archiveFile)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to remove %s from archive: %s", filename, err.Error()))
	}
	svc.logInfo(js, fmt.Sprintf("Archived file %s was removed", archiveFile))
}

func (svc *ServiceContext) renameArchive(js *jobStatus, unitID int64, origFile, origMD5, newFile string) {
	origArchive := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID), origFile)
	newArchive := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID), newFile)
	svc.logInfo(js, fmt.Sprintf("Rename archived file %s -> %s", origArchive, newArchive))

	err := os.Rename(origArchive, newArchive)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to rename %s: %s", origFile, err.Error()))
	}
	newMD5 := md5Checksum(newArchive)
	if newMD5 != origMD5 {
		svc.logError(js, fmt.Sprintf("MD5 does not match for rename %s -> %s; %s vs %s", origArchive, newArchive, origMD5, newMD5))
	}
}

func (svc *ServiceContext) checkOrderArchiveComplete(js *jobStatus, orderID int64) {
	svc.logInfo(js, fmt.Sprintf("Check if all units in order %d are arhived", orderID))
	var cnt int64
	err := svc.GDB.Table("units").Where("order_id=? and unit_status !=? and date_archived is null", orderID, "canceled").Count(&cnt).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to determine if all units archived: %s", err.Error()))
		return
	}

	if cnt == 0 {
		svc.GDB.Model(&order{ID: orderID}).Update("date_archiving_complete", time.Now())
		svc.logInfo(js, fmt.Sprintf("All units in order %d are archived.", orderID))
	}
}
