package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func (svc *ServiceContext) downloadFromArchive(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("CopyArchivedFilesToProduction", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create CopyArchivedFilesToProduction job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	type dlReq struct {
		Filename  string `json:"filename"`
		ComputeID string `json:"computeID"`
	}
	svc.logInfo(js, "Staring process to download master files from the archive...")
	var req dlReq
	err = c.ShouldBindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to parse request: %s", err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	var cnt int64
	svc.GDB.Table("staff_members").Where("computing_id=?", req.ComputeID).Count(&cnt)
	if cnt != 1 {
		svc.logFatal(js, fmt.Sprintf("%s is not a valid computing ID", req.ComputeID))
		c.String(http.StatusBadRequest, "invalid compute id")
		return
	}
	svc.logInfo(js, fmt.Sprintf("%s requests to download %s from unit %d", req.ComputeID, req.Filename, unitID))
	destPath := path.Join(svc.ProcessingDir, "from_archive", req.ComputeID, fmt.Sprintf("%09d", unitID))
	err = ensureDirExists(destPath, 0775)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to create download directory %s: %s", destPath, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if strings.ToLower(req.Filename) == "all" {
		go svc.copyAllFromArchive(js, unitID, destPath)
		c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
		return
	}

	err = svc.copyArchivedFile(js, unitID, req.Filename, destPath)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to copy %s: %s", req.Filename, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	svc.logInfo(js, fmt.Sprintf("Masterfile %s copied to %s", req.Filename, destPath))

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

// called as goroutine to copy all from the archive. it may take a long time
func (svc *ServiceContext) copyAllFromArchive(js *jobStatus, unitID int64, destDir string) {
	var tgtUnit unit
	err := svc.GDB.Preload("MasterFiles", func(db *gorm.DB) *gorm.DB {
		return db.Order("master_files.filename ASC")
	}).First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
		return
	}
	for _, mf := range tgtUnit.MasterFiles {
		if mf.DeaccessionedAt != nil {
			svc.logInfo(js, fmt.Sprintf("Skipping deaccessioned file %s ", mf.Filename))
			continue
		}
		svc.logInfo(js, fmt.Sprintf("Copying %s", mf.Filename))
		err := svc.copyArchivedFile(js, unitID, mf.Filename, destDir)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to copy %s: %s", mf.Filename, err.Error()))
			return
		}
	}
	svc.logInfo(js, fmt.Sprintf("Masterfiles from unit %d copied to %s", unitID, destDir))
	svc.jobDone(js)
}

func (svc *ServiceContext) copyArchivedFile(js *jobStatus, unitID int64, filename string, destDir string) error {
	archiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID), filename)
	if strings.Contains(filename, "ARCH") {
		unitDir := strings.Split(filename, "_")[0]
		archiveFile = path.Join(svc.ArchiveDir, unitDir, filename)
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
// EX: 20160809ARCH/20160809ARCH_0001.tif
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
