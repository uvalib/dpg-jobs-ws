package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
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
	err := svc.GDB.Preload("MasterFiles").First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
		return
	}
	for _, mf := range tgtUnit.MasterFiles {
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

// archiveFile will create the unit directory, copy the target file and return an MD5 checksum
func (svc *ServiceContext) archiveFile(js *jobStatus, srcPath string, unitID int64, filename string) (string, error) {
	archiveUnitDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID))
	archiveFile := path.Join(archiveUnitDir, filename)

	svc.logInfo(js, fmt.Sprintf("Archive %s to %s", srcPath, archiveFile))
	err := ensureDirExists(archiveUnitDir, 0777)
	if err != nil {
		return "", fmt.Errorf("unable to create %s: %s", archiveUnitDir, err.Error())
	}

	newMD5, err := copyFile(srcPath, archiveFile, 0664)
	if err != nil {
		return "", err
	}
	svc.logInfo(js, fmt.Sprintf("%s archived to %s. MD5 checksum [%s]", filename, archiveFile, newMD5))
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