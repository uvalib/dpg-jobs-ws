package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"

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

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
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
