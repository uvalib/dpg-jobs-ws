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

func (svc *ServiceContext) getAttachment(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	attachFileName := c.Param("file")
	log.Printf("INFO: download attachment %s from unit %d", attachFileName, unitID)

	attachDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID), "attachments", attachFileName)
	if pathExists(attachDir) == false {
		log.Printf("INFO: attachment %s does not exist", attachDir)
		c.String(http.StatusBadRequest, fmt.Sprintf("attachment %s doe not exist", attachFileName))
		return
	}

	c.File(attachDir)
}

func (svc *ServiceContext) deleteAttachment(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	attachFileName := c.Param("file")
	log.Printf("INFO: delete attachment %s from unit %d", attachFileName, unitID)
	attachDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", unitID), "attachments", attachFileName)
	if pathExists(attachDir) {
		err := os.Remove(attachDir)
		if err != nil {
			log.Printf("ERROR: unable to remove attached file %s: %s", attachDir, err.Error())
		}
	}

	var tgtAttach attachment
	err := svc.GDB.Where("filename=?", attachFileName).Where("unit_id=?", unitID).Delete(&tgtAttach).Error
	if err != nil {
		log.Printf("ERROR: unable to delete attachment record %s for unit %d: %s", attachFileName, unitID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "done")
}

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
