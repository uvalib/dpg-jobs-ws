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
