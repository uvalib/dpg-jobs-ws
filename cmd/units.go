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

	go func() {
		for _, cr := range req {
			var tgtUnit unit
			err = svc.GDB.Preload("MasterFiles").Preload("MasterFiles.ImageTechMeta").First(&tgtUnit, cr.UnitID).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", cr.UnitID, err.Error()))
				return
			}

			if cr.AllFiles {
				svc.cloneAllMasterFiles(js, &tgtUnit)
			} else {
				for _, mf := range cr.Files {
					svc.cloneMasterFile(js, &tgtUnit, mf.ID, mf.Title)
				}
			}
		}
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) cloneAllMasterFiles(js *jobStatus, tgtUnit *unit) {

}

func (svc *ServiceContext) cloneMasterFile(js *jobStatus, tgtUnit *unit, tgtID int64, newTitle string) {

}
