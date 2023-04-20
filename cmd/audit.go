package main

import (
	"fmt"
	"log"
	"net/http"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type masterFileAudit struct {
	ID            int64       `json:"id"`
	MasterFileID  int64       `json:"masterFileID"`
	MasterFile    *masterFile `gorm:"foreignKey:MasterFileID" json:"masterFile,omitempty"`
	ArchiveExists bool        `json:"archiveExists"`
	ChecksumMatch bool        `json:"checksumMatch"`
	AuditChecksum string      `json:"auditChecksum"`
	AuditedAt     time.Time   `json:"auditedAt"`
}

type auditRequest struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

func (svc *ServiceContext) auditMasterFiles(c *gin.Context) {
	log.Printf("INFO: received masterfile audit request")
	var req auditRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse audit request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.Type != "id" && req.Type != "year" {
		log.Printf("ERROR: invalid audit request type %s", req.Type)
		c.String(http.StatusBadRequest, fmt.Sprintf("invalid audit type %s", req.Type))
		return
	}

	if req.Type == "year" {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("ERROR: Panic recovered during audit year %s: %v", req.Data, r)
					debug.PrintStack()
				}
			}()
			err = svc.auditYear(req.Data)
			if err != nil {
				log.Printf("ERROR: %s", err.Error())
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
		}()
		c.String(http.StatusOK, fmt.Sprintf("audit year %s started", req.Data))
	} else {
		mfID, _ := strconv.ParseInt(req.Data, 10, 64)
		audit, err := svc.auditMasterFile(mfID)
		if err != nil {
			log.Printf("ERROR: %s", err.Error())
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.JSON(http.StatusOK, audit)
	}
}

func (svc *ServiceContext) auditMasterFile(mfID int64) (*masterFileAudit, error) {
	log.Printf("INFO: audit master file %d", mfID)
	var mf masterFile
	err := svc.GDB.Preload("Unit").First(&mf, mfID).Error
	if err != nil {
		return nil, err
	}

	var auditRec *masterFileAudit
	err = svc.GDB.Where("master_file_id=?", mfID).Find(&auditRec).Limit(1).Error
	if err != nil {
		return nil, err
	}

	// regardless of the query above finding anthing, set the master file ID and new audit time
	auditRec.AuditedAt = time.Now()
	auditRec.MasterFileID = mfID
	auditRec.ArchiveExists = false
	auditRec.ChecksumMatch = false
	auditRec.AuditChecksum = ""

	srcDir := fmt.Sprintf("%09d", mf.UnitID)
	if strings.Contains(mf.Unit.StaffNotes, "Archive: ") {
		srcDir = strings.Split(mf.Unit.StaffNotes, "Archive: ")[1]
	}

	archiveFile := path.Join(svc.ArchiveDir, srcDir, mf.Filename)
	if strings.Contains(mf.Filename, "ARCH") || strings.Contains(mf.Filename, "AVRN") || strings.Contains(mf.Filename, "VRC") {
		if strings.Contains(mf.Filename, "_") {
			overrideDir := strings.Split(mf.Filename, "_")[0]
			archiveFile = path.Join(svc.ArchiveDir, overrideDir, mf.Filename)
			log.Printf("INFO: audit masterfile %s is archived in non-standard location %s", mf.Filename, archiveFile)
		}
	}

	if pathExists(archiveFile) == false {
		log.Printf("WARNING: audit finds that masterfile %d is missing archive %s", mfID, archiveFile)
		auditRec.ArchiveExists = false
		auditRec.ChecksumMatch = false
		auditRec.AuditChecksum = ""
	} else {
		auditRec.ArchiveExists = true
		auditRec.AuditChecksum = md5Checksum(archiveFile)
		if auditRec.AuditChecksum != mf.MD5 {
			auditRec.ChecksumMatch = false
			log.Printf("WARNING: master file %d audit finds a checksum mismatch record %s vs archive %s", mfID, mf.MD5, auditRec.AuditChecksum)
		} else {
			auditRec.ChecksumMatch = true
		}
	}

	// if the ID is zero, no record was found and this is the first audit. Create a rec
	if auditRec.ID == 0 {
		err = svc.GDB.Create(auditRec).Error
	} else {
		// update an existing audit
		err = svc.GDB.Save(auditRec).Error
	}

	return auditRec, err
}

func (svc *ServiceContext) auditYear(year string) error {
	log.Printf("INFO: audit all master files from year %s", year)
	// TODO
	return nil
}
