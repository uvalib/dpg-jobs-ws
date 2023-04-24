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
	Type  string `json:"type"`
	Data  string `json:"data"`
	Email string `json:"email"`
}

type auditItem struct {
	ID         int64
	UnitID     int64
	Filename   string
	MD5        string
	StaffNotes string
}

type auditYearResults struct {
	StartedAt            string
	Year                 string
	MasterFileCount      uint
	MasterFileErrorCount uint
	ChecksumErrorCount   uint
	MissingArchiveCount  uint
	SuccessCount         uint
	FatalError           string
	FinishedAt           string
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
		if req.Email == "" {
			log.Printf("ERROR: audit year requires email recipient")
			c.String(http.StatusBadRequest, "email is required for a year audit")
			return
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("ERROR: Panic recovered during audit year %s: %v", req.Data, r)
					debug.PrintStack()
				}
			}()
			svc.auditYear(req.Data, req.Email)
		}()
		c.String(http.StatusOK, fmt.Sprintf("audit year %s started for %s", req.Data, req.Email))
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
	var mf auditItem
	mfQ := "select master_files.id as id, filename, md5, unit_id, u.staff_notes as staff_notes from master_files"
	mfQ += " inner join units u on u.id = unit_id where master_files.id = ?"
	err := svc.GDB.Raw(mfQ, mfID).Scan(&mf).Error
	if err != nil {
		return nil, err
	}
	return svc.performAudit(&mf)
}

func (svc *ServiceContext) auditYear(year, emailRecipient string) {
	log.Printf("INFO: audit all master files from year %s", year)
	mfQ := "select master_files.id as id, filename, md5, unit_id, u.staff_notes as staff_notes from master_files"
	mfQ += " inner join units u on u.id = unit_id"
	mfQ += " where year(master_files.created_at) = ? and master_files.date_archived is not null and original_mf_id is null"

	auditSummary := auditYearResults{StartedAt: time.Now().Format("2006-01-02 03:04:05 PM"), Year: year}

	rows, err := svc.GDB.Raw(mfQ, year).Rows()
	defer rows.Close()
	if err != nil {
		log.Printf("ERROR: unable to get master files for year %s: %s", year, err.Error())
		auditSummary.FatalError = err.Error()
		return
	}

	for rows.Next() {
		auditSummary.MasterFileCount++

		var mf auditItem
		err := svc.GDB.ScanRows(rows, &mf)
		if err != nil {
			log.Printf("ERROR: unable to load master file data for audit: %s", err.Error())
			auditSummary.MasterFileErrorCount++
		} else {
			res, err := svc.performAudit(&mf)
			if err != nil {
				log.Printf("ERROR: unable to audit master file %d: %s", mf.ID, err.Error())
				auditSummary.MasterFileErrorCount++
			} else {
				if res.ArchiveExists == false {
					auditSummary.MissingArchiveCount++
				} else if res.ChecksumMatch == false {
					// if the archive is missing, there cannot be a checksum; don't count this as an errors
					auditSummary.ChecksumErrorCount++
				}

				if res.ArchiveExists && res.ChecksumMatch {
					auditSummary.SuccessCount++
				}
			}
		}
	}

	auditSummary.FinishedAt = time.Now().Format("2006-01-02 03:04:05 PM")
	svc.sendAuditResultsEmail(emailRecipient, auditSummary)

	log.Printf("INFO: audit for year %s is done", year)
}

func (svc *ServiceContext) performAudit(mf *auditItem) (*masterFileAudit, error) {
	var auditRec *masterFileAudit
	err := svc.GDB.Where("master_file_id=?", mf.ID).Find(&auditRec).Limit(1).Error
	if err != nil {
		return nil, err
	}

	// regardless of the query above finding anthing, set the master file ID and new audit time
	auditRec.AuditedAt = time.Now()
	auditRec.MasterFileID = mf.ID
	auditRec.ArchiveExists = false
	auditRec.ChecksumMatch = false
	auditRec.AuditChecksum = ""

	srcDir := fmt.Sprintf("%09d", mf.UnitID)
	if strings.Contains(mf.StaffNotes, "Archive: ") {
		srcDir = strings.Split(mf.StaffNotes, "Archive: ")[1]
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
		log.Printf("WARNING: audit finds that masterfile %d is missing archive %s", mf.ID, archiveFile)
		auditRec.ArchiveExists = false
		auditRec.ChecksumMatch = false
		auditRec.AuditChecksum = ""
	} else {
		auditRec.ArchiveExists = true
		auditRec.AuditChecksum = md5Checksum(archiveFile)
		if auditRec.AuditChecksum != mf.MD5 {
			auditRec.ChecksumMatch = false
			log.Printf("WARNING: master file %d audit finds a checksum mismatch record %s vs archive %s", mf.ID, mf.MD5, auditRec.AuditChecksum)
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
