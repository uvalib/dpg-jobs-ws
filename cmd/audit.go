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
	"gorm.io/gorm"
)

type masterFileAudit struct {
	ID             int64       `json:"id"`
	MasterFileID   int64       `json:"masterFileID"`
	MasterFile     *masterFile `gorm:"foreignKey:MasterFileID" json:"masterFile,omitempty"`
	ArchiveExists  bool        `json:"archiveExists"`
	ChecksumExists bool        `json:"checksumExists"`
	ChecksumMatch  bool        `json:"checksumMatch"`
	AuditChecksum  string      `json:"auditChecksum"`
	IIIFExists     bool        `gorm:"column:iiif_exists" json:"iiifExists"`
	AuditedAt      time.Time   `json:"auditedAt"`
}

type auditRequest struct {
	Type   string `json:"type"`
	Data   string `json:"data"`
	Email  string `json:"email"`
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
}

type auditItem struct {
	ID         int64
	PID        string `gorm:"column:pid"`
	UnitID     int64
	Filename   string
	MD5        string
	StaffNotes string
}

type auditYearResults struct {
	StartedAt            string
	Offset               int
	Limit                int
	Year                 string
	MasterFileCount      uint
	MasterFileErrorCount uint
	ChecksumErrorCount   uint
	MissingChecksumCount uint
	MissingArchiveCount  uint
	MissingIIIFCount     uint
	SuccessCount         uint
	FatalError           string
	FinishedAt           string
}

type auditFixLimit struct {
	Limit int `json:"limit"`
}

func (svc *ServiceContext) checkMissingMD5Audit(c *gin.Context) {
	log.Printf("INFO: received request to fix missing md5 checsksum identified by audit")
	var req auditFixLimit
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse fix request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if req.Limit > 0 {
		log.Printf("INFO: md5 fixes will be limited to %d files", req.Limit)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: panic recovered when processing md5 repair: %v", r)
				debug.PrintStack()
			}

			batchSize := 1000
			var fails []masterFileAudit
			cnt := 0
			err := svc.GDB.Joins("MasterFile").Where("checksum_exists=? and archive_exists=?", false, true).FindInBatches(&fails, batchSize, func(tx *gorm.DB, batch int) error {
				for _, mfa := range fails {
					cnt++

					archiveFile := svc.getArchiveFileName(mfa.MasterFile)
					log.Printf("INFO: check missing md5 for %s", archiveFile)
					fileMD5 := md5Checksum(archiveFile)
					if fileMD5 != mfa.AuditChecksum {
						log.Printf("ERROR:checksum match for %s failed", mfa.MasterFile.Filename)
					} else {
						log.Printf("INFO: md5 matches original audit, set it as the md5 for the masterfile")
						tgtMF := mfa.MasterFile
						tgtMF.MD5 = fileMD5
						err = svc.GDB.Model(&tgtMF).Select("MD5").Updates(tgtMF).Error
						if err != nil {
							log.Printf("ERROR: unable to update verified but missing md5 for masterfile %d - %s: %s", tgtMF.ID, tgtMF.Filename, err.Error())
						} else {
							log.Printf("INFO: update md5 audit record")
							mfa.AuditedAt = time.Now()
							mfa.ChecksumMatch = true
							mfa.ChecksumExists = true
							err = svc.GDB.Model(&mfa).Select("AuditedAt", "ChecksumMatch", "ChecksumExists").Updates(mfa).Error
							if err != nil {
								log.Printf("ERROR: unable to update audit rec %d: %s", mfa.ID, err.Error())
							}
						}
					}

					if req.Limit > 0 && cnt >= req.Limit {
						log.Printf("INFO: stopping after processing %d master files", req.Limit)
						break
					}
				}
				if req.Limit > 0 && cnt >= req.Limit {
					return fmt.Errorf("reached max processing count %d", cnt)
				}
				return nil
			}).Error
			if err != nil {
				log.Printf("ERROR: fix md5 process has stopped: %s", err.Error())
			}
		}()
	}()

	c.String(http.StatusOK, "md5 fix started")
}

// curl -X POST https://dpg-jobs.lib.virginia.edu/audit/fix/jp2  -H "Content-Type: application/json" --data '{"limit": 1}'
func (svc *ServiceContext) fixFailedJP2Audit(c *gin.Context) {
	log.Printf("INFO: received request to fix missing jp2 files identified by audit")
	var req auditFixLimit
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse fix request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if req.Limit > 0 {
		log.Printf("INFO: fixes will be limited to %d files", req.Limit)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: panic recovered when processing jp2 repair: %v", r)
				debug.PrintStack()
			}

			batchSize := 1000
			var fails []masterFileAudit
			cnt := 0
			err := svc.GDB.Preload("MasterFile").Preload("MasterFile.ImageTechMeta").
				Where("iiif_exists=? and archive_exists=?", false, true).
				FindInBatches(&fails, batchSize, func(tx *gorm.DB, batch int) error {
					for _, mfa := range fails {
						cnt++
						log.Printf("INFO: Publish masterfile %s: %s from audit record %d", mfa.MasterFile.PID, mfa.MasterFile.Filename, mfa.ID)

						archiveFile := svc.getArchiveFileName(mfa.MasterFile)
						log.Printf("INFO: publish %s to iiif", archiveFile)
						if pathExists(archiveFile) == false {
							log.Printf("ERROR: master file %d archive %s does not exist", mfa.MasterFileID, archiveFile)
						} else {
							err := svc.publishToIIIF(nil, mfa.MasterFile, archiveFile, true)
							if err != nil {
								log.Printf("ERROR: publish jp2 for %s failed: %s", mfa.MasterFile.Filename, err.Error())
							} else {
								mfa.AuditedAt = time.Now()
								mfa.IIIFExists = true
								err = svc.GDB.Model(&mfa).Select("AuditedAt", "IIIFExists").Updates(mfa).Error
								if err != nil {
									log.Printf("ERROR: unable to update audit rec %d: %s", mfa.ID, err.Error())
								}
							}
						}
						if req.Limit > 0 && cnt >= req.Limit {
							log.Printf("INFO: stopping after processing %d master files", req.Limit)
							break
						}
					}
					if req.Limit > 0 && cnt >= req.Limit {
						return fmt.Errorf("reached max processing count %d", cnt)
					}
					return nil
				}).Error
			if err != nil {
				log.Printf("ERROR: fix missing jp2 images process has stopped: %s", err.Error())
			}
		}()
	}()
	c.String(http.StatusOK, "jp2 fix started")
}

func (svc *ServiceContext) getArchiveFileName(mf *masterFile) string {
	unitDir := fmt.Sprintf("%09d", mf.UnitID)
	archiveFile := path.Join(svc.ArchiveDir, unitDir, mf.Filename)
	if strings.Contains(mf.Filename, "ARCH") || strings.Contains(mf.Filename, "AVRN") || strings.Contains(mf.Filename, "VRC") {
		if strings.Contains(mf.Filename, "_") {
			overrideDir := strings.Split(mf.Filename, "_")[0]
			archiveFile = path.Join(svc.ArchiveDir, overrideDir, mf.Filename)
		}
	}
	return archiveFile
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

	if req.Type != "id" && req.Type != "year" && req.Type != "unit" {
		log.Printf("ERROR: invalid audit request type %s", req.Type)
		c.String(http.StatusBadRequest, fmt.Sprintf("invalid audit type %s", req.Type))
		return
	}

	switch req.Type {
	case "year":
		if req.Email == "" {
			log.Printf("ERROR: audit year requires email recipient")
			c.String(http.StatusBadRequest, "email is required for a year audit")
			return
		}
		if req.Offset > 0 && req.Limit == 0 {
			log.Printf("ERROR: audit year offset requires a limit")
			c.String(http.StatusBadRequest, "non-zero offset requires non-zero limit")
			return
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("ERROR: Panic recovered during audit year %s: %v", req.Data, r)
					debug.PrintStack()
				}
			}()
			svc.auditYear(req)
		}()
		c.String(http.StatusOK, fmt.Sprintf("audit year %s started for %s", req.Data, req.Email))
	case "unit":
		unitID, _ := strconv.ParseInt(req.Data, 10, 64)
		go svc.auditUnitMasterFiles(unitID)
		c.String(http.StatusOK, fmt.Sprintf("audit unit %d started", unitID))
	default:
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
	mfQ := "select master_files.id as id, pid, filename, md5, unit_id, u.staff_notes as staff_notes from master_files"
	mfQ += " inner join units u on u.id = unit_id where master_files.id = ?"
	err := svc.GDB.Raw(mfQ, mfID).Scan(&mf).Error
	if err != nil {
		return nil, err
	}
	return svc.performAudit(&mf)
}

func (svc *ServiceContext) auditUnitMasterFiles(unitID int64) {
	js, err := svc.createJobStatus("AuditUnitMasterFiles", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		return
	}
	svc.logInfo(js, fmt.Sprintf("Begin audit master files from unit %d", unitID))
	var tgtUnit unit
	err = svc.GDB.First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
		return
	}

	if tgtUnit.Reorder {
		svc.logFatal(js, "Cannot audit reorders")
		return
	}

	var unitMasterFiles []auditItem
	mfQ := "select master_files.id as id, pid, filename, md5, unit_id, u.staff_notes as staff_notes from master_files"
	mfQ += " inner join units u on u.id = unit_id where unit_id = ?"
	err = svc.GDB.Raw(mfQ, unitID).Scan(&unitMasterFiles).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit master files: %s", err.Error()))
		return
	}

	for _, mf := range unitMasterFiles {
		svc.logInfo(js, fmt.Sprintf("Audit master file %d", mf.ID))
		_, err := svc.performAudit(&mf)
		if err != nil {
			svc.logError(js, fmt.Sprintf("Audit failed: %s", err.Error()))
		}
	}

	svc.jobDone(js)
}

func (svc *ServiceContext) auditYear(req auditRequest) {
	year := req.Data
	log.Printf("INFO: %s requets master files audit from year %s offest %d limit %d", req.Email, year, req.Offset, req.Limit)

	auditSummary := auditYearResults{StartedAt: time.Now().Format("2006-01-02 03:04:05 PM"),
		Year: year, Offset: req.Offset, Limit: req.Limit}

	mfQ := svc.GDB.Model(&masterFile{}).Joins("inner join units u on u.id = unit_id").
		Where("year(master_files.created_at) = ? and master_files.date_archived is not null and original_mf_id is null", year)
	if req.Offset > 0 {
		mfQ = mfQ.Offset(req.Offset)
	}
	if req.Limit > 0 {
		mfQ = mfQ.Limit(req.Limit)
	}

	var hits []auditItem
	batchSize := 1000
	err := mfQ.FindInBatches(&hits, batchSize, func(tx *gorm.DB, batch int) error {
		log.Printf("INFO: processing batch %d of master files from year %s; total processed: %d", batch, year, auditSummary.MasterFileCount)
		for _, mf := range hits {
			auditSummary.MasterFileCount++

			res, err := svc.performAudit(&mf)
			if err != nil {
				log.Printf("ERROR: unable to audit master file %d: %s", mf.ID, err.Error())
				auditSummary.MasterFileErrorCount++
			} else {
				if res.ArchiveExists == false {
					// if there is no archive, there cannot be a checksum nor checcksum comparison
					auditSummary.MissingArchiveCount++
				} else {
					// archive exists; only track mismatch errors if the master file checksum also exists
					if res.ChecksumExists == false {
						auditSummary.MissingChecksumCount++
					} else if res.ChecksumMatch == false {
						auditSummary.ChecksumErrorCount++
					}
				}
				if res.IIIFExists == false {
					auditSummary.MissingIIIFCount++
				}

				if res.ArchiveExists && res.ChecksumMatch && res.IIIFExists {
					auditSummary.SuccessCount++
				}
			}
		}

		// return error will stop future batches
		return nil
	}).Error

	if err != nil {
		log.Printf("ERROR: unable to get master files for year %s: %s", year, err.Error())
		auditSummary.FatalError = err.Error()
		return
	}

	auditSummary.FinishedAt = time.Now().Format("2006-01-02 03:04:05 PM")
	svc.sendAuditResultsEmail(req.Email, auditSummary)

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
	auditRec.ChecksumExists = mf.MD5 != ""
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
		// log.Printf("WARNING: audit finds that masterfile %d is missing archive %s", mf.ID, archiveFile)
		auditRec.ArchiveExists = false
		auditRec.ChecksumMatch = false
		auditRec.AuditChecksum = ""
	} else {
		auditRec.ArchiveExists = true
		auditRec.AuditChecksum = md5Checksum(archiveFile)
		if auditRec.AuditChecksum != mf.MD5 {
			auditRec.ChecksumMatch = false
			// log.Printf("WARNING: master file %d audit finds a checksum mismatch record %s vs archive %s", mf.ID, mf.MD5, auditRec.AuditChecksum)
		} else {
			auditRec.ChecksumMatch = true
		}
	}

	iiifInfo := svc.getIIIFContext(mf.PID)
	auditRec.IIIFExists = true
	iiifExist, err := svc.iiifExists(iiifInfo)
	if err != nil {
		log.Printf("ERROR: call to check for iiif file failed: %s", err.Error())
	} else {
		if iiifExist == false {
			auditRec.IIIFExists = false
			// log.Printf("WARNING: master file %d audit finds no iiif file", mf.ID)
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
