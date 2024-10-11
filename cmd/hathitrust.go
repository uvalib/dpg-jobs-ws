package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-xmlfmt/xmlfmt"
)

type hathitrustStatus struct {
	ID                  uint       `json:"id"`
	MetadataID          int64      `json:"metadataID"`
	RequestedAt         time.Time  `json:"requestedAt"`
	PackageCreatedAt    *time.Time `json:"packageCreatedAt"`
	PackageSubmittedAt  *time.Time `json:"packageSubmittedAt"`
	PackageStatus       string     `json:"packageStatus"`
	MetadataSubmittedAt *time.Time `json:"metadataSubmittedAt"`
	MetadataStatus      string     `json:"metadataStatus"`
	FinishedAt          *time.Time `json:"finishedAt"`
	Notes               string     `json:"notes"`
}

type hathiTrustRequest struct {
	ComputeID   string  `json:"computeID"`
	MetadataIDs []int64 `json:"records"`
	OrderIDs    []int64 `json:"orders"`
	Mode        string  `json:"mode"`
	Name        string  `json:"name"`
}

type hathiTrustInitRequest struct {
	ComputeID string `json:"computeID"`
	OrderID   int64  `json:"orderID"`
}

type hathiTrustSubmitRequest struct {
	ComputeID string   `json:"computeID"`
	OrderID   int64    `json:"order"`
	Barcodes  []string `json:"barcodes"`
}

type hathiTrustSubmission struct {
	Path     string
	Name     string
	Size     int
	MimeType string
	IsDir    bool
	ID       string
}

// EX: curl -X POST  https://dpg-jobs.lib.virginia.edu/hathitrust/init -H "Content-Type: application/json" --data '{"computeID": "lf6f", "orderID": 11441}'
// (LOCAL)  curl -X POST  http://localhost:8180/hathitrust/init -H "Content-Type: application/json" --data '{"computeID": "lf6f", "orderID": 10104}'
func (svc *ServiceContext) flagOrderForHathiTrust(c *gin.Context) {
	log.Printf("INFO: received request to init order for submission to hathitrust")
	var req hathiTrustInitRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse hathitrust add request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.OrderID == 0 {
		log.Printf("INFO: add hathitrust request is missing required order id")
		c.String(http.StatusBadRequest, "orderID is required")
		return
	}

	submitUser, err := svc.validateHathiTrustRequestor(req.ComputeID)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	js, err := svc.createJobStatus("HathiTrustInit", "StaffMember", submitUser.ID)
	if err != nil {
		log.Printf("ERROR: unable to create HathiTrustInit job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var tgtUnits []unit
	err = svc.GDB.Where("order_id=? and unit_status != ? and intended_use_id=?", req.OrderID, "canceled", 110).Find(&tgtUnits).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("unable to get units for order %d: %s", req.OrderID, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("%d units in order %d are suitable for hathitrust submission", len(tgtUnits), req.OrderID))
	flagCnt := 0
	for _, tgtUnit := range tgtUnits {
		var mfCnt int64
		err = svc.GDB.Table("master_files").Where("unit_id=?", tgtUnit.ID).Count(&mfCnt).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("unable to get master file count for unit %d, it will not be flagged for hathitrust inclusion: %s", tgtUnit.ID, err.Error()))
			continue
		}
		if mfCnt == 0 {
			svc.logInfo(js, fmt.Sprintf("unit %d has no master files and will not be flagged for hathitrust inclusion", tgtUnit.ID))
			continue
		}

		svc.logInfo(js, fmt.Sprintf("[%d] flag metadata %d from unit %d for inclusion in hathitrust", (flagCnt+1), *tgtUnit.MetadataID, tgtUnit.ID))
		err = svc.flagMetadataForHathiTrust(js, *tgtUnit.MetadataID)
		if err != nil {
			log.Printf("ERROR: %s", err.Error())
			continue
		}
		flagCnt++
	}
	svc.logInfo(js, fmt.Sprintf("%d metadata records fro order %d flagged for hathitrust", flagCnt, req.OrderID))
	svc.jobDone(js)
	c.String(http.StatusOK, "ok")
}

func (svc *ServiceContext) flagMetadataForHathiTrust(js *jobStatus, mdID int64) error {
	svc.logInfo(js, fmt.Sprintf("flag metadata %d for inclusion in hathitrust", mdID))
	var tgtMD metadata
	err := svc.GDB.First(&tgtMD, mdID).Error
	if err != nil {
		return fmt.Errorf("unable to load metadata %d: %s", mdID, err.Error())
	}

	if tgtMD.HathiTrust {
		svc.logInfo(js, fmt.Sprintf("metadata %d is already flagged for hathitrust", mdID))
		return nil
	}

	var existCnt int64
	err = svc.GDB.Table("hathitrust_statuses").Where("metadata_id=?", mdID).Count(&existCnt).Error
	if err != nil {
		return fmt.Errorf("unable to determine if metadata %d has hathitrust status: %s", mdID, err.Error())
	}
	if existCnt > 0 {
		svc.logInfo(js, fmt.Sprintf("metadata %d is already has hathitrust ststus", mdID))
		return nil
	}

	err = svc.GDB.Model(&metadata{ID: mdID}).Update("hathitrust", 1).Error
	if err != nil {
		return fmt.Errorf("unable to flag metadata %d for hathitrust: %s", mdID, err.Error())
	}

	htStatus := hathitrustStatus{MetadataID: mdID, RequestedAt: time.Now(), MetadataStatus: "pending", PackageStatus: "pending"}
	err = svc.GDB.Create(&htStatus).Error
	if err != nil {
		return fmt.Errorf("unable to create hathitrust status for metadata %d: %s", mdID, err.Error())
	}
	return nil
}

// curl -X POST https://dpg-jobs.lib.virginia.edu/hathitrust/metadata -H "Content-Type: application/json" --data '{"computeID": "lf6f", "mode": "prod", "orders": [11195,11441], "name": "batch20240523"}'
// curl -X POST https://dpg-jobs.lib.virginia.edu/hathitrust/metadata -H "Content-Type: application/json" --data '{"computeID": "lf6f", "mode": "dev", "orders": [11195,11441], "name": "batch20240523"}'
// curl -X POST http://localhost:8180/hathitrust/metadata -H "Content-Type: application/json" --data '{"computeID": "lf6f", "mode": "dev", "orders": [11884], "name": "testorder11884"}'
func (svc *ServiceContext) submitHathiTrustMetadata(c *gin.Context) {
	log.Printf("INFO: received hathitrust metadata request")
	var req hathiTrustRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse hathitrust metadata request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.Mode != "dev" && req.Mode != "test" && req.Mode != "prod" {
		log.Printf("INFO: hathitrust metadata request requires mode dev,test or prod")
		c.String(http.StatusBadRequest, "mode must be dev, test or prod")
		return
	}

	submitUser, err := svc.validateHathiTrustRequestor(req.ComputeID)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if len(req.MetadataIDs) == 0 && len(req.OrderIDs) == 0 {
		log.Printf("INFO: hathitrust metadata request requires a list of order or metadata ids")
		c.String(http.StatusBadRequest, "order or metadata id list is required")
		return
	}

	js, err := svc.createJobStatus("HathiTrustMetadata", "StaffMember", submitUser.ID)
	if err != nil {
		log.Printf("ERROR: unable to create HathiTrush metadata job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	submissionInfo := fmt.Sprintf("for metadata records %v", req.MetadataIDs)
	if len(req.OrderIDs) > 0 {
		// when selecting metadata records from an order to submit, don't pick records that have already been submitted or accepted
		mdQ := "select u.metadata_id from units u inner join hathitrust_statuses hs on hs.metadata_id = u.metadata_id "
		mdQ += " where order_id in ? and unit_status != ? and package_status != ? and package_status != ?"
		err = svc.GDB.Raw(mdQ, req.OrderIDs, "canceled", "submitted", "accepted").Scan(&req.MetadataIDs).Error
		if err != nil {
			log.Printf("ERROR: unable to get metadata ids for orders %v: %s", req.OrderIDs, err.Error())
			c.String(http.StatusInternalServerError, fmt.Sprintf("uable to get metadata ids for orders: %s", err.Error()))
			return
		}
		submissionInfo = fmt.Sprintf("for orders %v with %d metadata records", req.OrderIDs, len(req.MetadataIDs))
	}

	svc.logInfo(js, fmt.Sprintf("%s requests %s hathitrust metadata submission %s", req.ComputeID, req.Mode, submissionInfo))

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: panic recovered during hathitrust metadata submission: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered while submitting hathitrust metadata: %v", r))
			}
		}()

		dateStr := time.Now().Format("20060102")
		uploadFN := fmt.Sprintf("UVA-2_%s", dateStr)
		if req.Name != "" {
			uploadFN += fmt.Sprintf("_%s", req.Name)
		}
		uploadFN += ".xml"
		mdFileName := path.Join(svc.ProcessingDir, "hathitrust", uploadFN)
		svc.logInfo(js, fmt.Sprintf("Write local copy of metadata submission to %s", mdFileName))
		if pathExists(mdFileName) {
			svc.logInfo(js, fmt.Sprintf("%s already exists; removing", mdFileName))
			os.Remove(mdFileName)
		}
		metadataFile, err := os.Create(mdFileName)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to create metadaa file: %s", err.Error()))
			return
		}

		metadataFile.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<collection xmlns=\"http://www.loc.gov/MARC21/slim\">")
		updatedIDs := make([]int64, 0)
		generatedCatKeys := make([]string, 0)
		for _, mdID := range req.MetadataIDs {
			var tgtMD metadata
			err = svc.GDB.First(&tgtMD, mdID).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to load metadata %d: %s", mdID, err.Error()))
				continue
			}

			// NOTE: skip any barcodes that are not like Xnnnnnn (these have a dash; ex: 500878-2001)
			// Multi-volume items have different metadata records. Each has the same cat key, but different barocdes. The
			// MARC record for the cat key will list all barcodes, so don't request the same cat key multiple times
			if strings.Contains(tgtMD.Barcode, "-") {
				svc.logInfo(js, fmt.Sprintf("Skipping record with barcode that has an autogenerated item ID: %s", tgtMD.Barcode))
				continue
			}

			alreadySubmitted := false
			for _, catKey := range generatedCatKeys {
				if catKey == tgtMD.CatalogKey {
					alreadySubmitted = true
					break
				}
			}

			if alreadySubmitted == true {
				svc.logInfo(js, fmt.Sprintf("Catalog key %s has already been submitted; skipping", tgtMD.CatalogKey))
				continue
			}

			generatedCatKeys = append(generatedCatKeys, tgtMD.CatalogKey)
			xml, err := svc.getMARCMetadata(tgtMD)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to retrieve MARC XML for %d: %s", mdID, err.Error()))
				continue
			}
			metadataFile.WriteString(fmt.Sprintf("\n%s", xml))
			updatedIDs = append(updatedIDs, tgtMD.ID)
		}
		metadataFile.WriteString("\n</collection>")
		metadataFile.Close()
		mdSize := getFileSize(mdFileName)
		svc.logInfo(js, fmt.Sprintf("Metadata for %d records with size %d has been written to %s", len(updatedIDs), mdSize, mdFileName))

		if req.Mode == "dev" {
			// In dev mode, there is nothing more to do. just log the location where the metadata file can be found
			svc.logInfo(js, "Metadata request is in dev mode. File not submitted, no email sent and status not updated")

		} else if len(updatedIDs) > 0 {
			uploadDirectory := "submissions"
			if req.Mode == "test" {
				uploadDirectory = "testrecs"
			}
			svc.logInfo(js, fmt.Sprintf("Upload %s to ftps server %s in directory %s as %s", uploadFN, svc.HathiTrust.FTPS, uploadDirectory, svc.HathiTrust.User))

			// curl --tls-max 1.2 -u ht-uva:ht-uva:PASS -T UVA-2_20240610_order11884.xml --ssl-reqd --ftp-pasv ftp://ftps.cdlib.org/submissions/UVA-2_20240610_order11884.xml
			userPass := fmt.Sprintf("%s:%s", svc.HathiTrust.User, svc.HathiTrust.Pass)
			ftpDest := fmt.Sprintf("ftp://%s/%s/%s", svc.HathiTrust.FTPS, uploadDirectory, uploadFN)
			ftpOut, err := exec.Command("curl", "--tls-max", "1.2", "-u", userPass, "-T", mdFileName, "--ssl-reqd", "--ftp-pasv", ftpDest).CombinedOutput()
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Upload failed: %s", ftpOut))
				return
			}

			if req.Mode == "prod" {
				svc.logInfo(js, "Send email notification to hathitrust")
				err = svc.sendHathiTrustUploadEmail(submitUser, uploadFN, mdSize, len(updatedIDs))
				if err != nil {
					svc.logFatal(js, fmt.Sprintf("Unable to send email to HathiTrust: %s", err.Error()))
					return
				}

				svc.logInfo(js, "Update metadata submitted dates")
				now := time.Now()
				err = svc.GDB.Model(&hathitrustStatus{}).Where("metadata_id in ?", updatedIDs).
					Updates(hathitrustStatus{MetadataSubmittedAt: &now, MetadataStatus: "submitted"}).Error
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to update HathiTrust status records: %s", err.Error()))
				}
			} else {
				svc.logInfo(js, fmt.Sprintf("metadata request is in mode=%s, no email sent and status not updated", req.Mode))
			}
		} else {
			svc.logFatal(js, "No metadata records uploaded")
			return
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, "submit request started")
}

// curl -X POST  https://dpg-jobs.lib.virginia.edu/hathitrust/package -H "Content-Type: application/json" --data '{"computeID": "lf6f", "records": [107255]}'
// curl -X POST  https://dpg-jobs.lib.virginia.edu/hathitrust/package -H "Content-Type: application/json" --data '{"computeID": "lf6f", "orders": [12121]}'
func (svc *ServiceContext) createHathiTrustPackage(c *gin.Context) {
	log.Printf("INFO: received hathitrust package request")
	var req hathiTrustRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse hathitrust package request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if len(req.MetadataIDs) == 0 && len(req.OrderIDs) == 0 {
		log.Printf("INFO: hathitrust package request requires order id or metadata ids")
		c.String(http.StatusBadRequest, "order or metadata id list is required")
		return
	}

	submitUser, err := svc.validateHathiTrustRequestor(req.ComputeID)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	js, err := svc.createJobStatus("HathiTrustPackage", "StaffMember", submitUser.ID)
	if err != nil {
		log.Printf("ERROR: unable to create HathiTrush package job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	submissionInfo := fmt.Sprintf("for metadata records %v", req.MetadataIDs)
	if len(req.OrderIDs) > 0 {
		err = svc.GDB.Raw("select metadata_id from units where order_id in ? and unit_status != ?", req.OrderIDs, "canceled").Scan(&req.MetadataIDs).Error
		if err != nil {
			log.Printf("ERROR: unable to get metadata ids for orders %v: %s", req.OrderIDs, err.Error())
			c.String(http.StatusInternalServerError, fmt.Sprintf("uable to get metadata ids for orders: %s", err.Error()))
			return
		}
		submissionInfo = fmt.Sprintf("for orders %v with %d metadata records", req.OrderIDs, len(req.MetadataIDs))
	}

	svc.logInfo(js, fmt.Sprintf("%s requests hathitrust package generation %s", req.ComputeID, submissionInfo))

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered while generating hathitrust packages: %v", r))
			}
		}()

		packagedIDs := make([]int64, 0)
		for _, mdID := range req.MetadataIDs {
			svc.logInfo(js, fmt.Sprintf("Validate metadata record %d", mdID))
			var md metadata
			err = svc.GDB.Preload("OcrHint").Find(&md, mdID).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to get metadata %d: %s", mdID, err.Error()))
				continue
			}

			if md.Barcode == "" {
				svc.failHathiTrustPackage(js, md.ID, "required barcode is missing")
				continue
			}

			if md.OcrHint == nil {
				svc.failHathiTrustPackage(js, md.ID, "required OCRHint setting is missing")
				continue
			}

			svc.logInfo(js, "Find target units")
			var units []unit
			err = svc.GDB.Where("metadata_id=? and unit_status != ? and reorder = ? and intended_use_id=?", mdID, "canceled", false, 110).Find(&units).Error
			if err != nil {
				svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to get a unit: %s", err.Error()))
				continue
			}

			if len(units) == 0 {
				svc.failHathiTrustPackage(js, md.ID, "no units found")
				continue
			}

			svc.logInfo(js, fmt.Sprintf("%d units found; validate orderID and do necessary OCR", len(units)))
			var unitIDs []int64
			var orderID int64
			for _, tgtUnit := range units {
				unitIDs = append(unitIDs, tgtUnit.ID)
				if orderID == 0 {
					orderID = tgtUnit.OrderID
				} else if orderID != tgtUnit.OrderID {
					svc.logError(js, "Units are from multiple orders")
					orderID = 0
					break
				}

				if md.OcrHint.OcrCandidate {
					svc.logInfo(js, fmt.Sprintf("This metadata record is an OCR candidate; check master files in unit %d to see if OCR needs to be done", tgtUnit.ID))
					var mfOCRCnt int64
					err = svc.GDB.Table("master_files").Where("unit_id=? and NOT ISNULL(transcription_text) and transcription_text !=?", tgtUnit.ID, "").Count(&mfOCRCnt).Error
					if err != nil {
						svc.logInfo(js, fmt.Sprintf("Unable to determine OCR status for unit %d, assume it needs to be done: %s", tgtUnit.ID, err.Error()))
						mfOCRCnt = 0
					}
					if mfOCRCnt == 0 {
						// note; this call will not return until all master files in the unit have OCR results
						err = svc.requestUnitOCR(js, md.PID, tgtUnit.ID, md.OcrLanguageHint)
						if err != nil {
							svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to request ocr for unit %d: %s", tgtUnit.ID, err.Error()))
						}
					}
				}
			}

			if orderID == 0 {
				svc.failHathiTrustPackage(js, md.ID, "unable to determine package order")
				continue
			}

			// date archived on the order should be latest date that images were compressed.
			// load the unit and get that date and use it for the manifest
			var tgtOrder order
			err = svc.GDB.First(&tgtOrder, orderID).Error
			if err != nil {
				svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unabel to find order %d for metadata %s: %s", orderID, md.PID, err.Error()))
				continue
			}
			compressDate := tgtOrder.DateArchivingComplete
			if compressDate == nil {
				svc.failHathiTrustPackage(js, md.ID, "unable to determine compression date")
				continue
			}

			svc.logInfo(js, fmt.Sprintf("Load master files from digitial collection units for metadata %d", mdID))
			var masterFiles []masterFile
			err = svc.GDB.Where("unit_id in ?", unitIDs).Find(&masterFiles).Error
			if err != nil {
				svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to load master files: %s", err.Error()))
				continue
			}

			if len(masterFiles) == 0 {
				svc.failHathiTrustPackage(js, md.ID, "no master files found")
				continue
			}

			// Setup package assembly directory; /digiserv-production/hathitrust/order_[order_id]/[barcode]
			// final package data will reside at /digiserv-production/hathitrust/order_[order_id]/[barcode].zip
			orderDir := fmt.Sprintf("order_%d", orderID)
			assembleDir := path.Join(svc.ProcessingDir, "hathitrust", orderDir, md.Barcode)
			packageName := fmt.Sprintf("%s.zip", md.Barcode)
			packageFilename := path.Join(svc.ProcessingDir, "hathitrust", orderDir, packageName)
			if pathExists(assembleDir) {
				svc.logInfo(js, fmt.Sprintf("Clean up pre-existing package assembly directory %s", assembleDir))
				err := os.RemoveAll(assembleDir)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to cleanup prior package assembly directory %s: %s", assembleDir, err.Error()))
				}
			}
			if pathExists(packageFilename) {
				svc.logInfo(js, fmt.Sprintf("Clean up pre-existing package %s", packageFilename))
				err = os.Remove(packageFilename)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to cleanup prior package %s: %s", packageFilename, err.Error()))
				}
			}

			svc.logInfo(js, fmt.Sprintf("Ensure package assembly directory %s exists", assembleDir))
			err = ensureDirExists(assembleDir, 0777)
			if err != nil {
				svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to create package assembly directory %s: %s", assembleDir, err.Error()))
				continue
			}

			// Create the package ZIP file
			svc.logInfo(js, fmt.Sprintf("Package will be generated here %s", packageFilename))
			zipFile, err := os.Create(packageFilename)
			if err != nil {
				svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to create package zip %s: %s", packageFilename, err.Error()))
				continue
			}
			zipWriter := zip.NewWriter(zipFile)
			defer zipFile.Close()
			defer zipWriter.Close()

			// Create the checksum file; it will appended as files are processed
			checksumPath := path.Join(assembleDir, "checksum.md5")
			checksumFile, err := os.Create(checksumPath)
			if err != nil {
				svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to create checksum file %s: %s", checksumPath, err.Error()))
				continue
			}
			checksumFile.Chmod(0666)
			defer checksumFile.Close()

			// Write the meta.yml file
			lastCaptureDate := masterFiles[len(masterFiles)-1].CreatedAt
			ymlMD5, err := svc.writeMetaYML(assembleDir, &lastCaptureDate, compressDate)
			if err != nil {
				svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to write meta.yml: %s", err.Error()))
				continue
			}
			checksumFile.WriteString(fmt.Sprintf("%s  meta.yml\n", ymlMD5))

			masterFileError := false
			for idx, mf := range masterFiles {
				// download jp2 from iiif to assembly directory, then add it to the zip
				destFN := fmt.Sprintf("%08d.jp2", (idx + 1))
				destPath := path.Join(assembleDir, destFN)
				iiifInfo := svc.getIIIFContext(mf.PID)
				err = svc.downlodFromIIIF(js, iiifInfo.S3Key(), destPath)
				if err != nil {
					svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to download masterfile %s from iiif: %s", mf.Filename, err.Error()))
					masterFileError = true
					break
				}

				_, err := addFileToZip(packageFilename, zipWriter, assembleDir, destFN)
				if err != nil {
					svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to add %s to zip file: %s", destPath, err.Error()))
					masterFileError = true
					break
				}
				checksumFile.WriteString(fmt.Sprintf("%s  %s\n", md5Checksum(destPath), destFN))

				// if applicable, copy ocr text to the package dir. make the name match the image name
				if md.OcrHint.OcrCandidate {
					txtFileName := fmt.Sprintf("%08d.txt", (idx + 1))
					destTxtPath := path.Join(assembleDir, txtFileName)
					err = os.WriteFile(destTxtPath, []byte(mf.TranscriptionText), 0666)
					if err != nil {
						svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to write ocr text to %s: %s", destTxtPath, err.Error()))
						masterFileError = true
						break
					}
					_, err := addFileToZip(packageFilename, zipWriter, assembleDir, txtFileName)
					if err != nil {
						svc.failHathiTrustPackage(js, md.ID, fmt.Sprintf("unable to add ocr text file %s to zip: %s", destTxtPath, err.Error()))
						masterFileError = true
						break
					}
					checksumFile.WriteString(fmt.Sprintf("%s  %s\n", md5Checksum(destTxtPath), txtFileName))
				}
			}

			if masterFileError {
				continue
			}

			addFileToZip(packageFilename, zipWriter, assembleDir, "meta.yml")
			addFileToZip(packageFilename, zipWriter, assembleDir, "checksum.md5")
			svc.logInfo(js, fmt.Sprintf("%s successfully generated", packageFilename))

			checksumFile.Close()
			zipWriter.Close()
			zipFile.Close()
			packagedIDs = append(packagedIDs, mdID)

			defer func() {
				svc.logInfo(js, fmt.Sprintf("Cleaning up assembly directory %s", assembleDir))
				err = os.RemoveAll(assembleDir)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to clean up assembly directory: %s", err.Error()))
				}
			}()
		}

		if len(packagedIDs) > 0 {
			svc.logInfo(js, "Update metadata package created dates")
			now := time.Now()
			err = svc.GDB.Model(&hathitrustStatus{}).Where("metadata_id in ?", packagedIDs).
				Updates(hathitrustStatus{PackageCreatedAt: &now}).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to update HathiTrust status records: %s", err.Error()))
			}
			if len(packagedIDs) != len(req.MetadataIDs) {
				svc.logError(js, fmt.Sprintf("Not all packages created: Total: %d, Uploaded: %d", len(packagedIDs), len(req.MetadataIDs)))
			}
		} else {
			svc.logFatal(js, "No packages created")
			return
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) failHathiTrustPackage(js *jobStatus, mdID int64, reason string) {
	svc.logError(js, fmt.Sprintf("metadata %d failed package creation: %s", mdID, reason))

	var htStatus hathitrustStatus
	err := svc.GDB.Where("metadata_id=?", mdID).First(&htStatus).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to find HathiTrust status for metadata %d: %s", mdID, err.Error()))
		return
	}
	htStatus.Notes += reason
	htStatus.PackageStatus = "failed"
	err = svc.GDB.Save(&htStatus).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to update HathiTrust status record: %s", err.Error()))
	}
}

// EXAMPLE: curl -X POST  https://dpg-jobs.lib.virginia.edu/hathitrust/package/submit -H "Content-Type: application/json" --data '{"computeID": "lf6f", "order": 12121}'
// EXAMPLE: curl -X POST  https://dpg-jobs.lib.virginia.edu/hathitrust/package/submit -H "Content-Type: application/json" --data '{"computeID": "lf6f", "order": 12059, "barcodes": ["X004938494"]}'
func (svc *ServiceContext) submitHathiTrustPackage(c *gin.Context) {
	log.Printf("INFO: received hathitrust package submit request")
	var req hathiTrustSubmitRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse hathitrust submit request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.OrderID == 0 {
		log.Printf("INFO: hathitrust package submit request requires am order id")
		c.String(http.StatusBadRequest, "order id is required")
		return
	}

	submitUser, err := svc.validateHathiTrustRequestor(req.ComputeID)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	orderDir := path.Join(svc.ProcessingDir, "hathitrust", fmt.Sprintf("order_%d", req.OrderID))
	if pathExists(orderDir) == false {
		log.Printf("ERROR: order directory %s not found", orderDir)
		c.String(http.StatusNotFound, fmt.Sprintf("order directory %s not found", orderDir))
		return
	}

	js, err := svc.createJobStatus("HathiTrustPackgeSubmit", "StaffMember", submitUser.ID)
	if err != nil {
		log.Printf("ERROR: unable to create HathiTrush submission job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if len(req.Barcodes) == 0 {
		svc.logInfo(js, fmt.Sprintf("%s requests hathitrust full package submission for order %d", req.ComputeID, req.OrderID))
	} else {
		svc.logInfo(js, fmt.Sprintf("%s requests hathitrust package submission of barcodes %v from order %d", req.ComputeID, req.Barcodes, req.OrderID))
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: panic recovered during hathitrust package submission: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered while submitting hathitrust packages: %v", r))
			}
		}()

		svc.logInfo(js, "get a list of packages in the submission directory")
		priorSubmissions, err := svc.getHathiTrustDirectoryContent()

		submitted := 0
		err = filepath.WalkDir(orderDir, func(filePath string, d fs.DirEntry, err error) error {
			if d.IsDir() {
				return nil
			}
			if filepath.Ext(d.Name()) != ".zip" {
				return nil
			}

			doSubmit := false
			tgtBC := strings.TrimSuffix(d.Name(), filepath.Ext(d.Name()))
			if len(req.Barcodes) > 0 {
				for _, bc := range req.Barcodes {
					if bc == tgtBC {
						doSubmit = true
						break
					}
				}
			} else {
				doSubmit = true
			}

			if doSubmit == false {
				return nil
			}

			for _, ps := range priorSubmissions {
				subName := strings.TrimSuffix(ps.Name, ".zip")
				if subName == tgtBC {
					svc.logInfo(js, fmt.Sprintf("package for %s already exists in the submission directory", tgtBC))
					doSubmit = false
					break
				}
			}
			if doSubmit == false {
				return nil
			}

			svc.logInfo(js, fmt.Sprintf("submit %s", filePath))
			cmd := exec.Command(path.Join(svc.HathiTrust.RCloneBin, "rclone"),
				"--config", svc.HathiTrust.RCloneConfig,
				"copyto", filePath,
				fmt.Sprintf("%s:%s/%s", svc.HathiTrust.RCloneRemote, svc.HathiTrust.RemoteDir, d.Name()))
			svc.logInfo(js, fmt.Sprintf("Submit command: %v", cmd))
			out, err := cmd.CombinedOutput()
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to submit %s: %s:%s", d.Name(), err.Error(), out))
				return nil
			}
			submitted++

			svc.logInfo(js, fmt.Sprintf("update status for %s", tgtBC))
			var mdRec metadata
			err = svc.GDB.InnerJoins("HathiTrustStatus").Where("barcode=?", tgtBC).First(&mdRec).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to load metadata for %s: %s", tgtBC, err.Error()))
				return nil
			}
			now := time.Now()
			mdRec.HathiTrustStatus.PackageSubmittedAt = &now
			mdRec.HathiTrustStatus.PackageStatus = "submitted"
			err = svc.GDB.Model(&mdRec.HathiTrustStatus).Select("PackageSubmittedAt", "PackageStatus").Updates(mdRec.HathiTrustStatus).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to update hathitrust status for %s: %s", tgtBC, err.Error()))
				return nil
			}

			return nil
		})

		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to traverse order dir: %s", err.Error()))
			return
		}

		svc.logInfo(js, fmt.Sprintf("%d packages submitted", submitted))
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, "package submit request started")
}

func (svc *ServiceContext) validateHathiTrustRequestor(computeID string) (*staffMember, error) {
	if computeID == "" {
		return nil, fmt.Errorf("compute id is required")
	}

	var submitUser staffMember
	err := svc.GDB.Where("computing_id=?", computeID).First(&submitUser).Error
	if err != nil {
		return nil, fmt.Errorf("user %s not found: %s", computeID, err.Error())
	}

	if submitUser.Role != Admin {
		return nil, fmt.Errorf("%s does not have permission to make this request", computeID)
	}
	return &submitUser, nil
}

func (svc *ServiceContext) listHathiTrustSubmissions(c *gin.Context) {
	resp, err := svc.getHathiTrustDirectoryContent()
	if err != nil {
		log.Printf("ERROR: listHathiTrustSubmissions failed: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (svc *ServiceContext) getHathiTrustDirectoryContent() ([]hathiTrustSubmission, error) {
	// rclone lsjson hathitrust:virginia
	cmd := exec.Command(path.Join(svc.HathiTrust.RCloneBin, "rclone"),
		"--config", svc.HathiTrust.RCloneConfig,
		"lsjson",
		fmt.Sprintf("%s:%s", svc.HathiTrust.RCloneRemote, svc.HathiTrust.RemoteDir))
	log.Printf("INFO: list submitted hathitrust package request: %v", cmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	var resp []hathiTrustSubmission
	err = json.Unmarshal(out, &resp)
	if err != nil {
		return nil, fmt.Errorf("unable to parse rclone response: %s", err.Error())
	}
	return resp, nil
}

func (svc *ServiceContext) getMARCMetadata(md metadata) (string, error) {
	marcBytes, mdErr := svc.getRequest(fmt.Sprintf("https://ils.lib.virginia.edu/uhtbin/getMarc?barcode=%s&hathitrust=yes&type=xml", md.Barcode))
	if mdErr != nil {
		return "", fmt.Errorf("%d:%s", mdErr.StatusCode, mdErr.Message)
	}
	marcStr := string(marcBytes)
	idx := strings.Index(marcStr, "<leader>")
	if idx < 0 {
		return "", fmt.Errorf("unable to get marc metadata for %s", md.PID)
	}
	marcStr = "<record>" + marcStr[idx:]

	prettyXML := xmlfmt.FormatXML(marcStr, "", "   ")
	prettyXML = strings.TrimSpace(prettyXML)
	return prettyXML, nil
}

func (svc *ServiceContext) writeMetaYML(assembleDir string, digitizationDate *time.Time, compressedAt *time.Time) (string, error) {
	ymlPath := path.Join(assembleDir, "meta.yml")
	ymlFile, err := os.Create(ymlPath)
	if err != nil {
		return "", err
	}
	ymlFile.Chmod(0666)
	defer ymlFile.Close()

	ymlFile.WriteString(fmt.Sprintf("capture_date: %s\n", digitizationDate.Format(time.RFC3339)))
	ymlFile.WriteString("scanner_user: \"University of Virginia: Digital Production Group\"\n")
	ymlFile.WriteString("contone_resolution_dpi: 600\n\n")
	ymlFile.WriteString(fmt.Sprintf("image_compression_date: %s\n", compressedAt.Format(time.RFC3339)))
	ymlFile.WriteString("image_compression_agent: virginia\n")
	ymlFile.WriteString("image_compression_tool: [\"kdu_compress v8.0.5\",\"ImageMagick 7.1.0\"]\n\n")

	ymlFile.WriteString("scanning_order: left-to-right\n")
	ymlFile.WriteString("reading_order: left-to-right\n")
	md5 := md5Checksum(ymlPath)

	return md5, nil
}

func copyJP2(src string, dest string) error {
	origFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer origFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, origFile)
	if err != nil {
		return err
	}
	destFile.Close()

	return os.Chmod(dest, 0666)
}
