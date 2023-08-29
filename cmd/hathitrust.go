package main

import (
	"archive/zip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-xmlfmt/xmlfmt"
	"github.com/kardianos/ftps"
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

	if req.ComputeID == "" {
		log.Printf("ERROR: hathitrust metadata request requires compute id")
		c.String(http.StatusBadRequest, "compute if is required")
		return
	}
	if len(req.MetadataIDs) == 0 && len(req.OrderIDs) == 0 {
		log.Printf("INFO: hathitrust metadata request requires a list of order or metadata ids")
		c.String(http.StatusBadRequest, "order or metadata id list is required")
		return
	}

	var submitUser staffMember
	err = svc.GDB.Where("computing_id=?", req.ComputeID).First(&submitUser).Error
	if err != nil {
		log.Printf("ERROR: user %s not found: %s", req.ComputeID, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if submitUser.Role != Admin {
		log.Printf("ERROR: hathitrust metadata requests can only be submitted by admin users")
		c.String(http.StatusBadRequest, "you do not have permission to make this request")
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
		err = svc.GDB.Raw("select metadata_id from units where order_id in ? and unit_status != ?", req.OrderIDs, "canceled").Scan(&req.MetadataIDs).Error
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

		svc.logInfo(js, fmt.Sprintf("connecting to ftps server %s as %s", svc.HathiTrust.FTPS, svc.HathiTrust.User))
		ftpsCtx, ftpsCancel := context.WithCancel(context.Background())
		defer ftpsCancel()
		ftpsConn, err := ftps.Dial(ftpsCtx, ftps.DialOptions{
			Host:     svc.HathiTrust.FTPS,
			Port:     21,
			Username: svc.HathiTrust.User,
			Passowrd: svc.HathiTrust.Pass,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			ExplicitTLS: true,
		})
		defer ftpsConn.Close()
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to connect to FTPS: %s", err.Error()))
			return
		}

		uploadDirectory := "submissions"
		if req.Mode == "test" || req.Mode == "dev" {
			uploadDirectory = "testrecs"
		}
		svc.logInfo(js, fmt.Sprintf("Set FTPS working directory to %s", uploadDirectory))
		err = ftpsConn.Chdir(uploadDirectory)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to switch to upload directory %s: %s", uploadDirectory, err.Error()))
			return
		}
		pwd, err := ftpsConn.Getwd()
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to get working directory: %s", err.Error()))
			return
		}
		if strings.Contains(pwd, uploadDirectory) == false {
			svc.logFatal(js, fmt.Sprintf("Working directory mismatch; %s vs %s", pwd, uploadDirectory))
			return
		}

		dateStr := time.Now().Format("20060102")
		uploadFN := fmt.Sprintf("UVA-2_%s", dateStr)
		if req.Name != "" {
			uploadFN += fmt.Sprintf("_%s", req.Name)
		}
		uploadFN += ".xml"
		metadataOut := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<collection xmlns=\"http://www.loc.gov/MARC21/slim\">"
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
			metadataOut += fmt.Sprintf("\n%s", xml)
			updatedIDs = append(updatedIDs, tgtMD.ID)
		}
		metadataOut += "\n</collection>"

		if len(updatedIDs) > 0 {
			if req.Mode == "dev" {
				svc.logInfo(js, metadataOut)
			} else {
				svc.logInfo(js, fmt.Sprintf("Upload %d MARC records with total size %d to FTPS %s as %s", len(updatedIDs), len(metadataOut), svc.HathiTrust.FTPS, uploadFN))
				err = ftpsConn.Upload(ftpsCtx, uploadFN, strings.NewReader(metadataOut))
				if err != nil {
					svc.logFatal(js, fmt.Sprintf("upload failed: %s", err.Error()))
					return
				}

				localCopy := path.Join(svc.ProcessingDir, "hathitrust", uploadFN)
				svc.logInfo(js, fmt.Sprintf("Write local copy of metadata submission to %s", localCopy))
				err = os.WriteFile(localCopy, []byte(metadataOut), 0664)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to write local copy to %s: %s", localCopy, err.Error()))
				}

			}
			// cancel the ftps context immediately when the upload is done
			ftpsCancel()

			svc.sendHathiTrustUploadEmail(uploadFN, len(metadataOut), len(updatedIDs))
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to send email to HathiTrust: %s", err.Error()))
				return
			}

			if req.Mode == "prod" || req.Mode == "dev" {
				svc.logInfo(js, "Update metadata submitted dates")
				now := time.Now()
				err = svc.GDB.Model(&hathitrustStatus{}).Where("metadata_id in ?", updatedIDs).
					Updates(hathitrustStatus{MetadataSubmittedAt: &now, MetadataStatus: "pending"}).Error
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to update HathiTrust status records: %s", err.Error()))
				}
			}

			if len(updatedIDs) != len(req.MetadataIDs) {
				svc.logError(js, fmt.Sprintf("Not all metadata records uploaded: Total: %d, Uploaded: %d", len(updatedIDs), len(req.MetadataIDs)))
			}

		} else {
			svc.logFatal(js, "No metadata records uploaded")
			return
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, "submit request started")
}

func (svc *ServiceContext) createHathiTrustPackage(c *gin.Context) {
	log.Printf("INFO: received hathitrust package request")
	var req hathiTrustRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse hathitrust package request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.ComputeID == "" {
		log.Printf("ERROR: hathitrust package request requires compute id")
		c.String(http.StatusBadRequest, "compute id is required")
		return
	}
	if len(req.MetadataIDs) == 0 && len(req.OrderIDs) == 0 {
		log.Printf("INFO: hathitrust package request requires order id or metadata ids")
		c.String(http.StatusBadRequest, "order or metadata id list is required")
		return
	}

	var submitUser staffMember
	err = svc.GDB.Where("computing_id=?", req.ComputeID).First(&submitUser).Error
	if err != nil {
		log.Printf("ERROR: user %s not found: %s", req.ComputeID, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if submitUser.Role != Admin {
		log.Printf("ERROR: hathitrust package requests can only be submitted by admin users")
		c.String(http.StatusBadRequest, "you do not have permission to make this request")
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
				svc.logError(js, "Metadata is missing the required Barcode")
				continue
			}

			if md.OcrHint == nil {
				svc.logError(js, "Metadata is missing the required OCRHint setting")
				continue
			}

			svc.logInfo(js, "Find target unit")
			var units []unit
			err = svc.GDB.Where("metadata_id=? and unit_status != ?", mdID, "canceled").Find(&units).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to get a unit for metadata %d: %s", mdID, err.Error()))
				continue
			}

			if len(units) == 0 {
				svc.logError(js, fmt.Sprintf("No units found for metadata %d", mdID))
				continue
			}

			// SPECIAL CASE: metadata 104398, 104399, 104400 are items were multiple volumes were bound together
			// and have a single metadata record, but were scanned as separate units. For these 3 metadata records
			// pull master files from ALL  of the units
			// tgtUnit := units[0]
			var masterFiles []masterFile
			unitIDs := make([]int64, 0)
			var latestCompressDate *time.Time
			specialCase := (mdID == 104398 || mdID == 104399 || mdID == 104400)
			if specialCase == false {
				// This is the snandard case; enforce 1 unit per metadata record
				if len(units) > 1 {
					svc.logError(js, fmt.Sprintf("Too many units found (%d) for metadata %d", len(units), mdID))
					continue
				}
			} else {
				svc.logInfo(js, fmt.Sprintf("Metdata %d is a special case and all units will be accepted", mdID))
			}

			// In special case processing, there will be multiple units here. In the general case, just one
			log.Printf("INFO: load master files from [%d] units of metadata %d", len(units), mdID)
			for _, u := range units {
				unitIDs = append(unitIDs, u.ID)
				compressDate := u.DateDLDeliverablesReady
				if compressDate == nil {
					compressDate = u.DatePatronDeliverablesReady
				}
				if compressDate != nil {
					if latestCompressDate == nil {
						latestCompressDate = compressDate
					} else {
						if compressDate.After(*latestCompressDate) {
							latestCompressDate = compressDate
						}
					}
				}
			}

			if latestCompressDate == nil {
				svc.logError(js, fmt.Sprintf("Unabe to determine latest compression date for metadata %d; skipping", mdID))
				continue
			}

			svc.logInfo(js, fmt.Sprintf("Load master files for units [%v]", unitIDs))
			err = svc.GDB.Where("unit_id in ?", unitIDs).Find(&masterFiles).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unabe to load master files: %s", err.Error()))
				continue
			}

			if len(masterFiles) == 0 {
				svc.logError(js, fmt.Sprintf("No master files found for metadata %d", mdID))
				continue
			}

			// Setup package assembly directory; /digiserv-production/hathitrust/[barcode]
			// final package data will reside at /digiserv-production/hathitrust/[barcode].zip
			assembleDir := path.Join(svc.ProcessingDir, "hathitrust", md.Barcode)
			packageName := fmt.Sprintf("%s.zip", md.Barcode)
			packageFilename := path.Join(svc.ProcessingDir, "hathitrust", packageName)
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
				svc.logError(js, fmt.Sprintf("Unable to create package assembly directory %s: %s", assembleDir, err.Error()))
				continue
			}

			// If OCR is applicable, perform it first
			if md.OcrHint.OcrCandidate {
				svc.logInfo(js, "This unit is an OCR candidate; check master files to see if OCR needs to be done")
				doOCR := true
				for _, mf := range masterFiles {
					if mf.TranscriptionText != "" {
						svc.logInfo(js, fmt.Sprintf("Masterfile %s:%s has OCR text. Assume the whole unit has been OCR'd already", mf.PID, mf.Filename))
						doOCR = false
						break
					}
				}
				if doOCR {
					// NOTE: this works for the special case and normal case; in the normal case, units length will be 1
					for _, ocrUnit := range units {
						// note; this call will not return until all master files in the unit have OCR results
						err = svc.requestUnitOCR(js, &ocrUnit)
						if err != nil {
							svc.logError(js, fmt.Sprintf("Unable to request OCR for unit %d: %s", ocrUnit.ID, err.Error()))
							continue
						}
					}

					svc.logInfo(js, "Refreshing master file list after succesful OCR generation")
					err = svc.GDB.Where("unit_id in ?", unitIDs).Find(&masterFiles).Error
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unabe to refresh master files: %s", err.Error()))
						continue
					}
				}
			}

			// Create the package ZIP file
			svc.logInfo(js, fmt.Sprintf("Package will be generated here %s", packageFilename))
			zipFile, err := os.Create(packageFilename)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to create package zip %s: %s", packageFilename, err.Error()))
				continue
			}
			zipWriter := zip.NewWriter(zipFile)
			defer zipFile.Close()
			defer zipWriter.Close()

			// Create the checksum file; it will appended as files are processed
			checksumPath := path.Join(assembleDir, "checksum.md5")
			checksumFile, err := os.Create(checksumPath)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to create checksum file %s: %s", checksumPath, err.Error()))
				continue
			}
			checksumFile.Chmod(0666)
			defer checksumFile.Close()

			// Write the meta.yml file
			lastCaptureDate := masterFiles[len(masterFiles)-1].CreatedAt
			ymlMD5, err := svc.writeMetaYML(js, assembleDir, &lastCaptureDate, latestCompressDate)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to write meta.yml: %s", err.Error()))
				continue
			}
			checksumFile.WriteString(fmt.Sprintf("%s  meta.yml\n", ymlMD5))

			masterFileError := false
			for idx, mf := range masterFiles {
				// copy jp2 to assembly directory, then add it to the zip
				destFN := fmt.Sprintf("%08d.jp2", (idx + 1))
				jp2kInfo := svc.iiifPath(mf.PID)
				if pathExists(jp2kInfo.absolutePath) == false {
					svc.logFatal(js, fmt.Sprintf("MasterFile %s:%s is missing JP2 derivative %s", mf.PID, mf.Filename, jp2kInfo.absolutePath))
					masterFileError = true
					break
				}

				destPath := path.Join(assembleDir, destFN)
				err = copyJP2(jp2kInfo.absolutePath, destPath)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to copy %s to %s %s", jp2kInfo.absolutePath, destPath, err.Error()))
					masterFileError = true
					break
				}

				_, err := addFileToZip(packageFilename, zipWriter, assembleDir, destFN)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to add %s to zip file: %s", destPath, err.Error()))
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
						svc.logError(js, fmt.Sprintf("Unable to write OCR text to %s %s", destTxtPath, err.Error()))
						masterFileError = true
						break
					}
					_, err := addFileToZip(packageFilename, zipWriter, assembleDir, txtFileName)
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to add %s to zip file: %s", destTxtPath, err.Error()))
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

func (svc *ServiceContext) writeMetaYML(js *jobStatus, assembleDir string, digitizationDate *time.Time, compressedAt *time.Time) (string, error) {
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
