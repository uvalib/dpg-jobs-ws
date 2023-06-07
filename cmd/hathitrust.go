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

type hathiTrustRequest struct {
	ComputeID   string  `json:"computeID"`
	MetadataIDs []int64 `json:"records"`
	OrderID     int64   `json:"order"`
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
	if len(req.MetadataIDs) == 0 && req.OrderID == 0 {
		log.Printf("INFO: hathitrust metadata request requires order id or metadata ids")
		c.String(http.StatusBadRequest, "order id or metadata id list is required")
		return
	}

	var submitUser staffMember
	err = svc.GDB.Where("computing_id=?", req.ComputeID).First(&submitUser).Error
	if err != nil {
		log.Printf("ERROR: user %s not found: %s", req.ComputeID, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if submitUser.Role != 0 {
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
	if req.OrderID > 0 {
		err = svc.GDB.Raw("select metadata_id from units where order_id=? and unit_status != ?", req.OrderID, "canceled").Scan(&req.MetadataIDs).Error
		if err != nil {
			log.Printf("ERROR: unable to get metadata ids for order %d: %s", req.OrderID, err.Error())
			c.String(http.StatusInternalServerError, fmt.Sprintf("uable to get metadata ids for order: %s", err.Error()))
			return
		}
		submissionInfo = fmt.Sprintf("for order %d with %d metadata records", req.OrderID, len(req.MetadataIDs))
	}

	svc.logInfo(js, fmt.Sprintf("%s requests %s hathitrust metadata submission %s", req.ComputeID, req.Mode, submissionInfo))

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered while submitting hathitrust metadata: %v", r))
			}
		}()

		svc.logInfo(js, fmt.Sprintf("connecting to ftps server %s as %s", svc.HathiTrust.FTPS, svc.HathiTrust.User))
		ftpsCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
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
		if req.Mode == "test" {
			uploadDirectory = "testrecs"
		}
		svc.logInfo(js, fmt.Sprintf("Set FTPS working directory to%s", uploadDirectory))
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
		recordCnt := 0
		for _, mdID := range req.MetadataIDs {
			var tgtMD metadata
			err = svc.GDB.Find(&tgtMD, mdID).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to load metadata %d: %s", mdID, err.Error()))
				continue
			}

			xml, err := svc.getMARCMetadata(tgtMD)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to retrieve MARC XML for %d: %s", mdID, err.Error()))
				continue
			}
			metadataOut += fmt.Sprintf("\n%s", xml)
			recordCnt++
		}
		metadataOut += "\n</collection>"

		if recordCnt > 0 {
			if req.Mode == "dev" {
				svc.logInfo(js, metadataOut)
			} else {
				svc.logInfo(js, fmt.Sprintf("Upload %d MARC records with total size %d to FTPS %s as %s", recordCnt, len(metadataOut), svc.HathiTrust.FTPS, uploadFN))
				err = ftpsConn.Upload(ftpsCtx, uploadFN, strings.NewReader(metadataOut))
				if err != nil {
					svc.logFatal(js, fmt.Sprintf("upload failed: %s", err.Error()))
					return
				}
			}

			svc.sendHathiTrustUploadEmail(uploadFN, len(metadataOut), recordCnt)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to send email to HathiTrust: %s", err.Error()))
				return
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
	if len(req.MetadataIDs) == 0 && req.OrderID == 0 {
		log.Printf("INFO: hathitrust package request requires order id or metadata ids")
		c.String(http.StatusBadRequest, "order id or metadata id list is required")
		return
	}

	var submitUser staffMember
	err = svc.GDB.Where("computing_id=?", req.ComputeID).First(&submitUser).Error
	if err != nil {
		log.Printf("ERROR: user %s not found: %s", req.ComputeID, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if submitUser.Role != 0 {
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
	if req.OrderID > 0 {
		err = svc.GDB.Raw("select metadata_id from units where order_id=? and unit_status != ?", req.OrderID, "canceled").Scan(&req.MetadataIDs).Error
		if err != nil {
			log.Printf("ERROR: unable to get metadata ids for order %d: %s", req.OrderID, err.Error())
			c.String(http.StatusInternalServerError, fmt.Sprintf("uable to get metadata ids for order: %s", err.Error()))
			return
		}
		submissionInfo = fmt.Sprintf("for order %d with %d metadata records", req.OrderID, len(req.MetadataIDs))
	}

	svc.logInfo(js, fmt.Sprintf("%s requests hathitrust package generation %s", req.ComputeID, submissionInfo))

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered while generating bag: %v", r))
			}
		}()

		for _, mdID := range req.MetadataIDs {
			svc.logInfo(js, fmt.Sprintf("Validate metadata record %d", mdID))
			var md metadata
			err = svc.GDB.Preload("OcrHint").Find(&md, mdID).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to get metadata %d: %s", mdID, err.Error()))
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

			if len(units) > 1 {
				svc.logError(js, fmt.Sprintf("Too many units found (%d) for metadata %d", len(units), mdID))
				continue
			}

			if len(units) == 0 {
				svc.logError(js, fmt.Sprintf("No units found for metadata %d", mdID))
				continue
			}

			tgtUnit := units[0]
			tgtUnit.Metadata = &md
			svc.logInfo(js, fmt.Sprintf("Create HathiTrust submission package for metadata %s unit %d", md.PID, tgtUnit.ID))

			svc.logInfo(js, "Load master files for unit")
			var masterFiles []masterFile
			err = svc.GDB.Where("unit_id=?", tgtUnit.ID).Find(&masterFiles).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unabe to load master files for unit %d: %s", tgtUnit.ID, err.Error()))
				continue
			}
			if len(masterFiles) == 0 {
				svc.logError(js, fmt.Sprintf("No master files found for unit %d", tgtUnit.ID))
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
					// note; this call will not return until all master files in the unit have OCR results
					err = svc.requestUnitOCR(js, &tgtUnit)
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to request OCR: %s", err.Error()))
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
			ymlMD5, err := svc.writeMetaYML(js, assembleDir, &lastCaptureDate, tgtUnit.DateDLDeliverablesReady)
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
			zipWriter.Close()
			zipFile.Close()

			svc.logInfo(js, fmt.Sprintf("%s generated. Cleaning up assembly directory %s", packageFilename, assembleDir))
			err = os.RemoveAll(assembleDir)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to clean up assembly directory: %s", err.Error()))
			}
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) getMARCMetadata(md metadata) (string, error) {
	marcBytes, mdErr := svc.getRequest(fmt.Sprintf("%s/api/metadata/%s?type=marc", svc.TrackSys.API, md.PID))
	if mdErr != nil {
		return "", fmt.Errorf("%d:%s", mdErr.StatusCode, mdErr.Message)
	}
	marcStr := string(marcBytes)
	idx := strings.Index(marcStr, "<record>")
	marcStr = marcStr[idx:len(marcStr)]
	idx = strings.Index(marcStr, "</collection>")
	marcStr = marcStr[:idx]
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
