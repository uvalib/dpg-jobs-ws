package main

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-xmlfmt/xmlfmt"
)

func (svc *ServiceContext) createHathiTrustPackage(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("HathiTrust", "Metadata", mdID)
	if err != nil {
		log.Printf("ERROR: unable to create HathiTrush package job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Validate metadata record %d", mdID))
	var md metadata
	err = svc.GDB.Preload("OcrHint").Find(&md, mdID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to get metadata %d: %s", mdID, err.Error()))
		return
	}

	if md.OcrHint == nil {
		svc.logFatal(js, "Metadata is missing the required OCRHint setting")
		return
	}

	svc.logInfo(js, "Find target unit")
	var units []unit
	err = svc.GDB.Where("metadata_id=? and (intended_use_id=? or intended_use_id=?) and date_dl_deliverables_ready is not null", mdID, 110, 101).Find(&units).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to get a digitial collection building unit for metadata %d: %s", mdID, err.Error()))
		return
	}

	if len(units) > 1 {
		svc.logFatal(js, fmt.Sprintf("Too many units found (%d) for metadata %d", len(units), mdID))
		return
	}

	if len(units) == 0 {
		svc.logFatal(js, fmt.Sprintf("No units found for metadata %d", mdID))
		return
	}

	tgtUnit := units[0]
	svc.logInfo(js, fmt.Sprintf("Create HathiTrust submission package for metadata %s unit %d", md.PID, tgtUnit.ID))

	svc.logInfo(js, "Load master files for unit")
	var masterFiles []masterFile
	err = svc.GDB.Where("unit_id=?", tgtUnit.ID).Find(&masterFiles).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unabe to load master files for unit %d: %s", tgtUnit.ID, err.Error()))
		return
	}
	if len(masterFiles) == 0 {
		svc.logFatal(js, fmt.Sprintf("No master files found for unit %d", tgtUnit.ID))
		return
	}

	if md.OcrHint.OcrCandidate {
		svc.logInfo(js, "This unit is an OCR candidate; check master files to see if OCR needs to be done")
		doOCR := false
		for _, mf := range masterFiles {
			if mf.TranscriptionText == "" {
				svc.logInfo(js, fmt.Sprintf("Masterfile %s:%s has no OCR text; OCR must be run on the unit", mf.PID, mf.Filename))
				doOCR = true
				break
			}
		}
		if doOCR {
			// note; this call will not return until all master files in the unit have OCR results
			svc.logInfo(js, "Starting OCR for unit")
			err = svc.requestUnitOCR(js, &tgtUnit)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to request OCR: %s", err.Error()))
				return
			}
		} else {
			svc.logInfo(js, "OCR already exists")
		}
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered while generating bag: %v", r))
			}
		}()

		// Setup all working directories; /digiserv-production/hathitrust/[barcode]/content
		// final package data will reside in /digiserv-production/hathitrust/[barcode]
		packageDir := path.Join(svc.ProcessingDir, "hathitrust", md.Barcode)
		assembleDir := path.Join(packageDir, "content")
		packageName := fmt.Sprintf("%s.zip", md.Barcode)
		packageFilename := path.Join(packageDir, packageName)
		if pathExists(packageDir) {
			svc.logInfo(js, fmt.Sprintf("Clean up pre-existing package assembly directory %s", packageDir))
			err := os.RemoveAll(packageDir)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to cleanup prior package assembly directory %s: %s", packageDir, err.Error()))
				return
			}
		}
		err = ensureDirExists(assembleDir, 0777)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to create package / assembly directories %s: %s", assembleDir, err.Error()))
			return
		}

		// Create the pacage ZIP file
		svc.logInfo(js, fmt.Sprintf("Package will be generated here %s", packageFilename))
		zipFile, err := os.Create(packageFilename)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to create package zip %s: %s", packageFilename, err.Error()))
			return
		}
		zipWriter := zip.NewWriter(zipFile)
		defer zipFile.Close()
		defer zipWriter.Close()

		// Write the MARC XML to the package directory
		xmlMetadataFileName := path.Join(packageDir, fmt.Sprintf("%s.xml", md.Barcode))
		svc.logInfo(js, fmt.Sprintf("Get MARC metadata record and write it to %s", xmlMetadataFileName))
		marcBytes, mdErr := svc.getRequest(fmt.Sprintf("%s/api/metadata/%s?type=marc", svc.TrackSys.API, md.PID))
		if mdErr != nil {
			svc.logFatal(js, fmt.Sprintf("Get MARC metadata for %s failed: %d - %s", md.PID, mdErr.StatusCode, mdErr.Message))
			return
		}
		prettyXML := xmlfmt.FormatXML(string(marcBytes), "", "   ")
		err = os.WriteFile(xmlMetadataFileName, []byte(prettyXML), 0666)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to write MARC metadata text to %s %s", xmlMetadataFileName, err.Error()))
			return
		}

		for idx, mf := range masterFiles {
			// copy jp2 to assembly directory, then add it to the zip
			destFN := fmt.Sprintf("%08d.jp2", (idx + 1))
			jp2kInfo := svc.iiifPath(mf.PID)
			if pathExists(jp2kInfo.absolutePath) == false {
				svc.logFatal(js, fmt.Sprintf("MasterFile %s:%s is missing JP2 derivative %s", mf.PID, mf.Filename, jp2kInfo.absolutePath))
				return
			}

			destPath := path.Join(assembleDir, destFN)
			err = copyJP2(jp2kInfo.absolutePath, destPath)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to copy %s to %s %s", jp2kInfo.absolutePath, destPath, err.Error()))
				return
			}

			_, err := addFileToZip(packageFilename, zipWriter, assembleDir, destFN)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to add %s to zip file: %s", destPath, err.Error()))
				return
			}

			// if applicable, copy ocr text to the package dir. make the name match the image name
			if md.OcrHint.OcrCandidate {
				txtFileName := fmt.Sprintf("%08d.txt", (idx + 1))
				destTxtPath := path.Join(assembleDir, txtFileName)
				err = os.WriteFile(destTxtPath, []byte(mf.TranscriptionText), 0666)
				if err != nil {
					svc.logFatal(js, fmt.Sprintf("Unable to write OCR text to %s %s", destTxtPath, err.Error()))
					return
				}
				_, err := addFileToZip(packageFilename, zipWriter, assembleDir, txtFileName)
				if err != nil {
					svc.logFatal(js, fmt.Sprintf("Unable to add %s to zip file: %s", destTxtPath, err.Error()))
					return
				}
			}
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
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