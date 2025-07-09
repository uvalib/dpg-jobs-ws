package main

import (
	"archive/zip"
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (svc *ServiceContext) createPatronPDF(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "Unit requires the creation of PDF patron deliverables.")
	svc.logInfo(js, "Setting up assemble delivery directory to be used to build the PDF...")
	assembleDir := path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", tgtUnit.ID))
	err := ensureDirExists(assembleDir, 0755)
	if err != nil {
		return err
	}

	pdfFileName := fmt.Sprintf("%d.pdf", tgtUnit.ID)
	pdfPath := path.Join(assembleDir, pdfFileName)
	os.Remove(pdfPath)

	// Send to PDF WS to start creation
	key := fmt.Sprintf("%s.unit%d", tgtUnit.Metadata.PID, tgtUnit.ID)
	token := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	url := fmt.Sprintf("%s/%s?unit=%d&embed=1&token=%s", svc.PdfURL, tgtUnit.Metadata.PID, tgtUnit.ID, token)
	svc.logInfo(js, fmt.Sprintf("Request PDF with %s", url))
	_, pdfErr := svc.getRequest(url)
	if pdfErr != nil {
		return fmt.Errorf("pdf request failed: %d:%s", pdfErr.StatusCode, pdfErr.Message)
	}

	// ...poll for status
	svc.logInfo(js, "PDF generate stated; poll for status")
	done := false
	errorMsg := ""
	statusURL := fmt.Sprintf("%s/%s/status?token=%s", svc.PdfURL, tgtUnit.Metadata.PID, token)
	for done == false {
		time.Sleep(15 * time.Second)
		statusResp, pdfErr := svc.getRequest(statusURL)
		if pdfErr != nil {
			done = true
			errorMsg = fmt.Sprintf("status check failed: %d:%s", pdfErr.StatusCode, pdfErr.Message)
		} else {
			strStatus := string(statusResp)
			switch strStatus {
			case "FAILED":
				errorMsg = "PDF generation failed"
				done = true
			case "READY":
				svc.logInfo(js, "PDF generation is done")
				done = true
			}
		}
	}
	if errorMsg != "" {
		return fmt.Errorf("%s", errorMsg)
	}

	// once complete, download into pdfPath
	downloadURL := fmt.Sprintf("%s/%s/download?token=%s", svc.PdfURL, tgtUnit.Metadata.PID, token)
	svc.logInfo(js, fmt.Sprintf("Download PDF from %s to %s", downloadURL, pdfPath))
	err = downloadPDF(downloadURL, pdfPath)
	if err != nil {
		return err
	}

	// Zip the PDF into the delivery directory
	deliveryDir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", tgtUnit.OrderID))
	ensureDirExists(deliveryDir, 0755)
	zipFile := path.Join(deliveryDir, fmt.Sprintf("%d.zip", tgtUnit.ID))
	svc.logInfo(js, fmt.Sprintf("Zip PDF to %s", zipFile))
	os.Remove(zipFile)

	zf, err := os.Create(zipFile)
	if err != nil {
		return fmt.Errorf("Unable to create zip file: %s", err.Error())
	}
	defer zf.Close()
	zipWriter := zip.NewWriter(zf)
	_, err = addFileToZip(zipFile, zipWriter, assembleDir, pdfFileName)
	if err != nil {
		return err
	}
	zipWriter.Close()

	svc.logInfo(js, "Zip deliverable of PDF created.")
	return nil
}

func downloadPDF(url string, pdfPath string) error {
	out, err := os.Create(pdfPath)
	if err != nil {
		return fmt.Errorf("unable to create %s: %s", pdfPath, err.Error())
	}
	defer out.Close()

	pdfResp, dlErr := http.Get(url)
	if dlErr != nil {
		return fmt.Errorf("download request failed: %s", dlErr.Error())
	}
	defer pdfResp.Body.Close()
	_, err = io.Copy(out, pdfResp.Body)
	if err != nil {
		return fmt.Errorf("download failed: %s", err.Error())
	}
	return nil
}

func (svc *ServiceContext) createPatronDeliverable(js *jobStatus, tgtUnit *unit, mf *masterFile, mfPath, destDir, callNumber, location string) error {
	svc.logInfo(js, fmt.Sprintf("Create patron deliverable for %s", mf.Filename))
	suffix := ""
	actualRes := mf.ImageTechMeta.Resolution
	desiredRes := tgtUnit.IntendedUse.DeliverableResolution
	addLegalNotice := false
	resample, _ := strconv.Atoi(desiredRes)
	if desiredRes == "300" && actualRes == 0 {
		return fmt.Errorf("actual_resolution is required when desired_resolution is specified")
	}

	switch tgtUnit.IntendedUse.DeliverableFormat {
	case "jpeg":
		suffix = ".jpg"
		addLegalNotice = true
		useID := *tgtUnit.IntendedUseID

		// New from Brandon; web publication and online exhibits don't need watermarks
		if tgtUnit.Metadata.IsPersonalItem || tgtUnit.RemoveWatermark || useID == 103 || useID == 109 {
			addLegalNotice = false
			svc.logInfo(js, "Patron deliverable is a jpg file and will NOT a watermark")
			svc.logInfo(js, "One of the following is the reason:")
			svc.logInfo(js, fmt.Sprintf("personal_item: %t, remove_watermark: %t, use_id: %d = 103/109",
				tgtUnit.Metadata.IsPersonalItem, tgtUnit.RemoveWatermark, useID))
		} else {
			svc.logInfo(js, "Patron deliverable is a jpg file and will have a watermark")
		}
	case "tiff":
		svc.logInfo(js, "Patron deliverable is a tif file and will NOT have a watermark")
		suffix = ".tif"
	default:
		return fmt.Errorf("unknown deliverable format %s", tgtUnit.IntendedUse.DeliverableFormat)
	}

	// format output path so it includes order number and unit number, like so: .../order123/54321/...
	baseFN := strings.TrimSuffix(mf.Filename, filepath.Ext(mf.Filename))
	destPath := path.Join(destDir, fmt.Sprintf("%s%s", baseFN, suffix))
	if pathExists(destPath) {
		svc.logInfo(js, fmt.Sprintf("Deliverable already exists at %s", destPath))
		return nil
	}

	// Simple case; just a copy of tif at full resolution. No imagemagick needed
	if suffix == ".tif" && (desiredRes == "" || desiredRes == "Highest Possible") {
		_, err := copyFile(mfPath, destPath, 0664)
		return err
	}

	cmdArray := []string{fmt.Sprintf("%s[0]", mfPath)}
	if addLegalNotice {
		svc.logInfo(js, "Adding legal notice")
		notice := ""
		if len(tgtUnit.Metadata.Title) < 145 {
			notice += fmt.Sprintf("Title: %s\n", tgtUnit.Metadata.Title)
		} else {
			notice += fmt.Sprintf("Title: %s\n", tgtUnit.Metadata.Title[0:145])
		}
		if callNumber != "" {
			notice += fmt.Sprintf("Call Number: %s\n", callNumber)
		}
		if location != "" {
			notice += fmt.Sprintf("Location: %s\n", location)
		}

		// notice for personal research or presentation
		switch *tgtUnit.IntendedUseID {
		case 106, 104:
			svc.logInfo(js, "Notice of private study")
			notice += "This single copy was produced for the purposes of private study, scholarship, or research pursuant to 17 USC § 107 and/or 108.\nCopyright and other legal restrictions may apply to further uses. Special Collections, University of Virginia Library."
		case 100:
			svc.logInfo(js, "Notice of classroom teaching")
			notice += "This single copy was produced for the purposes of classroom teaching pursuant to 17 USC § 107 (fair use).\nCopyright and other legal restrictions may apply to further uses. Special Collections, University of Virginia Library."
		}

		pointSize := float64(mf.ImageTechMeta.Width) * 0.015
		if pointSize < 22 {
			pointSize = 22
		}

		// text size, center justify with colored background
		cmdArray = append(cmdArray, "-bordercolor", "lightgray", "-border", "0x10")
		cmdArray = append(cmdArray, "-pointsize", fmt.Sprintf("%.2f", pointSize), "-size", fmt.Sprintf("%dx", mf.ImageTechMeta.Width))
		cmdArray = append(cmdArray, "-background", "lightgray", "-gravity", "center")
		cmdArray = append(cmdArray, fmt.Sprintf("caption:\"%s\"", notice))

		// append the notoce to bottom center
		cmdArray = append(cmdArray, "-gravity", "Center", "-append")

		// add a 20 border
		cmdArray = append(cmdArray, "-bordercolor", "lightgray", "-border", "30x20")
	}

	if resample > 0 {
		cmdArray = append(cmdArray, "-resample", fmt.Sprintf("%d", resample))
	}
	cmdArray = append(cmdArray, destPath)
	cmd := exec.Command("magick", cmdArray...)
	svc.logInfo(js, fmt.Sprintf("%+v", cmd))
	cmdOut, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("ERROR: convert command failed: %s - %s", err.Error(), cmdOut)
		return err
	}

	svc.logInfo(js, fmt.Sprintf("Patron deliverable created at %s", destPath))
	return nil
}

func (svc *ServiceContext) zipPatronDeliverables(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, fmt.Sprintf("Zipping deliverables for unit %d", tgtUnit.ID))
	deliveryDir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", tgtUnit.OrderID))
	err := ensureDirExists(deliveryDir, 0755)
	if err != nil {
		log.Printf("ERROR: unable to create delivery dir %s: %s", deliveryDir, err.Error())
		return err
	}
	assembleDir := path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", tgtUnit.ID))

	svc.logInfo(js, "Getting up-to-date list of master files for unit")
	var masterfiles []masterFile
	err = svc.GDB.Where("unit_id=?", tgtUnit.ID).Find(&masterfiles).Error
	if err != nil {
		return fmt.Errorf("Unable to get masterfiles: %s", err.Error())
	}

	// IF OCR was requested, generate a single text file containing all of the page OCR results
	ocrFileName := ""
	if tgtUnit.OcrMasterFiles {
		ocrFileName = path.Join(assembleDir, fmt.Sprintf("%d.txt", tgtUnit.ID))
		svc.logInfo(js, fmt.Sprintf("OCR was requeseted for this unit; generate text file with OCR resuls here: %s", ocrFileName))
		ocrFile, err := os.OpenFile(ocrFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to open OCR file %s: %s", ocrFileName, err.Error()))
		} else {
			ocrWriter := bufio.NewWriter(ocrFile)
			for _, mf := range masterfiles {
				ocrWriter.WriteString(fmt.Sprintf("%s\n", mf.Filename))
				ocrWriter.WriteString(fmt.Sprintf("%s\n", mf.TranscriptionText))
				svc.logInfo(js, fmt.Sprintf("Added OCR results for master file %s", mf.Filename))
			}
			ocrWriter.Flush()
			ocrFile.Close()
		}
	}

	var zipSize int64
	zipNum := 0
	zipFN := ""
	var zipFile *os.File
	var zipWriter *zip.Writer
	for _, mf := range masterfiles {
		// max filesize is 2GB
		if zipSize == 0 || zipSize > int64(2.0*1024.0*1024.0*1024.0) {
			zipNum++
			if zipSize > 0 {
				zipWriter.Close()
				zipFile.Close()
				zipSize = 0
				svc.logInfo(js, fmt.Sprintf("Zip 2GB max filesize exceeded; creating zip #%d", zipNum))
			}

			zipFN = path.Join(deliveryDir, fmt.Sprintf("%d_%d.zip", tgtUnit.ID, zipNum))
			svc.logInfo(js, fmt.Sprintf("Create %s...", zipFN))
			os.Remove(zipFN)
			zipFile, err := os.Create(zipFN)
			if err != nil {
				return fmt.Errorf("Unable to create zip file: %s", err.Error())
			}
			zipWriter = zip.NewWriter(zipFile)
			defer zipFile.Close()
			defer zipWriter.Close()
		}

		deliverableFN := mf.Filename
		if tgtUnit.IntendedUse.DeliverableFormat == "jpeg" {
			baseFN := strings.TrimSuffix(mf.Filename, filepath.Ext(mf.Filename))
			deliverableFN = baseFN + ".jpg"
		}
		svc.logInfo(js, fmt.Sprintf("Add %s to %s", deliverableFN, zipFN))
		newZipSize, err := addFileToZip(zipFN, zipWriter, assembleDir, deliverableFN)
		if err != nil {
			return err
		}
		zipSize = newZipSize
	}

	return nil
}
