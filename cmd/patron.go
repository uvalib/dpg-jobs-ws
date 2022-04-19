package main

import (
	"archive/zip"
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

func (svc *ServiceContext) createPatronPDF(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "Setting up assemble delivery directory to be used to build the PDF...")
	assembleDir := path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", tgtUnit.ID))
	err := ensureDirExists(assembleDir, 0755)
	if err != nil {
		return err
	}

	pdfFileName := fmt.Sprintf("%d.pdf", tgtUnit.ID)
	pdfPath := path.Join(assembleDir, pdfFileName)
	os.Remove(pdfPath)

	unitDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", tgtUnit.ID))
	tifFiles, err := getTifFiles(unitDir, tgtUnit.ID)
	if err != nil {
		return fmt.Errorf("Unable to read tif files from %s: %s", unitDir, err.Error())
	}

	// process each tif one at a time. convert to scaled down jpg for PDF
	for _, tf := range tifFiles {
		svc.logInfo(js, fmt.Sprintf("Covert %s to scaled down JPG...", tf.filename))
		cmdArray := []string{"-quiet", "-resize", "1024x", "-density", "150",
			"-format", "jpg", "-path", assembleDir, fmt.Sprintf("%s[0]", tf.path)}
		_, err := exec.Command("mogrify", cmdArray...).Output()
		if err != nil {
			return fmt.Errorf("Unable to downsize %s: %s", tf.path, err.Error())
		}
	}

	jpgFiles := path.Join(assembleDir, "*.jpg")
	svc.logInfo(js, fmt.Sprintf("Covert %s to %s...", jpgFiles, pdfPath))
	cmdArray := []string{jpgFiles, pdfPath}
	_, err = exec.Command("convert", cmdArray...).Output()
	if err != nil {
		return fmt.Errorf("Unable to convert jpg files to pdf: %s", err.Error())
	}

	if pathExists(pdfPath) == false {
		return fmt.Errorf("Target PDF %s was not created", pdfPath)
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

	if tgtUnit.IntendedUse.DeliverableFormat == "jpeg" {
		suffix = ".jpg"
		addLegalNotice = true
		useID := tgtUnit.IntendedUseID

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
	} else if tgtUnit.IntendedUse.DeliverableFormat == "tiff" {
		svc.logInfo(js, "Patron deliverable is a tif file and will NOT have a watermark")
		suffix = ".tif"
	} else {
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
		if tgtUnit.IntendedUseID == 106 || tgtUnit.IntendedUseID == 104 {
			svc.logInfo(js, "Notice of private study")
			notice += "This single copy was produced for the purposes of private study, scholarship, or research pursuant to 17 USC § 107 and/or 108.\nCopyright and other legal restrictions may apply to further uses. Special Collections, University of Virginia Library."
		} else if tgtUnit.IntendedUseID == 100 {
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
		cmdArray = append(cmdArray, fmt.Sprintf("caption:%s", notice))

		// append the notoce to bottom center
		cmdArray = append(cmdArray, "-gravity", "Center", "-append")

		// add a 20 border
		cmdArray = append(cmdArray, "-bordercolor", "lightgray", "-border", "30x20")
	}

	if resample > 0 {
		cmdArray = append(cmdArray, "-resample", fmt.Sprintf("%d", resample))
	}
	cmdArray = append(cmdArray, destPath)
	_, err := exec.Command("convert", cmdArray...).Output()
	if err != nil {
		return err
	}

	svc.logInfo(js, fmt.Sprintf("Patron deliverable created at %s", destPath))
	return nil
}

func (svc *ServiceContext) zipPatronDeliverables(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, fmt.Sprintf("Zipping deliverables for unit %d", tgtUnit.ID))
	deliveryDir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", tgtUnit.OrderID))
	ensureDirExists(deliveryDir, 0755)
	assembleDir := path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", tgtUnit.ID))

	// IF OCR was requested, generate a single text file containing all of the page OCR results
	ocrFileName := ""
	if tgtUnit.OCRMasterFiles {
		ocrFileName = path.Join(assembleDir, fmt.Sprintf("%d.txt", tgtUnit.ID))
		svc.logInfo(js, fmt.Sprintf("OCR was requeseted for this unit; generate text file with OCR resuls here: %s", ocrFileName))
		ocrFile, err := os.OpenFile(ocrFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to open OCR file %s: %s", ocrFileName, err.Error()))
		} else {
			ocrWriter := bufio.NewWriter(ocrFile)
			for _, mf := range tgtUnit.MasterFiles {
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
	for _, mf := range tgtUnit.MasterFiles {
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
			defer zipFile.Close()
			zipWriter = zip.NewWriter(zipFile)
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
	zipWriter.Close()
	zipFile.Close()

	return nil
}

// add a file to the target zip and return the new zip filesize
func addFileToZip(zipFilePath string, zw *zip.Writer, filePath string, fileName string) (int64, error) {
	fileToZip, err := os.Open(path.Join(filePath, fileName))
	if err != nil {
		return 0, err
	}
	defer fileToZip.Close()
	zipFileWriter, err := zw.Create(fileName)
	if _, err := io.Copy(zipFileWriter, fileToZip); err != nil {
		return 0, err
	}
	fi, _ := os.Stat(zipFilePath)
	return fi.Size(), nil
}
