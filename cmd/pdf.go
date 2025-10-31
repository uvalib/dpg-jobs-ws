package main

import (
	"archive/zip"
	"bufio"
	"crypto/md5"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) getUnitPDFBundle(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("CreatePDFBundle", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create CreatePDFBundle job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	svc.logInfo(js, fmt.Sprintf("Generate pdf bundle for unit %d", unitID))

	assembleDir := path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("pdf_%d", unitID))
	svc.logInfo(js, fmt.Sprintf("Create pdf assembly directory: %s", assembleDir))
	if pathExists(assembleDir) {
		if err := os.RemoveAll(assembleDir); err != nil {
			svc.logError(js, fmt.Sprintf("Unable to delete existing assembly directory %s: %s", assembleDir, err.Error()))
		}
	}
	if err := ensureDirExists(assembleDir, 0777); err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to create pdf assembly dir: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var metadataPIDs []string
	mfQ := "select distinct m.pid from master_files f inner join metadata m on m.id = metadata_id where unit_id=?"
	if dbErr := svc.GDB.Raw(mfQ, unitID).Scan(&metadataPIDs).Error; dbErr != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to get metadata records for unit %d: %s", unitID, dbErr.Error()))
		c.String(http.StatusInternalServerError, dbErr.Error())
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered during pdf bundle generation for unit %d: %v", unitID, r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("%v", r))
			}
		}()

		svc.logInfo(js, fmt.Sprintf("Generate pdf files for metadata records [%v]", metadataPIDs))
		for _, mdPID := range metadataPIDs {
			svc.logInfo(js, fmt.Sprintf("Request pdf for metadata %s", mdPID))
			if err := svc.requestMetadataPDF(js, unitID, mdPID, assembleDir); err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to get metadata %s pdf: %s", mdPID, err.Error()))
				return
			}
		}

		if err := svc.createPDFBundle(js, unitID, assembleDir); err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to create zip bundle for unit %d: %s", unitID, err.Error()))
			return
		}

		svc.logInfo(js, "Bundle created, cleaning up assembly directory")
		if err := os.RemoveAll(assembleDir); err != nil {
			svc.logInfo(js, fmt.Sprintf("Unable to remove assembly directory %s: %s", assembleDir, err.Error()))
		}

		dlLink := fmt.Sprintf("https://digiservdelivery.lib.virginia.edu/pdf_%d.zip", unitID)
		svc.logInfo(js, fmt.Sprintf("Bundle can be downloaded from <a href=\"%s\">%s</a>", dlLink, dlLink))
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) requestMetadataPDF(js *jobStatus, unitID int64, mdPID, pdfDir string) error {
	url := fmt.Sprintf("%s/%s?unit=%d&embed=1", svc.PdfURL, mdPID, unitID)
	token := fmt.Sprintf("%x", md5.Sum([]byte(mdPID)))
	url += fmt.Sprintf("&token=%s", token)
	_, err := svc.getRequest(url)
	if err != nil {
		return fmt.Errorf("%s", err.Message)
	}

	svc.logInfo(js, "Await pdf completion")
	url = fmt.Sprintf("%s/%s/status?token=%s", svc.PdfURL, mdPID, token)
	pdfReady := false
	for pdfReady == false {
		// responses: READY, FAILED, PROCESSING, percentage% (includes the percent symbol)
		resp, err := svc.getRequest(url)
		if err != nil {
			return fmt.Errorf("%s", err.Message)
		}
		strResp := string(resp)
		if strResp == "FAILED" {
			return fmt.Errorf("pdf generate for %s failed", mdPID)
		}
		if strResp == "READY" {
			pdfReady = true
		}

		time.Sleep(5 * time.Second)
	}

	svc.logInfo(js, "PDF generation complete; request download")
	// only way here is if the pdf is ready; download it to the pdf assembly dir
	url = fmt.Sprintf("%s/%s/download?token=%s", svc.PdfURL, mdPID, token)
	pdfResp, rawErr := http.Get(url)
	if rawErr != nil {
		return rawErr
	}

	cleanPID := strings.ReplaceAll(mdPID, ":", "_")
	pdfFilePath := path.Join(pdfDir, fmt.Sprintf("%s.pdf", cleanPID))
	svc.logInfo(js, fmt.Sprintf("Write pdf to %s", pdfFilePath))
	pdfFile, fErr := os.Create(pdfFilePath)
	if fErr != nil {
		return fmt.Errorf("unable to create %s: %s", pdfFilePath, fErr.Error())
	}
	pdfWriter := bufio.NewWriter(pdfFile)
	pdfReader := bufio.NewReader(pdfResp.Body)
	defer pdfResp.Body.Close()
	_, wErr := pdfReader.WriteTo(pdfWriter)
	if wErr != nil {
		return fmt.Errorf("unable to write to %s: %s", pdfFilePath, wErr.Error())
	}

	return nil
}

func (svc *ServiceContext) createPDFBundle(js *jobStatus, unitID int64, assembleDir string) error {
	zipPath := path.Join(svc.DeliveryDir, fmt.Sprintf("pdf_%d.zip", unitID))
	os.Remove(zipPath)
	svc.logInfo(js, fmt.Sprintf("Create zipped pdf bundle in %s", zipPath))

	zipFile, err := os.Create(zipPath)
	if err != nil {
		return fmt.Errorf("unable to create zip file: %s", err.Error())
	}
	zipWriter := zip.NewWriter(zipFile)
	defer zipFile.Close()
	defer zipWriter.Close()

	zErr := filepath.WalkDir(assembleDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(entry.Name()) != ".pdf" {
			return nil
		}

		_, err = addFileToZip(zipPath, zipWriter, assembleDir, entry.Name())
		if err != nil {
			return err
		}
		return nil
	})

	if zErr != nil {
		return zErr
	}

	return nil
}
