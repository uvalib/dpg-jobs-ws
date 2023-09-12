package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) getOCRLanguages(c *gin.Context) {
	log.Printf("INFO: request for OCR languages")
	langs, err := ioutil.ReadFile("./assets/tesseract-langs.txt")
	if err != nil {
		log.Printf("ERROR: unable to read OCR languages: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	langList := strings.Split(string(langs), "\n")
	sort.Strings(langList)
	c.String(http.StatusOK, strings.Join(langList, ","))
}

func (svc *ServiceContext) handleOCRRequest(c *gin.Context) {
	type ocrReq struct {
		Type string `json:"type"`
		ID   int64  `json:"id"`
	}
	var req ocrReq
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: could not parse OCR request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.Type != "unit" && req.Type != "masterfile" {
		log.Printf("ERROR: unsupported OCR request type: %s", req.Type)
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a supported ocr type", req.Type))
		return
	}
	log.Printf("INFO: request for OCR on %s:%d", req.Type, req.ID)
	itemType := "Unit"
	if req.Type != "unit" {
		itemType = "MasterFile"
	}
	js, err := svc.createJobStatus("OCR", itemType, req.ID)
	if err != nil {
		log.Printf("ERROR: unable to create OCR job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	go func() {
		if req.Type == "unit" {
			var tgtUnit unit
			err = svc.GDB.Preload("Metadata").Preload("Metadata.OcrHint").First(&tgtUnit, req.ID).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", req.ID, err.Error()))
				return
			}
			err = svc.requestUnitOCR(js, tgtUnit.Metadata.PID, tgtUnit.ID, tgtUnit.Metadata.OcrLanguageHint)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to request unit OCR: %s", err.Error()))
			}
		} else {
			err = svc.requestMasterFileOCR(js, req.ID)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to request masterfile OCR: %s", err.Error()))
			}
		}
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) requestUnitOCR(js *jobStatus, metadataPID string, unitID int64, lang string) error {
	svc.logInfo(js, "Requesting OCR for unit")
	callbackURL := fmt.Sprintf("%s/callbacks/%d/ocr", svc.ServiceURL, js.ID)
	callbackURL = url.QueryEscape(callbackURL)
	ocrURL := fmt.Sprintf("%s/%s?lang=%s&unit=%d&force=true&callback=%s", svc.OcrURL, metadataPID, lang, unitID, callbackURL)
	svc.logInfo(js, fmt.Sprintf("OCR request URL: %s", ocrURL))
	_, getErr := svc.getRequest(ocrURL)
	if getErr != nil {
		return fmt.Errorf("ocr request failed %d:%s", getErr.StatusCode, getErr.Message)
	}
	svc.logInfo(js, "OCR Request successfully submitted. Awaiting results.")
	svc.OcrRequests = append(svc.OcrRequests, js.ID)
	done := false
	for done == false {
		time.Sleep(30 * time.Second)
		found := false
		for _, jsID := range svc.OcrRequests {
			if jsID == js.ID {
				found = true
				break
			}
		}
		if found == false {
			done = true
		}
	}

	svc.logInfo(js, "OCR request finished")

	return nil
}

func (svc *ServiceContext) requestMasterFileOCR(js *jobStatus, mfID int64) error {
	svc.logInfo(js, "Requesting OCR for master file")

	var tgtMF masterFile
	err := svc.GDB.First(&tgtMF, mfID).Error
	if err != nil {
		return err
	}
	var tgtMD metadata
	err = svc.GDB.Preload("OcrHint").First(&tgtMD, *tgtMF.MetadataID).Error
	if err != nil {
		return err
	}

	lang := tgtMD.OcrLanguageHint
	callbackURL := fmt.Sprintf("%s/callbacks/%d/ocr", svc.ServiceURL, js.ID)
	callbackURL = url.QueryEscape(callbackURL)
	ocrURL := fmt.Sprintf("%s/%s?lang=%s&force=true&callback=%s", svc.OcrURL, tgtMF.PID, lang, callbackURL)
	svc.logInfo(js, fmt.Sprintf("Masterfile OCR request URL: %s", ocrURL))
	_, getErr := svc.getRequest(ocrURL)
	if getErr != nil {
		return fmt.Errorf("ocr request failed %d:%s", getErr.StatusCode, getErr.Message)
	}
	svc.logInfo(js, "OCR Request successfully submitted. Awaiting results.")
	svc.OcrRequests = append(svc.OcrRequests, js.ID)
	done := false
	for done == false {
		time.Sleep(30 * time.Second)
		found := false
		for _, jsID := range svc.OcrRequests {
			if jsID == js.ID {
				found = true
				break
			}
		}
		if found == false {
			done = true
		}
	}

	svc.logInfo(js, "OCR request finished")

	return nil
}

func (svc *ServiceContext) ocrDoneCallback(c *gin.Context) {
	jobID, _ := strconv.ParseInt(c.Param("jid"), 10, 64)
	log.Printf("INFO: received ocr done callback for job %d", jobID)

	// whatever happens, clear out the pending request when done using a defer func
	defer func() {
		log.Printf("INFO: remove pending ocr job %d", jobID)
		jsIdx := -1
		for idx, jsID := range svc.OcrRequests {
			if jsID == jobID {
				jsIdx = idx
				break
			}
		}
		if jsIdx > -1 {
			svc.OcrRequests = append(svc.OcrRequests[:jsIdx], svc.OcrRequests[jsIdx+1:]...)
		} else {
			log.Printf("ERROR: could not find pending OCR job %d", jobID)
		}
	}()

	var pendingJob jobStatus
	err := svc.GDB.First(&pendingJob, jobID).Error
	if err != nil {
		log.Printf("ERROR: unable to get job status %d: %s", jobID, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	svc.logInfo(&pendingJob, "Received OCR callback")

	type ocrRespData struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	var cbResp ocrRespData
	qpErr := c.ShouldBindJSON(&cbResp)
	if qpErr != nil {
		log.Printf("ERROR: invalid OCR callback payload: %s", qpErr.Error())
		c.String(http.StatusBadRequest, qpErr.Error())
		return
	}

	if cbResp.Status == "success" {
		svc.logInfo(&pendingJob, "OCR request completed successfully")
	} else {
		svc.logInfo(&pendingJob, fmt.Sprintf("OCR request failed: %s", cbResp.Message))
	}

	c.String(http.StatusOK, "ok")
}
