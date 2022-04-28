package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) requestOCR(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "Requesting OCR for unit")
	lang := tgtUnit.Metadata.OcrLanguageHint
	callbackURL := fmt.Sprintf("%s/callbacks/%d/ocr", svc.ServiceURL, js.ID)
	callbackURL = url.QueryEscape(callbackURL)
	ocrURL := fmt.Sprintf("%s/%s?lang=%s&unit=%d&force=true&callback=%s", svc.OcrURL, tgtUnit.Metadata.PID, lang, tgtUnit.ID, callbackURL)
	svc.logInfo(js, fmt.Sprintf("OCR request URL: %s", ocrURL))
	_, getErr := svc.sendRequest("GET", ocrURL)
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
	defer func() {
		// whatever happens, clear out the pending request when done
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
			log.Printf("ERROR: could npt find pending OCR job %d", jobID)
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
		JSON struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"json"`
	}
	var cbResp ocrRespData
	qpErr := c.ShouldBindJSON(&cbResp)
	if qpErr != nil {
		log.Printf("ERROR: invalid OCR callback payload: %s", qpErr.Error())
		c.String(http.StatusBadRequest, qpErr.Error())
		return
	}

	if cbResp.JSON.Status == "success" {
		svc.logInfo(&pendingJob, "OCR request completed successfully")
	} else {
		svc.logInfo(&pendingJob, fmt.Sprintf("OCR request failed: %s", cbResp.JSON.Message))
	}

	c.String(http.StatusOK, "ok")
}
