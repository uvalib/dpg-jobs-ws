package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type apTrustResult struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	ETag             string `json:"etag"`
	ObjectIdentifier string `json:"object_identifier"`
	AltIdentifier    string `json:"alt_identifier"`
	StorageOption    string `json:"storage"`
	Note             string `json:"note"`
	Status           string `json:"status"`
	QueuedAt         string `json:"queued_at"`
	ProcessedAt      string `json:"date_processed"`
}

type apTrustResponse struct {
	Count   int64           `json:"count"`
	Results []apTrustResult `json:"results,omitempty"`
}

func (svc *ServiceContext) submitToAPTrust(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	if mdID == 0 {
		log.Printf("INFO: invalid id %s passed to aptrust request", c.Param("id"))
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid metadata id", c.Param("id")))
		return
	}

	var md metadata
	err := svc.GDB.Joins("APTrustStatus").Joins("PreservationTier").Find(&md, mdID).Error
	if err != nil {
		log.Printf("ERROR: unable to load metadata %d: %s", mdID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if md.PreservationTierID < 2 {
		log.Printf("INFO: metadata %d has not been flagged for aptrust", md.ID)
		c.String(http.StatusBadRequest, fmt.Sprintf("metadata %d has not been assigned for aptrust preservation", md.ID))
		return
	}

	if md.APTrustStatus != nil {
		// allow failed submissions to be retried
		log.Printf("INFO: prior status eists with status [%s]", md.APTrustStatus.Status)
		if md.APTrustStatus.Status != "Failed" {
			log.Printf("ERROR: request aptrust submission for metadata %d that already has been submitted", md.ID)
			c.String(http.StatusBadRequest, "this item has already been submitted to aptrust")
			return
		}
	} else {
		aptStatus := apTrustStatus{MetadataID: md.ID, Status: "Baggit", Note: "Bagging in process", SubmittedAt: time.Now()}
		err = svc.GDB.Create(&aptStatus).Error
		if err != nil {
			log.Printf("ERROR: unable to create aptrust status for metadata %d: %s", md.ID, err.Error())
			c.String(http.StatusBadRequest, "this item has already been submitted to aptrust")
			return
		}
		md.APTrustStatus = &aptStatus
	}

	js, err := svc.createJobStatus("APTrustSubmit", "Metadata", md.ID)
	if err != nil {
		log.Printf("ERROR: unable to create aptrust submission job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered during APTrust submission: %v", r))
			}
		}()

		bagFile, err := svc.createBag(js, &md)
		if err != nil {
			svc.updateAPTrustStatusRecord(js, md.APTrustStatus, "Failed", fmt.Sprintf("Baggit failed: %s", err.Error()))
			svc.logFatal(js, fmt.Sprintf("APTrust submission failed: %s", err.Error()))
			return
		}
		svc.logInfo(js, fmt.Sprintf("Baggit tar file created here: %s; validate it..", bagFile))

		cmd := exec.Command("apt-cmd", "bag", "validate", "-p", "aptrust", bagFile)
		svc.logInfo(js, fmt.Sprintf("validate command: %+v", cmd))
		aptOut, err := cmd.CombinedOutput()
		if err != nil {
			svc.updateAPTrustStatusRecord(js, md.APTrustStatus, "Failed", fmt.Sprintf("Bag validation failed: %s", err.Error()))
			svc.logFatal(js, fmt.Sprintf("Validate %s failed: %s", bagFile, aptOut))
			return
		}

		svc.logInfo(js, "Submit bag to APTrust S3 bucket...")
		svc.updateAPTrustStatusRecord(js, md.APTrustStatus, "Submit", "Submitting bag to S3 receiving bucket")
		cmd = exec.Command("apt-cmd", "s3", "upload", fmt.Sprintf("--host=%s", svc.APTrust.AWSHost), fmt.Sprintf("--bucket=%s", svc.APTrust.AWSBucket), bagFile)
		svc.logInfo(js, fmt.Sprintf("submit command: %+v", cmd))
		aptOut, err = cmd.CombinedOutput()
		if err != nil {
			svc.updateAPTrustStatusRecord(js, md.APTrustStatus, "Failed", fmt.Sprintf("Bag submission failed: %s", err.Error()))
			svc.logFatal(js, fmt.Sprintf("Validate %s failed: %s", bagFile, aptOut))
			return
		}

		svc.logInfo(js, fmt.Sprintf("%s has been submitted to APTrust. Status will be updated when the metadata page is refreshed. Ingest may take several hours.", bagFile))

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) updateAPTrustStatusRecord(js *jobStatus, aptStatus *apTrustStatus, status string, note string) {
	aptStatus.Status = status
	aptStatus.Note = note
	err := svc.GDB.Save(aptStatus).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to update APTRust status: %s", err.Error()))
	}
}

func (svc *ServiceContext) getAPTrustStatus(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	if mdID == 0 {
		log.Printf("INFO: invalid id %s passed to aptrust request", c.Param("id"))
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid metadata id", c.Param("id")))
		return
	}

	var md metadata
	err := svc.GDB.Joins("PreservationTier").Find(&md, mdID).Error
	if err != nil {
		log.Printf("ERROR: unable to load metadata %d: %s", mdID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if md.PreservationTierID < 2 {
		log.Printf("INFO: metadata %d has not been flagged for aptrust", md.ID)
		c.String(http.StatusBadRequest, fmt.Sprintf("metadata %d has not been assigned for aptrust preservation", md.ID))
		return
	}

	objType := "sirsimetadata"
	if md.Type == "XmlMetadata" {
		objType = "xmlmetadata"
	}
	aptName := fmt.Sprintf("virginia.edu.tracksys-%s-%d.tar", objType, mdID)
	cmd := exec.Command("apt-cmd", "registry", "list", "workitems", fmt.Sprintf("name=%s", aptName), "sort=date_processed__desc")
	log.Printf("INFO: aptrust command: %+v", cmd)
	aptOut, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("ERROR: aptrust request failed: %s", aptOut)
		c.String(http.StatusInternalServerError, string(aptOut))
		return
	}
	log.Printf("INFO: raw aptrust response: %s", aptOut)

	var jsonResp apTrustResponse
	err = json.Unmarshal(aptOut, &jsonResp)
	if err != nil {
		log.Printf("ERROR: unable to parse response: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if jsonResp.Count == 0 {
		log.Printf("INFO: metadata %d has no aptrust status", md.ID)
		c.String(http.StatusNotFound, fmt.Sprintf("%d has no aptrust status", md.ID))
	} else {
		// always return the last status as it will be the most recent
		c.JSON(http.StatusOK, jsonResp.Results[0])
	}
}
