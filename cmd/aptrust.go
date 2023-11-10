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
	StorageOption    string `json:"storage_option"`
	Note             string `json:"note"`
	Status           string `json:"status"`
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

	// See if therre is a submit record. Note: use limit 1 and find to avoid throwing an error for
	// the normal condition of no record being present for the first submit attempt,
	var aptSubmission apTrustSubmission
	err = svc.GDB.Where("metadata_id=?", md.ID).Limit(1).Find(&aptSubmission).Error
	if err != nil {
		log.Printf("ERROR: unable to get submission info metadata %d: %s", md.ID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	// if ID is zeo, there is no submission record so the item has not yet been submitted, create one now
	if aptSubmission.ID == 0 {
		aptSubmission = apTrustSubmission{MetadataID: md.ID, Bag: getBagFileName(&md), RequestedAt: time.Now()}
		err = svc.GDB.Create(&aptSubmission).Error
		if err != nil {
			log.Printf("ERROR: unable to create submission record for metadatata %d: %s", md.ID, err.Error())
			c.String(http.StatusInternalServerError, fmt.Sprintf("unable to create submission record: %s", err.Error()))
			return
		}
	} else {
		// a submission record exists. See if it is in a processing state and fail if it is
		aptStatus, err := svc.getAPTrustStatus(&md)
		if err != nil {
			log.Printf("ERROR: unable to check aptstatus  metadatata %d: %s", md.ID, err.Error())
			c.String(http.StatusInternalServerError, fmt.Sprintf("unable to check staatus: %s", err.Error()))
			return
		}
		if aptStatus.Count > 0 {
			statusRec := aptStatus.Results[0]
			if statusRec.Status != "Failed" && statusRec.Status != "Canceled" {
				log.Printf("ERROR: aptrust submission is already in progress for metadatata %d; status %s", md.ID, statusRec.Status)
				c.String(http.StatusBadRequest, "aptrust submission is already in progress")
				return
			}
		}
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
			svc.logFatal(js, fmt.Sprintf("APTrust submission failed: %s", err.Error()))
			return
		}
		svc.logInfo(js, fmt.Sprintf("Baggit tar file created here: %s; validate it...", bagFile))

		cmd := exec.Command("apt-cmd", "bag", "validate", "-p", "aptrust", bagFile)
		svc.logInfo(js, fmt.Sprintf("validate command: %+v", cmd))
		aptOut, err := cmd.CombinedOutput()
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Validate %s failed: %s", bagFile, aptOut))
			return
		}

		svc.logInfo(js, "Submit bag to APTrust S3 bucket...")
		submitTime := time.Now()
		aptSubmission.SubmittedAt = &submitTime
		svc.GDB.Save(&aptSubmission)
		cmd = exec.Command("apt-cmd", "s3", "upload", fmt.Sprintf("--host=%s", svc.APTrust.AWSHost), fmt.Sprintf("--bucket=%s", svc.APTrust.AWSBucket), bagFile)
		svc.logInfo(js, fmt.Sprintf("submit command: %+v", cmd))
		aptOut, err = cmd.CombinedOutput()
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Validate %s failed: %s", bagFile, aptOut))
			return
		}

		svc.logInfo(js, fmt.Sprintf("%s has been submitted to APTrust; awaiting completion", bagFile))
		done := false
		for done == false {
			time.Sleep(1 * time.Minute)
			jsonResp, err := svc.getAPTrustStatus(&md)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Status check failed: %s", err.Error()))
				done = true
			} else {
				if jsonResp.Count > 0 {
					itemStatus := jsonResp.Results[0]
					if itemStatus.Status == "Failed" {
						svc.logFatal(js, fmt.Sprintf("Submission failed: %s", itemStatus.Note))
						done = true
					} else if itemStatus.Status == "Success" {
						svc.logInfo(js, "Submission successful")
						done = true
					} else if itemStatus.Status == "Canceled" || itemStatus.Status == "Suspended" {
						svc.logFatal(js, fmt.Sprintf("Submission was canceled or suspended: %s", itemStatus.Note))
						done = true
					}
				}
			}
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) apTrustStatusRequest(c *gin.Context) {
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

	jsonResp, err := svc.getAPTrustStatus(&md)
	if err != nil {
		log.Printf("ERROR: aptrust status request for metadata %d failed: %s", md.ID, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("INFO: raw aptrust response: %+v", jsonResp)

	if jsonResp.Count == 0 {
		log.Printf("INFO: metadata %d has no aptrust status", md.ID)
		c.String(http.StatusNotFound, fmt.Sprintf("%d has no aptrust status", md.ID))
	} else {
		// always return the last status as it will be the most recent
		c.JSON(http.StatusOK, jsonResp.Results[0])
	}
}

func (svc *ServiceContext) getAPTrustStatus(md *metadata) (*apTrustResponse, error) {
	cmd := exec.Command("apt-cmd", "registry", "list", "workitems", fmt.Sprintf("name=%s", getBagFileName(md)), "sort=date_processed__desc")
	log.Printf("INFO: aptrust command: %+v", cmd)
	aptOut, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf(string(aptOut))
	}

	var jsonResp apTrustResponse
	err = json.Unmarshal(aptOut, &jsonResp)
	if err != nil {
		return nil, fmt.Errorf("malformed response: %s", err.Error())
	}
	return &jsonResp, nil
}
