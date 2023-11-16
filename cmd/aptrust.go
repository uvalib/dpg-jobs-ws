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

	log.Printf("INFO: prepare metadata %d for aptrust submission", mdID)
	tgtMD, err := svc.prepareAPTrustSubmission(mdID)
	if err != nil {
		log.Printf("ERROR: aptrust submission request failed: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	js, err := svc.createJobStatus("APTrustSubmit", "Metadata", tgtMD.ID)
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

		if tgtMD.IsCollection {
			svc.logInfo(js, fmt.Sprintf("Metadata %d is a collection; load child record IDs for APTrust submission", tgtMD.ID))
			var inCollectionIDs []int64
			err = svc.GDB.Raw("select id from metadata where parent_metadata_id=?", tgtMD.ID).Scan(&inCollectionIDs).Error
			if err != nil {
				svc.setAPTrustProcessedStatus(tgtMD.APTrustSubmission, false)
				svc.logFatal(js, fmt.Sprintf("Unable to load child metadata records for collection %d: %s", tgtMD.ID, err.Error()))
				return
			}

			svc.logInfo(js, fmt.Sprintf("Collection %d has %d items; submit each", tgtMD.ID, len(inCollectionIDs)))
			for _, tgtID := range inCollectionIDs {
				log.Printf("INFO: prepare metadata %d for aptrust submission", tgtID)
				md, err := svc.prepareAPTrustSubmission(tgtID)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Prepare metadata %d for APTrust submission failed: %s", tgtID, err.Error()))
				} else {
					err = svc.doAPTrustSubmission(js, md)
					if err != nil {
						svc.setAPTrustProcessedStatus(md.APTrustSubmission, false)
						svc.logError(js, fmt.Sprintf("Metadata %d APTrust submission failed: %s", md.ID, err.Error()))
					}
				}
			}
			svc.logInfo(js, fmt.Sprintf("All items in collection %d submitted; flag collection as submitted", tgtMD.ID))
			svc.setAPTrustProcessedStatus(tgtMD.APTrustSubmission, true)
		} else {
			err = svc.doAPTrustSubmission(js, tgtMD)
			if err != nil {
				svc.logFatal(js, err.Error())
			}
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) prepareAPTrustSubmission(mdID int64) (*metadata, error) {
	var md metadata
	err := svc.GDB.Joins("APTrustSubmission").Joins("PreservationTier").Find(&md, mdID).Error
	if err != nil {
		return nil, fmt.Errorf("unable to load metadata %d: %s", mdID, err.Error())
	}

	if md.PreservationTierID < 2 {
		return nil, fmt.Errorf("metadata %d has not been flagged for aptrust", md.ID)
	}

	if md.APTrustSubmission == nil {
		aptSubmission := apTrustSubmission{MetadataID: md.ID, Bag: getBagFileName(&md), RequestedAt: time.Now()}
		if md.IsCollection {
			aptSubmission.Bag = "collection: no bag"
		}
		err = svc.GDB.Create(&aptSubmission).Error
		if err != nil {
			return nil, fmt.Errorf("unable to create submission record for metadatata %d: %s", md.ID, err.Error())
		}
		md.APTrustSubmission = &aptSubmission
	} else {
		// a submission record exists. See if it is in a processing state and fail if it is
		aptStatus, err := svc.getAPTrustStatus(&md)
		if err != nil {
			return nil, fmt.Errorf("aptrust status check failed for metadatata %d: %s", md.ID, err.Error())
		}
		if aptStatus.Count > 0 {
			statusRec := aptStatus.Results[0]
			if statusRec.Status != "Failed" && statusRec.Status != "Canceled" {
				return nil, fmt.Errorf("submission is already in progress for metadata %d; status %s", md.ID, statusRec.Status)
			}
		}
	}
	return &md, nil
}

func (svc *ServiceContext) setAPTrustProcessedStatus(aptSub *apTrustSubmission, success bool) {
	now := time.Now()
	aptSub.Success = success
	aptSub.ProcessedAt = &now
	svc.GDB.Save(aptSub)
}

func (svc *ServiceContext) doAPTrustSubmission(js *jobStatus, md *metadata) error {
	svc.logInfo(js, fmt.Sprintf("Begin APTrust submission for metadata %d", md.ID))
	var collectionMD *metadata
	if md.ParentMetadataID > 0 {
		svc.logInfo(js, fmt.Sprintf("Metadata %d is part of collection %d; load collection record", md.ID, md.ParentMetadataID))
		err := svc.GDB.Find(&collectionMD, md.ParentMetadataID)
		if err != nil {
			return fmt.Errorf("unable to load collection record %d:  %s", md.ParentMetadataID, err.Error)
		}
	}
	bagFile, err := svc.createBag(js, md, collectionMD)
	if err != nil {
		return fmt.Errorf("unable to create bag for metadata %d: %s", md.ID, err.Error())
	}
	svc.logInfo(js, fmt.Sprintf("Baggit tar file created here: %s; validate it...", bagFile))

	cmd := exec.Command("apt-cmd", "bag", "validate", "-p", "aptrust", bagFile)
	svc.logInfo(js, fmt.Sprintf("validate command: %+v", cmd))
	aptOut, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bag %s validation failed: %s", bagFile, aptOut)
	}

	svc.logInfo(js, fmt.Sprintf("Submit %s to APTrust S3 bucket...", bagFile))
	submitTime := time.Now()
	md.APTrustSubmission.SubmittedAt = &submitTime
	md.APTrustSubmission.ProcessedAt = nil
	md.APTrustSubmission.Success = false
	svc.GDB.Save(&md.APTrustSubmission)
	cmd = exec.Command("apt-cmd", "s3", "upload", fmt.Sprintf("--host=%s", svc.APTrust.AWSHost), fmt.Sprintf("--bucket=%s", svc.APTrust.AWSBucket), bagFile)
	svc.logInfo(js, fmt.Sprintf("submit command: %+v", cmd))
	aptOut, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("submission of %s failed: %s", bagFile, aptOut)
	}

	svc.logInfo(js, fmt.Sprintf("%s has been submitted to APTrust; check APTrust or the TrackSys metadata details page for ingest status", bagFile))
	return nil
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
