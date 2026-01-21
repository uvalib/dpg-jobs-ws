package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"path"
	"time"
)

type projectLookupResponse struct {
	Exists      bool   `json:"exists"`
	ProjectID   int64  `json:"projectID"`
	Workflow    string `json:"workflow"`
	CurrentStep string `json:"currentStep"`
	Started     bool   `json:"stated"`
	Finished    bool   `json:"finished"`
}

type createProjectRequest struct {
	UnitID          int64  `json:"unitID"`
	WorkflowID      int64  `json:"workflowID"`
	ContainerTypeID int64  `json:"containerTypeID"`
	CategoryID      int64  `json:"categoryID"`
	Condition       int64  `json:"condition"`
	Notes           string `json:"notes"`
}

func (svc *ServiceContext) projectsAPIPost(url string, payload any) *RequestError {
	log.Printf("INFO: auth project POST to %s with payload [%v]", url, payload)
	startTime := time.Now()
	projURL := fmt.Sprintf("%s/api/%s", svc.TrackSys.Imaging, url)

	projJWT, jwtErr := svc.mintTemporaryJWT()
	if jwtErr != nil {
		re := RequestError{StatusCode: http.StatusInternalServerError, Message: jwtErr.Error()}
		return &re
	}

	var req *http.Request
	if payload != nil {
		payloadBytes, _ := json.Marshal(payload)
		req, _ = http.NewRequest("POST", projURL, bytes.NewBuffer(payloadBytes))
	} else {
		req, _ = http.NewRequest("POST", projURL, nil)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", projJWT))
	rawResp, rawErr := svc.HTTPClient.Do(req)
	_, err := handleAPIResponse(projURL, rawResp, rawErr)
	elapsedNanoSec := time.Since(startTime)
	elapsedMS := int64(elapsedNanoSec / time.Millisecond)

	if err != nil {
		log.Printf("ERROR: Failed response from auth projects POST %s - %d:%s. Elapsed Time: %d (ms)",
			projURL, err.StatusCode, err.Message, elapsedMS)
	} else {
		log.Printf("INFO: Successful auth projects POST to %s. Elapsed Time: %d (ms)", projURL, elapsedMS)
	}
	return err
}

func (svc *ServiceContext) getUnitProject(unitID int64) (*projectLookupResponse, error) {
	var lookupResp projectLookupResponse
	respBytes, reqErr := svc.getRequest(fmt.Sprintf("%s/projects/lookup?unit=%d", svc.TrackSys.Imaging, unitID))
	if reqErr != nil {
		return nil, fmt.Errorf("%s", reqErr.Message)
	}
	if err := json.Unmarshal(respBytes, &lookupResp); err != nil {
		return nil, err
	}

	return &lookupResp, nil
}

func (svc *ServiceContext) projectFailedFinalization(js *jobStatus, projectID int64) {
	log.Printf("INFO: Project [%d] FAILED finalization", projectID)
	svc.failFinalization(js, projectID, js.Error)
}

func (svc *ServiceContext) failFinalization(js *jobStatus, projectID int64, reason string) {
	startTime := *js.StartedAt
	endTime := *js.EndedAt
	diff := endTime.Sub(startTime)
	payload := struct {
		Reason         string `json:"reason"`
		ProcessingMins uint   `json:"processingMins"`
		JobID          int64  `json:"jobID"`
	}{
		Reason:         reason,
		ProcessingMins: uint(math.Round(diff.Seconds() / 60.0)),
		JobID:          js.ID,
	}

	if err := svc.projectsAPIPost(fmt.Sprintf("projects/%d/fail", projectID), payload); err != nil {
		svc.logError(js, fmt.Sprintf("Unable fail project %d: %s", projectID, err.Message))
	}
}

func (svc *ServiceContext) projectFinishedFinalization(js *jobStatus, projectID int64, tgtUnit *unit) error {
	log.Printf("INFO: Project [%d] completed finalization", projectID)
	startTime := *js.StartedAt
	endTime := time.Now()
	diff := endTime.Sub(startTime)
	processingMins := uint(math.Round(diff.Seconds() / 60.0))

	var masterfiles []masterFile
	err := svc.GDB.Preload("ImageTechMeta").Where("unit_id=?", tgtUnit.ID).Find(&masterfiles).Error
	if err != nil {
		return fmt.Errorf("Unable to get updated list masterfiles: %s", err.Error())
	}

	svc.logInfo(js, "Validating finalized unit")
	if tgtUnit.ThrowAway == false {
		if tgtUnit.DateArchived == nil {
			svc.failFinalization(js, projectID, "Unit was not archived")
			return fmt.Errorf("unit was not archived")
		}
		archiveDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", tgtUnit.ID))
		tifFiles, _ := svc.getTifFiles(js, archiveDir, tgtUnit.ID)
		if len(tifFiles) == 0 {
			svc.failFinalization(js, projectID, "No tif files found in archive")
			return fmt.Errorf("No tif files found in archive")
		}
		if len(tifFiles) != len(masterfiles) {
			svc.failFinalization(js, projectID, fmt.Sprintf("MasterFile / tif count mismatch. %d tif files vs %d MasterFiles", len(tifFiles), len(masterfiles)))
			return fmt.Errorf("Archived file count different from unit masterfiles count")
		}
	}

	for _, mf := range masterfiles {
		if mf.MetadataID == nil {
			reason := fmt.Sprintf("Masterfile %s missing desc metadata", mf.Filename)
			svc.failFinalization(js, projectID, reason)
			return fmt.Errorf("%s", reason)
		}
		if mf.ImageTechMeta.ID == 0 {
			reason := fmt.Sprintf("Masterfile %s missing desc tech metadata", mf.Filename)
			svc.failFinalization(js, projectID, reason)
			return fmt.Errorf("%s", reason)
		}
	}

	// deliverables ready (patron or dl)
	if tgtUnit.IntendedUse.ID == 110 {
		if tgtUnit.IncludeInDL && tgtUnit.DateDLDeliverablesReady == nil {
			svc.failFinalization(js, projectID, "DL deliverables ready date not set")
			return fmt.Errorf("DL deliverables ready date not set")
		}
	} else {
		if tgtUnit.DatePatronDeliverablesReady == nil {
			svc.failFinalization(js, projectID, "Patron deliverables ready date not set")
			return fmt.Errorf("Patron deliverables ready date not set")
		}
	}

	// mark unit as done
	svc.logInfo(js, "Unit finished finalization")
	svc.setUnitStatus(tgtUnit, "done")

	// send request to imaging to mark project as finished
	if err := svc.finishProject(projectID, processingMins); err != nil {
		svc.logError(js, err.Error())
	}

	log.Printf("INFO: project %d finalization minutes: %d", projectID, processingMins)
	svc.logInfo(js, fmt.Sprintf("Total finalization minutes: %d", processingMins))
	return nil
}

func (svc *ServiceContext) finishProject(projectID int64, durationMins uint) error {
	finalizeReq := struct {
		ProcessingMins uint `json:"processingMins"`
	}{
		ProcessingMins: durationMins,
	}
	if err := svc.projectsAPIPost(fmt.Sprintf("projects/%d/done", projectID), finalizeReq); err != nil {
		return fmt.Errorf("finish project %d failed: %s", projectID, err.Message)
	}
	return nil
}
