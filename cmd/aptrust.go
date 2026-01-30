package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
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
	GroupIdentifier  string `json:"bag_group_identifier"`
	StorageOption    string `json:"storage_option"`
	Note             string `json:"note"`
	Status           string `json:"status"`
	ProcessedAt      string `json:"date_processed"`
}

type apTrustResponse struct {
	Count   int64           `json:"count"`
	Results []apTrustResult `json:"results,omitempty"`
}

type aptS3Response struct {
	Etag    string `json:"etag"`
	Name    string `json:"name"`
	Storage string `json:"storageClass"`
}

func (svc *ServiceContext) batchAPTrustSubmission(c *gin.Context) {
	var req struct {
		CollectionID    int64   `json:"collectionID"`
		MetadataRecords []int64 `json:"metadataRecords"`
	}
	err := c.BindJSON(&req)
	if err != nil {
		log.Printf("ERROR: invalid batch aptrust submit request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	js, err := svc.createJobStatus("APTrustBatchSubmit", "Metadata", req.CollectionID)
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
				svc.logFatal(js, fmt.Sprintf("Panic recovered during APTrust bulk submission: %v", r))
			}
		}()
		svc.logInfo(js, fmt.Sprintf("Batch submit records %v from collection %d to APTRust", req.MetadataRecords, req.CollectionID))
		for _, mdID := range req.MetadataRecords {
			svc.logInfo(js, fmt.Sprintf("Prepare metadata %d for aptrust submission", mdID))
			md, err := svc.prepareAPTrustSubmission(mdID, false)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Prepare metadata %d for APTrust submission failed: %s", mdID, err.Error()))
			} else {
				err = svc.doAPTrustSubmission(js, md)
				if err != nil {
					svc.setAPTrustProcessedStatus(md.APTrustSubmission, false)
					svc.logError(js, fmt.Sprintf("Metadata %d APTrust submission failed: %s", md.ID, err.Error()))
				}
			}
		}
		svc.jobDone(js)
	}()
	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) submitToAPTrust(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	if mdID == 0 {
		log.Printf("INFO: invalid id %s passed to aptrust request", c.Param("id"))
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid metadata id", c.Param("id")))
		return
	}
	resubmit, _ := strconv.ParseBool(c.Query("resubmit"))

	if resubmit {
		log.Printf("INFO: prepare metadata %d for aptrust resubmission", mdID)
	} else {
		log.Printf("INFO: validate metadata %d is a candidate aptrust submission", mdID)
		if err := svc.validateAPTrustSubmissionRequest(uint64(mdID)); err != nil {
			log.Printf("ERROR: metadata %d is not a candidate for aptrust: %s", mdID, err.Error())
			return
		}
		log.Printf("INFO: prepare metadata %d for aptrust submission", mdID)
	}
	tgtMD, err := svc.prepareAPTrustSubmission(mdID, resubmit)
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
			if resubmit {
				svc.logFatal(js, "Collections cannot be resubmitted")
				return
			}
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
				md, err := svc.prepareAPTrustSubmission(tgtID, false) // always leave this false. don't want to resubmit everything
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

func (svc *ServiceContext) validateAPTrustMetadata(metadataIDs []uint64) error {
	var mfResp []struct {
		MetadataID uint64
		UnitID     uint64
		Cnt        int
	}

	// get a list of master file counts for intended use 110 pr 101 units for the target metadataIDs
	mdQ := "select u.metadata_id as metadata_id, u.id as unit_id, count(mf.id) as cnt from units u "
	mdQ += " left join master_files mf on mf.unit_id = u.id"
	mdQ += " where (intended_use_id=110 or intended_use_id=101) AND (unit_status=? OR unit_status=?)"
	mdQ += " AND u.metadata_id in ? group by u.id"
	if err := svc.GDB.Raw(mdQ, "approved", "done", metadataIDs).Scan(&mfResp).Error; err != nil {
		return err
	}

	mfCnt := 0
	for _, rec := range mfResp {
		mfCnt += rec.Cnt
		if rec.Cnt == 0 {
			log.Printf("INFO: metadata %d has no valid units/masterfiles", rec.MetadataID)
		}
	}

	if mfCnt > 0 {
		log.Printf("INFO: %d masterfiles found in valid aptrust submission", mfCnt)
		return nil
	}

	log.Printf("INFO: no matching units found for aptrust submission; try masterfiles")
	mfQ := "select mf.metadata_id as metadata_id, mf.unit_id as unit_id, count(mf.id) as cnt from master_files mf "
	mfQ += " inner join units u on u.id = mf.unit_id "
	mfQ += " where (intended_use_id=110 or intended_use_id=101) AND (unit_status=? OR unit_status=?)"
	mfQ += " AND mf.metadata_id in ?"
	if err := svc.GDB.Raw(mfQ, "approved", "done", metadataIDs).Scan(&mfResp).Error; err != nil {
		return err
	}

	for _, rec := range mfResp {
		mfCnt += rec.Cnt
		if rec.Cnt == 0 {
			log.Printf("INFO: metadata %d has no valid units/masterfiles", rec.MetadataID)
		}
	}

	if mfCnt > 0 {
		log.Printf("INFO: %d masterfiles found in valid aptrust submission", mfCnt)
		return nil
	}

	return fmt.Errorf("invalid for aptrust; no master files found")
}

func (svc *ServiceContext) validateAPTrustSubmissionRequest(mdID uint64) error {
	var md metadata
	if err := svc.GDB.Joins("APTrustSubmission").Joins("PreservationTier").Find(&md, mdID).Error; err != nil {
		return err
	}

	if md.PreservationTierID < 2 {
		return fmt.Errorf("metadata %d has not been flagged for aptrust", md.ID)
	}

	if md.IsCollection {
		log.Printf("INFO: metadata %d is a collection; check members for masterfiles suitable for aptrust", mdID)
		var inCollectionIDs []uint64
		if err := svc.GDB.Raw("select id from metadata where parent_metadata_id=?", mdID).Scan(&inCollectionIDs).Error; err != nil {
			return err
		}
		if err := svc.validateAPTrustMetadata(inCollectionIDs); err != nil {
			return fmt.Errorf("collection metadata %d is not suitable for aptrust: %s", mdID, err.Error())
		}
	} else {
		log.Printf("INFO: validate aptrust submission for metadata %d", mdID)
		idParam := []uint64{mdID}
		if err := svc.validateAPTrustMetadata(idParam); err != nil {
			return fmt.Errorf("metadata %d is not suitable for aptrust: %s", mdID, err.Error())
		}
	}
	return nil
}

func (svc *ServiceContext) prepareAPTrustSubmission(mdID int64, resubmit bool) (*metadata, error) {
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
			aptSubmission.Bag = fmt.Sprintf("collection %s", md.CollectionID)
		}
		err = svc.GDB.Create(&aptSubmission).Error
		if err != nil {
			return nil, fmt.Errorf("unable to create submission record for metadatata %d: %s", md.ID, err.Error())
		}
		md.APTrustSubmission = &aptSubmission
	} else {
		// resubmit is used when there has been a successful submission, but the bag needs to change.
		// in this case, do not check status as it wil be Success and fail the checks below
		if resubmit == false {
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
			} else {
				log.Printf("INFO: aptrust has no record of metadata %d submission, just resubmit", md.ID)
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
		err := svc.GDB.Find(&collectionMD, md.ParentMetadataID).Error
		if err != nil {
			return fmt.Errorf("unable to load collection record %d:  %s", md.ParentMetadataID, err.Error())
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
	svc.logInfo(js, fmt.Sprintf("submit response: %s", aptOut))
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

	if md.IsCollection {
		log.Printf("INFO: metadata %d is a collection; request collection status instead of item", md.ID)
		collectionResp, err := svc.getAPTrustGroupStatus(&md)
		if err != nil {
			log.Printf("ERROR: aptrust status request for colection metadata %d failed: %s", md.ID, err.Error())
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.JSON(http.StatusOK, collectionResp)
	} else {
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
}

func (svc *ServiceContext) getAPTrustGroupStatus(collectionMD *metadata) ([]apTrustResult, error) {
	// collections are generally grouped by the collection PID. Exception is a collection comprised of ArchivesSpace items.
	// In this case, the group ID is the collection identifer of the AS group, which is contained in the  collection_id field.
	// Do small aptrust queries to determine which to use:
	// TODO T
	//    This might need to change if CollectionID field cannot be used. Instead it will be some mangling of the title but not a colon.
	//    maybe [ID] title instead or ID | Title
	groupID := ""
	candidates := []string{collectionMD.PID, collectionMD.CollectionID}
	for _, testID := range candidates {
		log.Printf("INFO: check aptrust for group id [%s]", testID)
		cmd := exec.Command("apt-cmd", "registry", "list", "workitems", fmt.Sprintf("bag_group_identifier=%s", testID), "per_page=1")
		log.Printf("INFO: %+v", cmd)
		aptOut, cmdErr := cmd.CombinedOutput()
		if cmdErr != nil {
			log.Printf("ERROR: unable to test existance of collection id %s: %s", testID, cmdErr.Error())
			continue
		}

		var jsonResp apTrustResponse
		jsonErr := json.Unmarshal(aptOut, &jsonResp)
		if jsonErr != nil {
			log.Printf("ERROR: unable to parse aptrust collection id %s response: %s", testID, jsonErr.Error())
			continue
		}

		if jsonResp.Count > 0 {
			log.Printf("INFO: group %s exists with %d records", testID, jsonResp.Count)
			groupID = testID
			break
		}
	}

	if groupID == "" {
		return nil, fmt.Errorf("no aptrust collection data found for metadata %d", collectionMD.ID)
	}

	log.Printf("INFO: get a list of aptrust submission info for collection %d from tracksys", collectionMD.ID)
	q := "select ats.* from ap_trust_submissions ats inner join metadata m on m.id = ats.metadata_id where m.parent_metadata_id = ?"
	var aptSubs []apTrustSubmission
	subErr := svc.GDB.Raw(q, collectionMD.ID).Scan(&aptSubs).Error
	if subErr != nil {
		return nil, fmt.Errorf("unable to get submission records for collection %d: %s", collectionMD.ID, subErr.Error())
	}
	totalRecs := len(aptSubs)

	page := 1
	done := false
	responseCount := 0
	statusResp := make([]apTrustResult, 0)
	var err error
	for done == false {
		log.Printf("INFO: request page %d of results for group %s - %s", page, groupID, collectionMD.Title)
		cmd := exec.Command("apt-cmd", "registry", "list", "workitems", fmt.Sprintf("bag_group_identifier=%s", groupID),
			"sort=date_processed__desc", "per_page=1000", fmt.Sprintf("page=%d", page))
		aptOut, cmdErr := cmd.CombinedOutput()
		if cmdErr != nil {
			done = true
			err = fmt.Errorf("%s", string(aptOut))
			break
		}

		var jsonResp apTrustResponse
		jsonErr := json.Unmarshal(aptOut, &jsonResp)
		if jsonErr != nil {
			done = true
			err = fmt.Errorf("malformed response: %s", jsonErr.Error())
			break
		}

		log.Printf("INFO: %d results from a total of %d received", len(jsonResp.Results), jsonResp.Count)
		responseCount += len(jsonResp.Results)
		if responseCount == int(jsonResp.Count) {
			log.Printf("INFO: all %d responses received for group %s", jsonResp.Count, groupID)
			done = true
		} else {
			log.Printf("INFO: more results remain; increase page number")
			page++
		}

		// walk the list if respnses and include the first instance of each
		// unique name (bag filename) in the response as this will be the latest status
		for _, resp := range jsonResp.Results {
			for subIdx, sub := range aptSubs {
				if sub.Bag == resp.Name {
					// submission info still exists in the full list; add the APTrust status
					// to the response and remove the submission info from the list
					statusResp = append(statusResp, resp)
					aptSubs = append(aptSubs[:subIdx], aptSubs[subIdx+1:]...)
					break
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	log.Printf("INFO: group %s status complete; %d unique items found from an expected total of %d", groupID, len(statusResp), totalRecs)
	for _, sub := range aptSubs {
		statusResp = append(statusResp, apTrustResult{
			Name:            sub.Bag,
			GroupIdentifier: groupID,
			Status:          "Pending",
			Note:            "Submitted to S3 bucket and awaiting ingest to APTrust"})
	}
	return statusResp, nil
}

func (svc *ServiceContext) getAPTrustStatus(md *metadata) (*apTrustResponse, error) {
	cmd := exec.Command("apt-cmd", "registry", "list", "workitems", fmt.Sprintf("name=%s", getBagFileName(md)), "sort=date_processed__desc")
	aptOut, err := cmd.CombinedOutput()
	log.Printf("aptrust registry response: %s", aptOut)
	if err != nil {
		return nil, fmt.Errorf("%s", aptOut)
	}

	var jsonResp apTrustResponse
	err = json.Unmarshal(aptOut, &jsonResp)
	if err != nil {
		return nil, fmt.Errorf("malformed response: %s", err.Error())
	}

	if jsonResp.Count == 0 {
		log.Printf("INFO: no work item found for metadata %d; check s3 bucket", md.ID)
		var aptSub apTrustSubmission
		err := svc.GDB.Where("metadata_id=?", md.ID).First(&aptSub).Error
		if err != nil {
			return nil, fmt.Errorf("no aptrust status found: %s", err.Error())
		}
		cmd = exec.Command("apt-cmd", "s3", "list", fmt.Sprintf("--host=%s", svc.APTrust.AWSHost), fmt.Sprintf("--bucket=%s", svc.APTrust.AWSBucket), fmt.Sprintf("--prefix=%s", aptSub.Bag))
		log.Printf("INFO: s3 list command: %+v", cmd)
		aptS3Out, err := cmd.CombinedOutput()
		log.Printf("INFO: s3 list response: [%s]", aptS3Out)
		if len(aptS3Out) > 0 {
			var s3Resp aptS3Response
			jsonErr := json.Unmarshal(aptS3Out, &s3Resp)
			if jsonErr != nil {
				return nil, fmt.Errorf("malformed s3 response: %s", jsonErr.Error())
			}

			jsonResp.Count = 1
			jsonResp.Results = make([]apTrustResult, 0)
			jsonResp.Results = append(jsonResp.Results, apTrustResult{
				ETag:          s3Resp.Etag,
				Name:          s3Resp.Name,
				StorageOption: s3Resp.Storage,
				Status:        "Pending",
				Note:          "Submitted to S3 bucket and awaiting ingest to APTrust",
			})
		} else {
			log.Printf("INFO: no aptrust status found for metadata %d; prior submission failed before being sent", md.ID)
			jsonResp.Count = 0
			jsonResp.Results = make([]apTrustResult, 0)
		}
	}

	return &jsonResp, nil
}

type findingAidResp struct {
	Metadata   metadata
	FindingAid []byte
}

func (svc *ServiceContext) submitFindingAidToAPTrust(c *gin.Context) {
	mdID := c.Param("id")
	log.Printf("INFO: submit finding aid for metadata %s to aptrust", mdID)
	faResp, err := svc.getFindingAid(mdID)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("INFO: finding aid for %s generated; create bag", mdID)
	faSubdir := getBagDirectoryName(&faResp.Metadata)
	bagBaseDir := path.Join(svc.ProcessingDir, "bags")
	bagAssembleDir := path.Join(bagBaseDir, faSubdir)
	if pathExists(bagAssembleDir) {
		log.Printf("INFO: clean up pre-existing bag directory %s", bagAssembleDir)
		err := os.RemoveAll(bagAssembleDir)
		if err != nil {
			log.Printf("ERROR: unable to cleanup %s: %s", bagAssembleDir, err.Error())
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
	}

	log.Printf("INFO: create finding aid bag directory %s", bagAssembleDir)
	err = ensureDirExists(bagAssembleDir, 0777)
	if err != nil {
		log.Printf("ERROR: unable to create %s: %s", bagAssembleDir, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	findingAidFile := path.Join(bagAssembleDir, fmt.Sprintf("findingaid-%d.xml", faResp.Metadata.ID))
	log.Printf("INFO: write finding aid for collection %d to %s", faResp.Metadata.ID, findingAidFile)
	os.WriteFile(findingAidFile, faResp.FindingAid, 0644)

	bagFileName := path.Join(bagBaseDir, fmt.Sprintf("%s.tar", faSubdir))
	if pathExists(bagFileName) {
		log.Printf("INFO: clean up pre-existing bag file %s", bagFileName)
		err = os.Remove(bagFileName)
		if err != nil {
			log.Printf("ERROR: unable to cleanup %s: %s", bagFileName, err.Error())
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
	}
	storage := "Standard"
	if faResp.Metadata.PreservationTierID == 2 {
		storage = "Glacier-Deep-OH"
	}

	// NOTE: must include double quote around the entire tag (name and value) if it contains spaces. See title as an example
	cmdArray := []string{"bag", "create", "--profile=aptrust", "--manifest-algs=md5,sha256"}
	cmdArray = append(cmdArray, fmt.Sprintf("--output-file=%s", bagFileName))
	cmdArray = append(cmdArray, fmt.Sprintf("--bag-dir=%s", bagAssembleDir))
	cmdArray = append(cmdArray, fmt.Sprintf("--tags=\"aptrust-info.txt/Title=%s\"", faResp.Metadata.Title))
	cmdArray = append(cmdArray, "--tags=aptrust-info.txt/Access=Consortia")
	cmdArray = append(cmdArray, fmt.Sprintf("--tags=aptrust-info.txt/Storage-Option=%s", storage))
	cmdArray = append(cmdArray, "--tags=bag-info.txt/Source-Organization=virginia.edu")
	cmdArray = append(cmdArray, "--tags=\"Bag-Count=1 of 1\"")
	cmdArray = append(cmdArray, fmt.Sprintf("--tags=\"Bag-Group-Identifier=%s\"", faResp.Metadata.CollectionID))
	cmdArray = append(cmdArray, fmt.Sprintf("--tags=\"Internal-Sender-Identifier=%s\"", faResp.Metadata.CollectionID))
	cmd := exec.Command("apt-cmd", cmdArray...)
	log.Printf("INFO: bag command: %+v", cmd)
	aptOut, err := exec.Command("apt-cmd", cmdArray...).CombinedOutput()
	if err != nil {
		log.Printf("ERROR: bag %s create failed: %s", bagFileName, aptOut)
		c.String(http.StatusInternalServerError, string(aptOut))
		return
	}

	cmd = exec.Command("apt-cmd", "bag", "validate", "-p", "aptrust", bagFileName)
	log.Printf("INFO: validate command: %+v", cmd)
	aptOut, err = cmd.CombinedOutput()
	if err != nil {
		log.Printf("ERROR: bag %s validation failed: %s", bagFileName, aptOut)
		c.String(http.StatusInternalServerError, string(aptOut))
		return
	}

	log.Printf("INFO: submit %s to aptrust bucket...", bagFileName)
	cmd = exec.Command("apt-cmd", "s3", "upload", fmt.Sprintf("--host=%s", svc.APTrust.AWSHost), fmt.Sprintf("--bucket=%s", svc.APTrust.AWSBucket), bagFileName)
	log.Printf("INFO: submit command: %+v", cmd)
	aptOut, err = cmd.CombinedOutput()
	log.Printf("INFO: submit response: %s", aptOut)
	if err != nil {
		log.Printf("ERROR: s3 submit failed: %s", aptOut)
		c.String(http.StatusInternalServerError, fmt.Sprintf("s3 submit failed: %s", aptOut))
		return
	}

	type aptS3SubmitResponse struct {
		Bucket string
		Key    string
		ETag   string
	}
	var jsonResp aptS3SubmitResponse
	err = json.Unmarshal(aptOut, &jsonResp)
	if err != nil {
		log.Printf("WARNING: s3 submit successful, but unable to parse response: %s", err.Error())
		c.String(http.StatusOK, fmt.Sprintf("submission success, but anable to parse resposne: %s", err.Error()))
		return
	}

	c.JSON(http.StatusOK, jsonResp)
}

func (svc *ServiceContext) generateFindingAid(c *gin.Context) {
	mdID := c.Param("id")
	log.Printf("INFO: get finding aid for metadata %s", mdID)
	faResp, err := svc.getFindingAid(mdID)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, string(faResp.FindingAid))
}

type findingAidError struct {
	StatusCode int
	Message    string
}

func (e *findingAidError) Error() string {
	return fmt.Sprintf("%d - %s", e.StatusCode, e.Message)
}

func (svc *ServiceContext) getFindingAid(mdID string) (*findingAidResp, error) {
	log.Printf("INFO: load collection metadata record %s", mdID)
	var faResp findingAidResp
	err := svc.GDB.Find(&faResp.Metadata, mdID).Error
	if err != nil {
		return nil, &findingAidError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("unable to get metadata %s: %s", mdID, err.Error()),
		}
	}

	// finding aid is only valid for collection records
	if faResp.Metadata.IsCollection == false {
		return nil, &findingAidError{
			StatusCode: http.StatusBadRequest,
			Message:    fmt.Sprintf("metadata %d is not a collection", faResp.Metadata.ID),
		}
	}

	// it is also only valid for collections made of only archivesspace records
	var nonASCount int64
	err = svc.GDB.Table("metadata").Where("parent_metadata_id=? and external_system_id!=?", faResp.Metadata.ID, 1).Count(&nonASCount).Error
	if err != nil {
		return nil, &findingAidError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("unable to get non-archivesspace record count for  metadata %d: %s", faResp.Metadata.ID, err.Error()),
		}
	}
	if nonASCount > 0 {
		return nil, &findingAidError{
			StatusCode: http.StatusBadRequest,
			Message:    "finding aid is only available for archivesspace collections",
		}
	}

	log.Printf("INFO: find archivesspace uri for collection %s", faResp.Metadata.CollectionID)
	asURL := fmt.Sprintf("/search?q=%s&type[]=resource&page=1&page_size=1", url.QueryEscape(faResp.Metadata.CollectionID))
	resp, asErr := svc.sendASGetRequest(asURL)
	if asErr != nil {
		return nil, &findingAidError{
			StatusCode: asErr.StatusCode,
			Message:    fmt.Sprintf("archivesspace request failed: %s", asErr.Message),
		}
	}

	var jsonResp asSearchResp
	err = json.Unmarshal(resp, &jsonResp)
	if err != nil {
		return nil, &findingAidError{
			StatusCode: http.StatusInternalServerError,
			Message:    fmt.Sprintf("unable to parse archivesspace results: %s", err.Error()),
		}
	}

	if jsonResp.TotalHits == 0 {
		return nil, &findingAidError{
			StatusCode: http.StatusBadRequest,
			Message:    fmt.Sprintf("no matches found for %s ", faResp.Metadata.CollectionID),
		}
	}
	if jsonResp.TotalHits > 1 {
		return nil, &findingAidError{
			StatusCode: http.StatusBadRequest,
			Message:    fmt.Sprintf("%s has %d matches; expect only 1", faResp.Metadata.CollectionID, jsonResp.TotalHits),
		}
	}

	uri := jsonResp.Ressults[0].URI
	log.Printf("INFO: request finding aid for %s using uri %s", faResp.Metadata.CollectionID, uri)
	parsedURL := parsePublicASURL(uri)
	asURL = fmt.Sprintf("/repositories/%s/resource_descriptions/%s.xml", parsedURL.RepositoryID, parsedURL.ParentID)
	faResp.FindingAid, asErr = svc.sendASGetRequest(asURL)
	if asErr != nil {
		return nil, &findingAidError{
			StatusCode: asErr.StatusCode,
			Message:    fmt.Sprintf("archivesspace request failed: %s", asErr.Message),
		}
	}
	return &faResp, nil
}
