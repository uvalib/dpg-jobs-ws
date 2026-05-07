package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/gin-gonic/gin"
	"github.com/seqsense/s3sync/v2"
)

type submitRegisterRequest struct {
	ClientIdentifier string `json:"cid"`        // the client identifier
	Collection       string `json:"collection"` // the collection name for the submission (optional)
}

type submitRegisterResponse struct {
	SubmissionIdentifier string `json:"sid"`
	DepositBucket        string `json:"bucket"`
	DepositPath          string `json:"path"`
}

type submitInitiateRequest struct {
	ClientIdentifier     string   `json:"cid"`         // the client identifier
	SubmissionIdentifier string   `json:"sid"`         // the submission identifier
	BagFolders           []string `json:"bag_folders"` // the bags to be included in this submission
}

type submitInitiateResponse struct {
	Submission string    `json:"submission"`
	Status     string    `json:"status"`
	Updated    time.Time `json:"updated"`
}

func (svc *ServiceContext) submitToAPTrust(c *gin.Context) {
	mdID := c.Param("id")
	log.Printf("INFO: request aptrust submission for metadata %s", mdID)
	var md metadata
	if err := svc.GDB.First(&md, mdID).Error; err != nil {
		log.Printf("INFO: unable to load metadata %s for aptrust submission: %s", mdID, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	js, err := svc.createJobStatus("APTrustSubmit", "Metadata", md.ID)
	if err != nil {
		log.Printf("ERROR: unable to create aptrust submission job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Validate metadata %d is a candidate aptrust submission", md.ID))
	if err := svc.validateAPTrustSubmissionRequest(&md); err != nil {
		svc.logFatal(js, fmt.Sprintf("Metadata %d is not a candidate for aptrust: %s", md.ID, err.Error()))
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

		// first, register a new submission. This gets the submission identifer is used as the
		// top-level directory name for assembling the sumission files
		regResp, err := svc.registerSubmission(js, &md)
		if err != nil {
			svc.logFatal(js, err.Error())
			return
		}
		svc.logInfo(js, fmt.Sprintf("Submission registered %+v", regResp))

		// create a top-level submission directory that will contain subdirectories for each metadata record being submitted
		// each metadata record beig submistted will be named like virginia.edu.tracksys-xmlmetadata-109241 and contain the following:
		//   * one tif file per master file
		//   * optional "aptrust-description.txt" and "aptrust-title.txt" that are used to populate aptrust-info.txt
		//   * one metadata xml file
		//   * one manifest-md5.txt has one line per file above; m5dchecksum filename
		submitBaseDir := path.Join(svc.ProcessingDir, "bags", regResp.SubmissionIdentifier)
		svc.logInfo(js, fmt.Sprintf("Create new submission base directory %s", submitBaseDir))
		if pathExists(submitBaseDir) {
			svc.logInfo(js, fmt.Sprintf("Clean up pre-existing submission directory %s", submitBaseDir))
			if err := os.RemoveAll(submitBaseDir); err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to clean up existing submission directory %s: %s", submitBaseDir, err.Error()))
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
		} else {
			if err := ensureDirExists(submitBaseDir, 0777); err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to create submission directory %s: %s", submitBaseDir, err.Error()))
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
		}

		bagFolderList := make([]string, 0)
		if md.IsCollection {
			svc.logInfo(js, fmt.Sprintf("Metadata %d is a collection; load child record IDs for APTrust submission", md.ID))
			var inCollectionMD []metadata
			if err := svc.GDB.Where("parent_metadata_id=?", md.ID).Find(&inCollectionMD).Error; err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to load child metadata records for collection %d: %s", md.ID, err.Error()))
				return
			}

			svc.logInfo(js, fmt.Sprintf("Collection %d has %d items; submit each", md.ID, len(inCollectionMD)))
			for _, tgtMD := range inCollectionMD {
				if err := svc.buildAPTrustSubmissionDirectory(js, submitBaseDir, &tgtMD); err != nil {
					svc.logError(js, fmt.Sprintf("Metadata %d APTrust submission failed: %s", md.ID, err.Error()))
				} else {
					bagFolderList = append(bagFolderList, getSubmissionDirectoryName(&tgtMD))
				}
			}
			svc.logInfo(js, fmt.Sprintf("All items in collection %d submitted; flag collection as submitted", md.ID))
		} else {
			if err := svc.buildAPTrustSubmissionDirectory(js, submitBaseDir, &md); err != nil {
				svc.logFatal(js, err.Error())
				return
			}
			bagFolderList = append(bagFolderList, getSubmissionDirectoryName(&md))
		}

		svc.logInfo(js, "All submission directories have been created")

		if err := svc.uploadToAPTrustBucket(js, submitBaseDir, regResp); err != nil {
			svc.logFatal(js, err.Error())
			return
		}

		if err := svc.initiateSubmission(js, regResp.SubmissionIdentifier, bagFolderList); err != nil {
			svc.logFatal(js, err.Error())
			return
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) validateAPTrustSubmissionRequest(md *metadata) error {
	if md.IsCollection {
		log.Printf("INFO: metadata %d is a collection; check members for masterfiles suitable for aptrust", md.ID)
		var inCollectionIDs []int64
		if err := svc.GDB.Raw("select id from metadata where parent_metadata_id=?", md.ID).Scan(&inCollectionIDs).Error; err != nil {
			return err
		}
		if err := svc.validateAPTrustMetadata(inCollectionIDs); err != nil {
			return fmt.Errorf("collection metadata %d is not suitable for aptrust: %s", md.ID, err.Error())
		}
	} else {
		log.Printf("INFO: validate aptrust submission for metadata %d", md.ID)
		idParam := []int64{md.ID}
		if err := svc.validateAPTrustMetadata(idParam); err != nil {
			return fmt.Errorf("metadata %d is not suitable for aptrust: %s", md.ID, err.Error())
		}
	}
	return nil
}

func (svc *ServiceContext) validateAPTrustMetadata(metadataIDs []int64) error {
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

func (svc *ServiceContext) buildAPTrustSubmissionDirectory(js *jobStatus, submitBaseDir string, md *metadata) error {
	svc.logInfo(js, fmt.Sprintf("Build APTrust submission directory for metadata %d", md.ID))
	var collectionMD *metadata
	if md.ParentMetadataID > 0 {
		svc.logInfo(js, fmt.Sprintf("Metadata %d is part of collection %d; load collection record", md.ID, md.ParentMetadataID))
		if err := svc.GDB.First(&collectionMD, md.ParentMetadataID).Error; err != nil {
			return fmt.Errorf("unable to load collection record %d:  %s", md.ParentMetadataID, err.Error())
		}
	}

	// create the metadata submission directory
	mdDirName := getSubmissionDirectoryName(md)
	submitAssembleDir := path.Join(submitBaseDir, mdDirName)
	if err := ensureDirExists(submitAssembleDir, 0777); err != nil {
		return fmt.Errorf("unable to create submission directory %s: %s", submitAssembleDir, err.Error())
	}

	// init a map of checksum => filename
	checksums := make(map[string]string, 0)

	// add aptrust-title.txt with with content being the title of the md record...
	titlePath := filepath.Join(submitAssembleDir, "aptrust-title.txt")
	if err := os.WriteFile(titlePath, []byte(md.Title), 0644); err != nil {
		svc.logError(js, fmt.Sprintf("unable to create %s: %s", titlePath, err.Error()))
	} else {
		titleMD5 := md5Checksum(titlePath)
		checksums[titleMD5] = "aptrust-title.txt"
	}

	// Add metadata record to submission directory...
	if md.ExternalSystemID == 1 {
		// Request archivesSpace metadata if necessary;ExternalSystemID 1 is ArchivesSpace
		svc.logInfo(js, "Metadata is linked to ArchivesSpace; request JSON metadata")
		asURL := parsePublicASURL(md.ExternalURI)
		if asURL == nil {
			return fmt.Errorf("%s is not a valid archivespoace url", md.ExternalURI)
		}

		if err := svc.validateArchivesSpaceAccessToken(); err != nil {
			return fmt.Errorf("unable to get archivesspace auth token: %s", err.Error())
		}

		asMetadata, err := svc.getArchivesSpaceMetadata(asURL, md.PID)
		if err != nil {
			return fmt.Errorf("unable to get archivesspace metadata: %s", err.Error())
		}
		svc.logInfo(js, "Add ArchivesSpace JSON metadata")
		jsonStr, err := json.MarshalIndent(asMetadata, "", "   ")
		if err != nil {
			return fmt.Errorf("unable to stringify as metadata: %s", err.Error())
		}

		mdPath := path.Join(submitAssembleDir, "metadata.json")
		if err := os.WriteFile(mdPath, jsonStr, 0644); err != nil {
			return fmt.Errorf("unable to create metadata.json for %s: %s", md.PID, err.Error())
		}
		md5 := md5Checksum(mdPath)
		checksums[md5] = "metadata.json"
	} else {
		svc.logInfo(js, "Add MODS XML metadata")
		mods, err := svc.getModsMetadata(md)
		if err != nil {
			return fmt.Errorf("unable to get mods metadata: %s", err.Error())
		}
		mdName := fmt.Sprintf("%s.xml", strings.ReplaceAll(md.PID, ":", "_"))
		mdPath := path.Join(submitAssembleDir, mdName)
		if err := os.WriteFile(mdPath, []byte(mods), 0644); err != nil {
			return fmt.Errorf("unable to create %s.xml: %s", md.PID, err.Error())
		}
		md5 := md5Checksum(mdPath)
		checksums[md5] = mdName
	}

	svc.logInfo(js, "Adding master files to submission")
	masterFiles := svc.getBestMasterFiles(js, uint64(md.ID))
	if len(masterFiles) == 0 {
		return fmt.Errorf("no masterfiles qualify for APTrust (intended use 110 or 101)")
	}

	svc.logInfo(js, fmt.Sprintf("%d masterfiles found", len(masterFiles)))
	for _, mf := range masterFiles {
		svc.logInfo(js, fmt.Sprintf("Adding masterfile %s", mf.Filename))
		archiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", mf.UnitID), mf.Filename)
		destFile := path.Join(submitAssembleDir, mf.Filename)
		if pathExists(archiveFile) == false {
			return fmt.Errorf("%s not found", archiveFile)
		}
		origMD5 := md5Checksum(archiveFile)
		md5, err := copyFile(archiveFile, destFile, 0744)
		if err != nil {
			return fmt.Errorf("copy %s to %s failed: %s", archiveFile, destFile, err.Error())
		}
		if md5 != origMD5 {
			return fmt.Errorf("copy %s MD5 checksum %s does not match original %s", destFile, md5, origMD5)
		}
		checksums[md5] = mf.Filename
	}

	md5FileName := path.Join(submitAssembleDir, "manifest-md5.txt")
	svc.logInfo(js, fmt.Sprintf("Create %s", md5FileName))
	var md5Data strings.Builder
	for md5, fn := range checksums {
		fmt.Fprintf(&md5Data, "%s %s\n", md5, fn)
	}
	os.WriteFile(md5FileName, []byte(md5Data.String()), 0644)

	svc.logInfo(js, fmt.Sprintf("Submission directory for %d is complete", md.ID))
	return nil
}

func (svc *ServiceContext) registerSubmission(js *jobStatus, md *metadata) (*submitRegisterResponse, error) {
	svc.logInfo(js, fmt.Sprintf("Register submission for metadata %d: %s", md.ID, md.Title))
	req := submitRegisterRequest{ClientIdentifier: svc.APTrust.ClientID, Collection: md.Title}
	respBytes, err := svc.sendAPTPostRequest("/register", req)
	if err != nil {
		return nil, fmt.Errorf("%d: %s", err.StatusCode, err.Message)
	}

	var resp submitRegisterResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (svc *ServiceContext) uploadToAPTrustBucket(js *jobStatus, submissionDir string, reg *submitRegisterResponse) error {
	svc.logInfo(js, fmt.Sprintf("Upload files from %s to aptrust submission bucket %s with key %s",
		submissionDir, reg.DepositBucket, reg.DepositPath))

	// setup the s3 sync manager
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}
	syncManager := s3sync.New(cfg, s3sync.WithParallel(5))

	// our destination location
	source := fmt.Sprintf("s3://%s/%s", reg.DepositBucket, path.Join(reg.DepositPath, reg.SubmissionIdentifier))
	svc.logInfo(js, fmt.Sprintf("Sync from [%s] -> [%s]", submissionDir, source))

	start := time.Now()
	if err := syncManager.Sync(context.TODO(), submissionDir, source); err != nil {
		return err
	}

	stats := syncManager.GetStatistics()
	duration := time.Since(start)
	svc.logInfo(js, fmt.Sprintf("Sync completed (elapsed %0.2f seconds)", duration.Seconds()))
	svc.logInfo(js, fmt.Sprintf("%d bytes written, %d files uploaded, %d files deleted", stats.Bytes, stats.Files, stats.DeletedFiles))
	return nil
}

func (svc *ServiceContext) initiateSubmission(js *jobStatus, sid string, bagList []string) error {
	svc.logInfo(js, fmt.Sprintf("Initiate submission %s with bags %v", sid, bagList))
	req := submitInitiateRequest{
		ClientIdentifier:     svc.APTrust.ClientID,
		SubmissionIdentifier: sid,
		BagFolders:           bagList,
	}

	respBytes, err := svc.sendAPTPostRequest("/initiate", req)
	if err != nil {
		return fmt.Errorf("%d: %s", err.StatusCode, err.Message)
	}

	var resp submitInitiateResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return err
	}

	svc.logInfo(js, fmt.Sprintf("Submission %s initate success with status %s", sid, resp.Status))
	return nil
}

func getSubmissionDirectoryName(md *metadata) string {
	return fmt.Sprintf("virginia.edu.tracksys-%s-%d", strings.ToLower(md.Type), md.ID)
}
