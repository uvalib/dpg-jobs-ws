package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

type submissionRec struct {
	Metadata    metadata
	MasterFiles []masterFile
}

func (svc *ServiceContext) submitToAPTrust(c *gin.Context) {
	mdID := c.Param("id")
	log.Printf("INFO: request aptrust submission for metadata %s", mdID)

	var priorReg submitRegisterResponse
	if err := c.ShouldBindBodyWithJSON(&priorReg); err != nil {
		// the post body is optional. If it is not present, the bind will throw an EOF error. Ignore it
		if err != io.EOF {
			log.Printf("INFO: unable to parse apt submit request: %s", err.Error())
			c.String(http.StatusBadRequest, err.Error())
			return
		}
	}

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

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered during APTrust submission: %v", r))
			}
		}()

		// first, insure the submission is acceptable and collect necessary submittion detail
		svc.logInfo(js, fmt.Sprintf("Validate metadata %d is a candidate for aptrust submission", md.ID))
		submissionInfo, err := svc.validateAPTrustSubmissionRequest(js, &md)
		if err != nil {
			svc.logFatal(js, err.Error())
			return
		}

		reuseSID := false
		var regResp *submitRegisterResponse
		if priorReg.SubmissionIdentifier == "" {
			// next, register a new submission. This gets the submission identifer is used as the
			// top-level directory name for assembling the sumission files
			resp, err := svc.registerSubmission(js, &md)
			if err != nil {
				svc.logFatal(js, err.Error())
				return
			}
			regResp = resp
			svc.logInfo(js, fmt.Sprintf("Submission registered %+v", regResp))
		} else {
			reuseSID = true
			regResp = &priorReg
			svc.logInfo(js, fmt.Sprintf("Submission will reuse a previously registered sid [%v]", priorReg))
		}

		// create a top-level submission directory that will contain subdirectories for each metadata record being submitted
		// each metadata record beig submistted will be named like virginia.edu.tracksys-xmlmetadata-109241 and contain the following:
		//   * one tif file per master file
		//   * optional "aptrust-description.txt" and "aptrust-title.txt" that are used to populate aptrust-info.txt
		//   * one metadata xml file
		//   * one manifest-md5.txt has one line per file above; m5dchecksum filename
		submitBaseDir := path.Join(svc.ProcessingDir, "bags", regResp.SubmissionIdentifier)
		if reuseSID {
			if pathExists(submitBaseDir) == false {
				svc.logFatal(js, fmt.Sprintf("Prior submission directory %s not found", submitBaseDir))
				return
			}
		} else {
			svc.logInfo(js, fmt.Sprintf("Create new submission base directory %s", submitBaseDir))
			if pathExists(submitBaseDir) {
				svc.logInfo(js, fmt.Sprintf("Clean up pre-existing submission directory %s", submitBaseDir))
				if err := os.RemoveAll(submitBaseDir); err != nil {
					svc.logFatal(js, fmt.Sprintf("Unable to clean up existing submission directory %s: %s", submitBaseDir, err.Error()))
					return
				}
			} else {
				if err := ensureDirExists(submitBaseDir, 0777); err != nil {
					svc.logFatal(js, fmt.Sprintf("Unable to create submission directory %s: %s", submitBaseDir, err.Error()))
					return
				}
			}
		}

		svc.logInfo(js, fmt.Sprintf("Collection %d has %d items; build submission directory for each", md.ID, len(submissionInfo)))
		bagFolderList := make([]string, 0)
		for _, rec := range submissionInfo {
			if err := svc.buildAPTrustSubmissionDirectory(js, submitBaseDir, &rec.Metadata, rec.MasterFiles, reuseSID); err != nil {
				svc.logFatal(js, fmt.Sprintf("Metadata %d APTrust submission setup failed: %s", md.ID, err.Error()))
				return
			} else {
				bagFolderList = append(bagFolderList, getSubmissionDirectoryName(&rec.Metadata))
			}
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

		svc.logInfo(js, "Add submission ID to all metadata records just submitted")
		md.APTrustSubmissionID = regResp.SubmissionIdentifier
		if err := svc.GDB.Model(&md).Update("apt_submission_id", regResp.SubmissionIdentifier).Error; err != nil {
			svc.logError(js, fmt.Sprintf("Unable to add submission id %s to metadata %d: %s", regResp.SubmissionIdentifier, md.ID, err.Error()))
		}
		q := "update metadata set apt_submission_id=? where parent_metadata_id=?"
		if err := svc.GDB.Exec(q, regResp.SubmissionIdentifier, md.ID).Error; err != nil {
			svc.logError(js, fmt.Sprintf("Unable to add submission id %s to child metdata records of parent %d: %s", regResp.SubmissionIdentifier, md.ID, err.Error()))
		}

		svc.logInfo(js, "Cleanup assembly directories")
		if err := os.RemoveAll(submitBaseDir); err != nil {
			svc.logError(js, fmt.Sprintf("Unable to clean up submission directory %s: %s", submitBaseDir, err.Error()))
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) validateAPTrustSubmissionRequest(js *jobStatus, md *metadata) ([]submissionRec, error) {
	if md.IsCollection == false {
		return nil, fmt.Errorf("only collection records can be submitted")
	}

	var out []submissionRec
	svc.logInfo(js, fmt.Sprintf("Load child record IDs from collection %s for APTrust submission", md.PID))
	var inCollectionMD []metadata
	if err := svc.GDB.Where("parent_metadata_id=?", md.ID).Find(&inCollectionMD).Error; err != nil {
		return nil, fmt.Errorf("Unable to load child metadata records for collection %d: %s", md.ID, err.Error())
	}

	var badMD []uint64
	for _, md := range inCollectionMD {
		masterFiles := svc.getBestMasterFiles(js, uint64(md.ID))
		if len(masterFiles) == 0 {
			badMD = append(badMD, uint64(md.ID))
		}
		out = append(out, submissionRec{Metadata: md, MasterFiles: masterFiles})
		time.Sleep(100 * time.Millisecond)
	}
	if len(badMD) > 0 {
		return nil, fmt.Errorf("These metadata records have no masterfiles suitable for submission to aptrust: %v", badMD)
	}

	return out, nil
}

func (svc *ServiceContext) buildAPTrustSubmissionDirectory(js *jobStatus, submitBaseDir string, md *metadata, masterFiles []masterFile, isRetry bool) error {
	svc.logInfo(js, fmt.Sprintf("Build APTrust submission directory for metadata %d", md.ID))
	mdDirName := getSubmissionDirectoryName(md)
	submitAssembleDir := path.Join(submitBaseDir, mdDirName)

	if isRetry {
		if pathExists(submitAssembleDir) == false {
			svc.logInfo(js, fmt.Sprintf("Submission directory is missing %s; regenerate it", submitAssembleDir))
		} else {
			// check contents of directory. Should have all masterfile .tif images, manifest-md5.txt,  aptrust-title.txt {PID_WITH UNDERSCORE}.xml
			mdName := fmt.Sprintf("%s.xml", strings.ReplaceAll(md.PID, ":", "_"))
			if md.ExternalSystemID == 1 {
				mdName = "metadata.json"
			}

			var fileNames []string
			fileNames = append(fileNames, "aptrust-title.txt")
			fileNames = append(fileNames, mdName)
			fileNames = append(fileNames, "manifest-md5.txt")
			for _, mf := range masterFiles {
				fileNames = append(fileNames, mf.Filename)
			}

			incomplete := false
			for _, fn := range fileNames {
				testFN := path.Join(submitAssembleDir, fn)
				if pathExists(testFN) == false {
					svc.logInfo(js, fmt.Sprintf("Existing submission directory is missing file %s; regenerate submission", testFN))
					incomplete = true
					break
				}
				if getFileSize(testFN) == 0 {
					svc.logInfo(js, fmt.Sprintf("Existing submission file %s is 0 length; regenerate submission", testFN))
					incomplete = true
					break
				}
			}
			if incomplete {
				if err := os.RemoveAll(submitAssembleDir); err != nil {
					svc.logError(js, fmt.Sprintf("Unable to clean up submission directory %s: %s", submitAssembleDir, err.Error()))
				}
			} else {
				svc.logInfo(js, fmt.Sprintf("Submission directory %s exists and is complete. Nothong to do", submitAssembleDir))
				return nil
			}
		}
	}

	if err := ensureDirExists(submitAssembleDir, 0777); err != nil {
		return fmt.Errorf("unable to create submission directory %s: %s", submitAssembleDir, err.Error())
	}

	// init a map of filename => checksum
	checksums := make(map[string]string, 0)

	// add aptrust-title.txt with with content being the title of the md record...
	titlePath := filepath.Join(submitAssembleDir, "aptrust-title.txt")
	if err := os.WriteFile(titlePath, []byte(md.Title), 0644); err != nil {
		svc.logError(js, fmt.Sprintf("unable to create %s: %s", titlePath, err.Error()))
	} else {
		titleMD5 := md5Checksum(titlePath)
		checksums["aptrust-title.txt"] = titleMD5
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
		checksums["metadata.json"] = md5
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
		checksums[mdName] = md5
	}

	for _, mf := range masterFiles {
		svc.logInfo(js, fmt.Sprintf("Adding masterfile %s", mf.Filename))
		archiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", mf.UnitID), mf.Filename)
		destFile := path.Join(submitAssembleDir, mf.Filename)
		exists := pathExists(archiveFile)
		if exists == false {
			svc.logInfo(js, fmt.Sprintf("%s not found; check for non-standard storage location", archiveFile))
			if strings.Contains(mf.Filename, "ARCH") || strings.Contains(mf.Filename, "AVRN") || strings.Contains(mf.Filename, "VRC") {
				if strings.Contains(mf.Filename, "_") {
					overrideDir := strings.Split(mf.Filename, "_")[0]
					archiveFile = path.Join(svc.ArchiveDir, overrideDir, mf.Filename)
					exists = pathExists(archiveFile)
				}
			}
		}

		if exists == false {
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
		checksums[mf.Filename] = md5
	}

	md5FileName := path.Join(submitAssembleDir, "manifest-md5.txt")
	svc.logInfo(js, fmt.Sprintf("Create manifest %s", md5FileName))
	md5Data := ""
	for fn, md5 := range checksums {
		md5Data += fmt.Sprintf("%s %s\n", md5, fn)
	}
	os.WriteFile(md5FileName, []byte(md5Data), 0644)

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
	source := fmt.Sprintf("s3://%s/%s", reg.DepositBucket, reg.DepositPath)
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
