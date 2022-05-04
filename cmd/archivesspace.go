package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type externalSystem struct {
	ID     int64
	Name   string
	APIURL string `gorm:"column:api_url"`
}

type asObjectDetails map[string]interface{}

type asDigitalObject struct {
	PID     string `json:"pid"`
	Title   string `json:"title"`
	IIIF    string `json:"iiif"`
	Created string `json:"created"`
}

type asRepoInfo struct {
	RepositoryID string
	ParentType   string
	ParentID     string
}

type asRepository struct {
	Name string
	Slug string
	URI  string
}

func (r *asRepository) ID() string {
	parts := strings.Split(r.URI, "/")
	return parts[len(parts)-1]
}

func (svc *ServiceContext) archivesSpaceMiddleware(c *gin.Context) {
	log.Printf("INFO: ensure archivesspace auth token exists for %s", c.Request.URL)
	now := time.Now()
	exp := time.Now()
	exp.Add(30 * time.Minute)
	if svc.ArchivesSpace.AuthToken == "" || svc.ArchivesSpace.AuthToken != "" && now.After(svc.ArchivesSpace.ExpiresAt) {
		authURL := fmt.Sprintf("%s/users/%s/login", svc.ArchivesSpace.APIURL, svc.ArchivesSpace.User)
		payload := url.Values{}
		payload.Add("password", svc.ArchivesSpace.Pass)
		resp, authErr := svc.postFormRequest(authURL, &payload)
		if authErr != nil {
			log.Printf("ERROR: auth post failed: %d:%s", authErr.StatusCode, authErr.Message)
			c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("%d:%s", authErr.StatusCode, authErr.Message))
			return
		}
		jsonResp := struct {
			Session string `json:"session"`
		}{}
		err := json.Unmarshal(resp, &jsonResp)
		if err != nil {
			log.Printf("ERROR: unable to parse auth response:%s", err.Error())
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}
		svc.ArchivesSpace.AuthToken = jsonResp.Session
		svc.ArchivesSpace.ExpiresAt = exp
	}

	c.Next()
}

func (svc *ServiceContext) validateArchivesSpaceURL(c *gin.Context) {
	asURL := c.Query("url")
	log.Printf("INFO: validate archivesspace url %s", asURL)

	// format is: (https://archives/lib.virginia.edu)/repositories/[repo_id]/[archival_objects|accessions|resources]/[object_id]
	// the part up to repositories is optional
	repoIdx := strings.Index(asURL, "/repositories")
	if repoIdx > 0 {
		asURL = asURL[repoIdx+1:]
	}
	parts := strings.Split(asURL, "/")
	if len(parts) == 5 && parts[0] == "" {
		parts = parts[1:]
	} else if len(parts) != 4 {
		log.Printf("INFO: url %s is malformed", asURL)
		c.String(http.StatusBadRequest, "url is malformed")
		return
	}

	// check for numeric repo
	log.Printf("INFO: validate repository ID [%s]", parts[1])
	partVal, _ := strconv.Atoi(parts[1])
	if partVal == 0 {
		log.Printf("Repo is not numeric, lookup existing repos")
		repos, err := svc.getASRepositories()
		if err != nil {
			log.Printf("ERROR: get repositories failed: %s", err.Error())
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		// tgt_repo_id = nil
		match := false
		for _, r := range repos {
			if r.Slug == parts[1] {
				match = true
				parts[1] = r.ID()
				break
			}
		}
		if match == false {
			log.Printf("INFO: invalid repository %s", parts[1])
			c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid repository", parts[1]))
			return
		}
	}

	// make sure only supported object types are listed
	log.Printf("INFO: Validate object type")
	supported := []string{"archival_objects", "accessions", "resources"}
	match := false
	for _, o := range supported {
		if o == parts[2] {
			match = true
			break
		}
	}
	if match == false {
		log.Printf("INFO: invalid object type %s", parts[1])
		c.String(http.StatusBadRequest, "only archival_objects, accessions and resources are supported")
		return
	}

	// check for numeric object id
	log.Printf("Validate object ID")
	partVal, _ = strconv.Atoi(parts[3])
	if partVal == 0 {
		log.Printf("ObjectID [%s] is not numeric, search for match", parts[3])
		slugID, err := svc.lookupASObjectSlug(parts[1], parts[3])
		if err != nil {
			log.Printf("INFO: invalid object slug %s: %s", parts[3], err.Error())
			c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid object slug", parts[3]))
			return
		}
		parts[3] = slugID
	}

	// now join all the corrected parts of the URL and see if we can pull AS data for it
	outURL := "/" + strings.Join(parts, "/")
	log.Printf("validated url: %s", outURL)
	c.String(http.StatusOK, outURL)
}

func (svc *ServiceContext) convertToArchivesSpace(c *gin.Context) {
	type convReqData struct {
		UserID     int64  `json:"userId"`
		MetadataID int64  `json:"metadataID"`
		ASURL      string `json:"asURL"`
	}
	var req convReqData
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: bad request to convert item to as: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	js, err := svc.createJobStatus("ConvertToAs", "StaffMember", req.UserID)
	if err != nil {
		log.Printf("ERROR: unable to create ConvertToAs job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("%v", r))
			}
		}()

		svc.logInfo(js, fmt.Sprintf("Convert TrackSys metadata %d to External ArchivesSpace referece %s", req.MetadataID, req.ASURL))
		var tgtMetadata metadata
		err = svc.GDB.Find(&tgtMetadata, req.MetadataID).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to get metadata %d", req.MetadataID))
			return
		}
		asInfo := parsePublicASURL(req.ASURL)
		if asInfo == nil {
			svc.logFatal(js, fmt.Sprintf("%s is not a valid public AS URL", req.ASURL))
			return
		}

		tgtASObj, err := svc.getASDetails(js, asInfo)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("%s:%s not found in repo %s", asInfo.ParentType, asInfo.ParentID, asInfo.RepositoryID))
			return
		}

		dObj := svc.getDigitalObject(js, tgtASObj, tgtMetadata.PID)
		if dObj != nil {
			svc.logInfo(js, fmt.Sprintf("%s:%s already has digital object. Use existing.", asInfo.ParentType, asInfo.ParentID))
		} else {
			svc.logInfo(js, fmt.Sprintf("No digital object found; creating one for PID %s", tgtMetadata.PID))
			err = svc.createDigitalObject(js, asInfo.RepositoryID, tgtASObj, &tgtMetadata)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to create digital object %s", err.Error()))
				return
			}
		}

		if tgtMetadata.Type != "ExternalMetadata" {
			svc.logInfo(js, fmt.Sprintf("Converting existing metadata record %s to ExternalMetadata", tgtMetadata.PID))
			var es externalSystem
			err = svc.GDB.Where("name=?", "ArchivesSpace").Find(&es).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to get archivesspace external system data %s", err.Error()))
				return
			}
			tgtMetadata.Type = "ExternalMetadata"
			tgtMetadata.ExternalURI = fmt.Sprintf("/repositories/%s/%s/%s", asInfo.RepositoryID, asInfo.ParentType, asInfo.ParentID)
			tgtMetadata.CreatorName = ""
			tgtMetadata.CatalogKey = ""
			tgtMetadata.CallNumber = ""
			tgtMetadata.Barcode = ""
			tgtMetadata.DescMetadata = ""
			tgtMetadata.ExternalSystemID = es.ID
			err := svc.GDB.Model(&tgtMetadata).
				Select("Type", "ExternalSystemID", "ExternalURI", "CreatorName", "CatalogKey", "CallNumber", "Barcode", "DescMetadata").
				Updates(tgtMetadata).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to convert %s to ArchivesSpace: %s", tgtMetadata.PID, err.Error()))
				return
			}
		} else {
			svc.logInfo(js, fmt.Sprintf("Metadata record %s is already ExternalMetadata", tgtMetadata.PID))
		}

		svc.logInfo(js, "ArchivesSpace link successfully created")
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) getDigitalObject(js *jobStatus, tgtObj asObjectDetails, metadataPID string) *asDigitalObject {
	svc.logInfo(js, fmt.Sprintf("Look for existing digitial object for %s", metadataPID))
	val2 := tgtObj["instances"]
	instancesCopy, ok := val2.([]interface{})
	if ok == false {
		svc.logInfo(js, fmt.Sprintf("No digitial object exists for %s", metadataPID))
		return nil
	}
	for _, instIface := range instancesCopy {
		inst, ok := instIface.(map[string]interface{})
		if ok == false {
			svc.logError(js, fmt.Sprintf("Unable to parse AS object instance data %+v", instIface))
			continue
		}
		if dobjIface, ok := inst["digital_object"]; ok {
			dobj, ok := dobjIface.(map[string]interface{})
			if !ok {
				continue
			}
			doURL := fmt.Sprintf("%v", dobj["ref"])
			doBytes, reqErr := svc.sendASGetRequest(doURL)
			if reqErr != nil {
				svc.logError(js, fmt.Sprintf("Unable to get digital object info: %s", reqErr.Message))
				continue
			}
			var doJSON map[string]interface{}
			err := json.Unmarshal(doBytes, &doJSON)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to parse digital object %s response: %s", doURL, err.Error()))
				continue
			}

			doPID := fmt.Sprintf("%v", doJSON["digital_object_id"])
			if doPID == metadataPID {
				out := asDigitalObject{
					PID:     doPID,
					Title:   fmt.Sprintf("%v", doJSON["title"]),
					Created: fmt.Sprintf("%v", doJSON["create_time"]),
				}

				fvIface := doJSON["file_versions"]
				if fvIface == nil {
					continue
				}
				fv, ok := fvIface.([]interface{})
				if ok == false {
					svc.logError(js, fmt.Sprintf("Unable to parse file_versions for digital object %s", doURL))
					continue
				}

				tgtVersionIFace := fv[0]
				tgtVersion, ok := tgtVersionIFace.(map[string]interface{})
				if ok == false {
					svc.logError(js, fmt.Sprintf("Unable to parse version for digital object %s", doURL))
					continue
				}
				out.IIIF = fmt.Sprintf("%s", tgtVersion["file_uri"])

				return &out
			}
		}
	}
	return nil
}

func (svc *ServiceContext) createDigitalObject(js *jobStatus, repoID string, tgtObj asObjectDetails, tgtMetadata *metadata) error {
	iiifURL := fmt.Sprintf("%s/pid/%s/exist", svc.IIIF.URL, tgtMetadata.PID)
	bytes, iiifErr := svc.getRequest(iiifURL)
	if iiifErr != nil {
		return fmt.Errorf("Unable to get IIIF manifest for %s: %d%s", tgtMetadata.PID, iiifErr.StatusCode, iiifErr.Message)
	}

	iiifResp := struct {
		Cached bool   `json:"cached"`
		Exists bool   `json:"exists"`
		URL    string `json:"url"`
	}{}
	json.Unmarshal(bytes, &iiifResp)
	if iiifResp.Cached == false {
		return fmt.Errorf("ArchivesSpace create DigitalObject could not find cached IIIF manifest")
	}

	type doFileVersion struct {
		UseStatement string `json:"use_statement"`
		FileURI      string `json:"file_uri"`
		Publish      bool   `json:"publish"`
	}
	type doPayload struct {
		DigitalObjectID string          `json:"digital_object_id"`
		Title           string          `json:"title"`
		Publish         bool            `json:"publish"`
		FileVersions    []doFileVersion `json:"file_versions"`
	}
	uri := fmt.Sprintf("%s/pid/%s", svc.IIIF.URL, tgtMetadata.PID)
	payload := doPayload{DigitalObjectID: tgtMetadata.PID, Title: tgtMetadata.Title, FileVersions: make([]doFileVersion, 0)}
	payload.FileVersions = append(payload.FileVersions, doFileVersion{UseStatement: "image-service-manifest", FileURI: uri})
	resp, asErr := svc.sendASPostRequest(fmt.Sprintf("/repositories/%s/digital_objects", repoID), payload)
	if asErr != nil {
		return fmt.Errorf("ArchivesSpace create DigitalObject request failed: %d:%s", asErr.StatusCode, asErr.Message)
	}
	log.Printf("%s", resp)
	createJSON := struct {
		ID int64 `json:"id"`
	}{}
	err := json.Unmarshal(resp, &createJSON)
	if err != nil {
		return fmt.Errorf("Unable to parse create response: %s", err.Error())
	}

	// Add newly created digital object URI reference as an instance in the target archival object
	svc.logInfo(js, fmt.Sprintf("Add newly created digital object %d to parent", createJSON.ID))
	doInst := make(map[string]interface{})
	doRef := make(map[string]interface{})
	doRef["ref"] = fmt.Sprintf("/repositories/%s/digital_objects/%d", repoID, createJSON.ID)
	doInst["instance_type"] = "digital_object"
	doInst["digital_object"] = doRef

	// get a copy of the instances array, append the instance of the new digital object, then replaces the instances with the new version
	instIface := tgtObj["instances"]
	instancesCopy, ok := instIface.([]interface{})
	if ok == false {
		return fmt.Errorf("Unable to get instances data from parent object")
	}
	instancesCopy = append(instancesCopy, doInst)
	tgtObj["instances"] = instancesCopy

	// Update the original item with the instance
	_, asErr = svc.sendASPostRequest(fmt.Sprintf("%s", tgtObj["uri"]), tgtObj)
	if asErr != nil {
		return fmt.Errorf("ArchivesSpace update parent failed: %d:%s", asErr.StatusCode, asErr.Message)
	}
	return nil
}

func (svc *ServiceContext) getASDetails(js *jobStatus, asInfo *asRepoInfo) (asObjectDetails, error) {
	svc.logInfo(js, fmt.Sprintf("Get details for %+v", *asInfo))
	var respBytes []byte
	if asInfo.ParentType == "resources" {
		svc.logInfo(js, fmt.Sprintf("Looking up parent resource %s in repo %s...", asInfo.ParentID, asInfo.RepositoryID))
		url := fmt.Sprintf("/repositories/%s/resources/%s", asInfo.RepositoryID, asInfo.ParentID)
		resp, err := svc.sendASGetRequest(url)
		if err != nil {
			return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
		}
		respBytes = resp
	} else if asInfo.ParentType == "archival_objects" {
		svc.logInfo(js, fmt.Sprintf("Looking up parent archival object %s in repo %s...", asInfo.ParentID, asInfo.RepositoryID))
		url := fmt.Sprintf("/repositories/%s/archival_objects/%s", asInfo.RepositoryID, asInfo.ParentID)
		resp, err := svc.sendASGetRequest(url)
		if err != nil {
			return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
		}
		respBytes = resp
	} else if asInfo.ParentType == "accessions" {
		svc.logInfo(js, fmt.Sprintf("Looking up parent accession %s in repo %s...", asInfo.ParentID, asInfo.RepositoryID))
		url := fmt.Sprintf("/repositories/%s/accessions/%s", asInfo.RepositoryID, asInfo.ParentID)
		resp, err := svc.sendASGetRequest(url)
		if err != nil {
			return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
		}
		respBytes = resp
	} else {
		return nil, fmt.Errorf("Unsupported parent type: %s", asInfo.ParentType)
	}

	var out asObjectDetails
	err := json.Unmarshal(respBytes, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (svc *ServiceContext) getASRepositories() ([]asRepository, error) {
	var out []asRepository

	resp, err := svc.sendASGetRequest("/repositories")
	if err != nil {
		return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
	}
	jsonErr := json.Unmarshal(resp, &out)
	if jsonErr != nil {
		return nil, jsonErr
	}

	return out, nil
}

func (svc *ServiceContext) lookupASObjectSlug(repoID, slug string) (string, error) {
	resp, asErr := svc.sendASGetRequest(fmt.Sprintf("/repositories/%s/search?q=%s&page=1", repoID, slug))
	if asErr != nil {
		return "", fmt.Errorf("%d:%s", asErr.StatusCode, asErr.Message)
	}
	parsed := struct {
		TotalHits int `json:"total_hits"`
		Results   []struct {
			URI string `json:"uri"`
		} `json:"results"`
	}{}
	err := json.Unmarshal(resp, &parsed)
	if err != nil {
		return "", err
	}
	if parsed.TotalHits != 1 {
		return "", fmt.Errorf("%s has more than one match", slug)
	}
	parts := strings.Split(parsed.Results[0].URI, "/")
	return parts[len(parts)-1], nil
}

func parsePublicASURL(asURL string) *asRepoInfo {
	//public AS urls look like this:
	//   https://archives.lib.virginia.edu/repositories/3/archival_objects/62839
	//OR Relative:
	//   /repositories/3/archival_objects/62839
	//only care about the repoID, object type and objID
	bits := strings.Split(asURL, "/")
	if len(bits) >= 4 {
		out := asRepoInfo{
			RepositoryID: bits[len(bits)-3],
			ParentType:   bits[len(bits)-2],
			ParentID:     bits[len(bits)-1],
		}
		return &out
	}
	return nil
}
