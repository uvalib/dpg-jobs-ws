package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type externalSystem struct {
	ID        int64
	Name      string
	APIURL    string `gorm:"column:api_url"`
	PublicURL string `gorm:"column:public_url"`
}

type asObjectDetails map[string]interface{}

type asMetadataResponse struct {
	Title           string `json:"title"`
	CreatedBy       string `json:"created_by"`
	CreateTime      string `json:"create_time"`
	Level           string `json:"level"`
	URL             string `json:"url"`
	Dates           string `json:"dates,omitempty"`
	PublishedAt     string `json:"published_at,omitempty"`
	Repo            string `json:"repo"`
	CollectionID    string `json:"collection_id"`
	CollectionTitle string `json:"collection_title"`
	Language        string `json:"language,omitempty"`
}

type asDigitalObject struct {
	ID      string `json:"id"`
	PID     string `json:"pid"`
	Title   string `json:"title"`
	IIIF    string `json:"iiif"`
	Created string `json:"created"`
}

type asURLInfo struct {
	RepositoryID string
	ParentType   string
	ParentID     string
}

type asRepository struct {
	Name string
	Slug string
	URI  string
}

type asOrderedRecordsResp struct {
	URIS []struct {
		Ref     string `json:"ref"`
		Display string `json:"display_string"`
		Depth   int64  `json:"depth"`
		Level   string `json:"levet"`
	} `json:"uris"`
}

type asSearchResp struct {
	TotalHits int64 `json:"total_hits"`
	Ressults  []struct {
		ID    string `json:"id"`
		URI   string `json:"uri"`
		Title string `json:"title"`
	} `json:"results"`
}

func (r *asRepository) ID() string {
	parts := strings.Split(r.URI, "/")
	return parts[len(parts)-1]
}

func (svc *ServiceContext) archivesSpaceMiddleware(c *gin.Context) {
	log.Printf("INFO: ensure archivesspace auth token exists for %s", c.Request.URL)
	err := svc.validateArchivesSpaceAccessToken()
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	c.Next()
}

func (svc *ServiceContext) validateArchivesSpaceAccessToken() error {
	now := time.Now()
	exp := time.Now()
	exp = exp.Add(30 * time.Minute)
	if svc.ArchivesSpace.AuthToken == "" || svc.ArchivesSpace.AuthToken != "" && now.After(svc.ArchivesSpace.ExpiresAt) {
		authURL := fmt.Sprintf("%s/users/%s/login", svc.ArchivesSpace.APIURL, svc.ArchivesSpace.User)
		log.Printf("INFO: archivesspace token missing or expired, requesting a new one with: %s", authURL)
		payload := url.Values{}
		payload.Add("password", svc.ArchivesSpace.Pass)
		resp, authErr := svc.postFormRequest(authURL, &payload)
		if authErr != nil {
			return fmt.Errorf("archivesspace auth post failed: %d:%s", authErr.StatusCode, authErr.Message)
		}
		jsonResp := struct {
			Session string `json:"session"`
		}{}
		err := json.Unmarshal(resp, &jsonResp)
		if err != nil {
			return fmt.Errorf("invalid auth response: %s", err.Error())
		}
		svc.ArchivesSpace.AuthToken = jsonResp.Session
		svc.ArchivesSpace.ExpiresAt = exp
	}
	return nil
}

func (svc *ServiceContext) getArchivesSpaceCollectionURIs(c *gin.Context) {
	collectionID := c.Param("id")
	log.Printf("INFO: get archivesspace collection %s record uris", collectionID)
	url2 := fmt.Sprintf("/repositories/3/resources/%s/ordered_records", collectionID)
	ao, err := svc.sendASGetRequest(url2)
	if err != nil {
		log.Printf("ERROR: as collection %s records request failed: %s", collectionID, err.Message)
		c.String(http.StatusInternalServerError, err.Message)
		return
	}
	var objRecs asOrderedRecordsResp
	jsonErr := json.Unmarshal(ao, &objRecs)
	if jsonErr != nil {
		log.Printf("ERROR: unable to parse as response: %s", jsonErr.Error())
		c.String(http.StatusInternalServerError, jsonErr.Error())
		return
	}

	out := ""
	for _, item := range objRecs.URIS {
		if strings.Contains(item.Ref, collectionID) == false {
			out += fmt.Sprintf("%s\n", item.Ref)
		}
	}
	c.String(http.StatusOK, out)
}

func (svc *ServiceContext) lookupArchivesSpaceURL(c *gin.Context) {
	tgtURI := c.Query("uri")
	tgtPID := c.Query("pid")
	log.Printf("INFO: lookup details for aSpace uri %s", tgtURI)

	asURL := parsePublicASURL(tgtURI)
	if asURL == nil {
		log.Printf("INFO: %s is not a valid aSpace URL", tgtURI)
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid aSpace URL", tgtURI))
		return
	}

	out, err := svc.getArchivesSpaceMetadata(asURL, tgtPID)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, out)
}

func (svc *ServiceContext) getArchivesSpaceMetadata(asURL *asURLInfo, tgtPID string) (*asMetadataResponse, error) {
	var es externalSystem
	err := svc.GDB.Where("name=?", "ArchivesSpace").Find(&es).Error
	if err != nil {
		return nil, fmt.Errorf("unable to get external system data: %s", err.Error())
	}

	tgtASObj, err := svc.getASDetails(nil, asURL)
	if err != nil {
		return nil, fmt.Errorf("%s:%s not found in repo %s", asURL.ParentType, asURL.ParentID, asURL.RepositoryID)
	}

	out := asMetadataResponse{
		URL:        fmt.Sprintf("%s/repositories/%s/%s/%s", es.PublicURL, asURL.RepositoryID, asURL.ParentType, asURL.ParentID),
		CreatedBy:  fmt.Sprintf("%v", tgtASObj["created_by"]),
		CreateTime: fmt.Sprintf("%v", tgtASObj["create_time"]),
		Level:      fmt.Sprintf("%v", tgtASObj["level"]),
	}

	if out.Level == "collection" || tgtASObj["resource_type"] == "collection" {
		out.CollectionID = fmt.Sprintf("%v", tgtASObj["id_0"])
		if tgtASObj["id_1"] != nil {
			out.CollectionID += fmt.Sprintf(" %v", tgtASObj["id_1"])
		}
		if tgtASObj["id_2"] != nil {
			out.CollectionID += fmt.Sprintf("-%v", tgtASObj["id_2"])
		}
	}

	if tgtASObj["title"] != nil {
		out.Title = fmt.Sprintf("%v", tgtASObj["title"])
	} else {
		out.Title = fmt.Sprintf("%v", tgtASObj["display_string"])
	}
	if tgtASObj["finding_aid_language_note"] != nil {
		out.Language = fmt.Sprintf("%v", tgtASObj["finding_aid_language_note"])
	}

	if tgtASObj["dates"] != nil {
		dates := tgtASObj["dates"].([]interface{})
		if len(dates) > 0 {
			tgtDate := dates[0].(map[string]interface{})
			if tgtDate["expression"] != nil {
				out.Dates = fmt.Sprintf("%v", tgtDate["expression"])
			} else if tgtDate["begin"] != nil {
				out.Dates = fmt.Sprintf("%v", tgtDate["begin"])
			}
		}
	}

	log.Printf("INFO: Lookup repository name")
	resp, asErr := svc.sendASGetRequest(fmt.Sprintf("/repositories/%s", asURL.RepositoryID))
	if asErr != nil {
		return nil, fmt.Errorf("unable to get repoisitory %s info: %s", asURL.RepositoryID, asErr.Message)
	}
	var repo map[string]interface{}
	json.Unmarshal(resp, &repo)
	out.Repo = fmt.Sprintf("%v", repo["name"])

	if tgtPID != "" {
		dObj := svc.getDigitalObject(nil, tgtASObj, tgtPID)
		if dObj != nil {
			out.PublishedAt = dObj.Created
		}
	}

	if tgtASObj["finding_aid_title"] != nil {
		ft := fmt.Sprintf("%v", tgtASObj["finding_aid_title"])
		out.CollectionTitle = strings.Split(ft, "<num")[0]
	}

	if tgtASObj["ancestors"] != nil {
		log.Printf("INFO: Record has ancestors; looking up details")
		ancIface := tgtASObj["ancestors"]
		ancestors := ancIface.([]interface{})
		ancestor := ancestors[len(ancestors)-1].(map[string]interface{})
		colBytes, asErr := svc.sendASGetRequest(fmt.Sprintf("%v", ancestor["ref"]))
		if asErr != nil {
			log.Printf("WARNING: Unable to get ancestor info: %s", asErr.Message)
		} else {
			var coll asObjectDetails
			json.Unmarshal(colBytes, &coll)
			out.CollectionID = fmt.Sprintf("%v", coll["id_0"])
			if coll["id_1"] != nil {
				out.CollectionID += fmt.Sprintf(" %v", coll["id_1"])
			}
			if coll["id_2"] != nil {
				out.CollectionID += fmt.Sprintf("-%v", coll["id_2"])
			}
			if coll["finding_aid_title"] != nil {
				ft := fmt.Sprintf("%v", coll["finding_aid_title"])
				out.CollectionTitle = strings.Split(ft, "<num")[0]
			} else if coll["title"] != nil {
				out.CollectionTitle = fmt.Sprintf("%v", coll["title"])
			}
		}
	}

	return &out, nil
}

func (svc *ServiceContext) validateArchivesSpaceURL(c *gin.Context) {
	asURL := c.Query("url")
	log.Printf("INFO: validate archivesspace url %s", asURL)

	// format is: (https://archives/lib.virginia.edu)/repositories/[repo_id]/[archival_objects|resources]/[object_id]
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
	supported := []string{"archival_objects", "resources"}
	match := false
	for _, o := range supported {
		if o == parts[2] {
			match = true
			break
		}
	}
	if match == false {
		log.Printf("INFO: invalid object type %s", parts[1])
		c.String(http.StatusBadRequest, "only archival_objects and resources are supported")
		return
	}

	// check for numeric object id
	log.Printf("Validate object ID")
	partVal, _ = strconv.Atoi(parts[3])
	if partVal == 0 {
		log.Printf("ObjectID [%s] is not numeric, search for match", parts[3])
		slugID, err := svc.lookupASObjectSlug(parts[1], parts[2], parts[3])
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

func (svc *ServiceContext) unpublishArchivesSpace(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("UnpublishArchivesSpace", "Metadata", mdID)
	if err != nil {
		log.Printf("ERROR: unable to create UnpublishArchivesSpace job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	svc.logInfo(js, fmt.Sprintf("Unublish metadata %d from ArchivesSpace", mdID))

	// grab he metadata record and parse the external_url to get the repo and parent info
	var tgtMetadata metadata
	err = svc.GDB.Find(&tgtMetadata, mdID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to get metadata %d", mdID))
		return
	}
	asURL := parsePublicASURL(tgtMetadata.ExternalURI)
	if asURL == nil {
		svc.logFatal(js, fmt.Sprintf("Metadata contains an invalid ArchivesSpace URL: %s", tgtMetadata.ExternalURI))
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid aSpace URL", tgtMetadata.ExternalURI))
		return
	}
	log.Printf("%+v", *asURL)

	svc.logInfo(js, fmt.Sprintf("Lookup existing digital object info from %s", tgtMetadata.ExternalURI))
	tgtASObj, err := svc.getASDetails(js, asURL)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("%s:%s not found in repo %s", asURL.ParentType, asURL.ParentID, asURL.RepositoryID))
		c.String(http.StatusBadRequest, fmt.Sprintf("%s was not found in aSpace", tgtMetadata.ExternalURI))
		return
	}

	dObj := svc.getDigitalObject(js, tgtASObj, tgtMetadata.PID)
	if dObj == nil {
		svc.logFatal(js, fmt.Sprintf("No digital object exists for %s", tgtMetadata.ExternalURI))
		c.String(http.StatusBadRequest, fmt.Sprintf("no digital object for %s", tgtMetadata.ExternalURI))
		return
	}
	// [:DELETE] /repositories/:repo_id/digital_objects/:id
	delURI := fmt.Sprintf("/repositories/%s/digital_objects/%s", asURL.RepositoryID, dObj.ID)
	svc.logInfo(js, fmt.Sprintf("Delete digital object request: %s", delURI))
	_, reqErr := svc.sendASDeleteRequest(delURI)
	if reqErr != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to delete digital object: %s", reqErr.Message))
		c.String(http.StatusInternalServerError, fmt.Sprintf("delete failed: %s", reqErr.Message))
		return
	}

	// once the object has been deleted, the record it was attached to must be re-published. There are two endpoints:
	// [:POST] /repositories/:repo_id/resources/:id/publish
	// [:POST] /repositories/:repo_id/archival_objects/:id/publish
	svc.logInfo(js, fmt.Sprintf("Publish the parent of the digital object to make the deletion public: %s %s", asURL.ParentType, asURL.ParentID))
	pubURL := fmt.Sprintf("/repositories/%s/%s/%s/publish", asURL.RepositoryID, asURL.ParentType, asURL.ParentID)
	_, reqErr = svc.sendASPostRequest(pubURL, nil)
	if reqErr != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to delete digital object: %s", reqErr.Message))
		c.String(http.StatusInternalServerError, fmt.Sprintf("delete failed: %s", reqErr.Message))
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "deleted")
}

func (svc *ServiceContext) publishToArchivesSpace(c *gin.Context) {
	type pubReqData struct {
		UserID     int64 `json:"userID"`
		MetadataID int64 `json:"metadataID"`
	}
	var req pubReqData
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: bad request to publish item to as: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	js, err := svc.createJobStatus("PublishToAS", "Metadata", req.MetadataID)
	if err != nil {
		log.Printf("ERROR: unable to create PublishToAS job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	svc.logInfo(js, fmt.Sprintf("Publish TrackSys metadata %d to ArchivesSpace by staff member %d", req.MetadataID, req.UserID))
	var tgtMetadata metadata
	err = svc.GDB.Find(&tgtMetadata, req.MetadataID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to get metadata %d", req.MetadataID))
		return
	}

	asURL := parsePublicASURL(tgtMetadata.ExternalURI)
	if asURL == nil {
		svc.logFatal(js, fmt.Sprintf("Metadata contains an invalid ArchivesSpace URL: %s", tgtMetadata.ExternalURI))
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a valid aSpace URL", tgtMetadata.ExternalURI))
		return
	}

	tgtASObj, err := svc.getASDetails(js, asURL)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("%s:%s not found in repo %s", asURL.ParentType, asURL.ParentID, asURL.RepositoryID))
		c.String(http.StatusBadRequest, fmt.Sprintf("%s was not found in aSpace", tgtMetadata.ExternalURI))
		return
	}

	dObj := svc.getDigitalObject(js, tgtASObj, tgtMetadata.PID)
	if dObj != nil {
		svc.logInfo(js, fmt.Sprintf("%s:%s already has digital object. Nothing more to do.", asURL.ParentType, asURL.ParentID))
	} else {
		svc.logInfo(js, fmt.Sprintf("Creating aSpace digital object for  %s", tgtMetadata.PID))
		err = svc.createDigitalObject(js, asURL.RepositoryID, tgtASObj, &tgtMetadata)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to create digital object %s", err.Error()))
			c.String(http.StatusBadRequest, fmt.Sprintf("Unable to create digital object %s", err.Error()))
			return
		}
		svc.logInfo(js, fmt.Sprintf("Set DateDlIngest for metadata %s", tgtMetadata.PID))
		now := time.Now()
		tgtMetadata.DateDlIngest = &now
		err = svc.GDB.Model(&tgtMetadata).Select("DateDlIngest").Updates(tgtMetadata).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update published date %s", err.Error()))
		}
	}

	svc.logInfo(js, "ArchivesSpace publish complete")
	svc.jobDone(js)
	c.String(http.StatusOK, "done")
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
				svc.logInfo(js, fmt.Sprintf("Found digital object for %s: %v", metadataPID, doJSON))
				out := asDigitalObject{
					PID:     doPID,
					Title:   fmt.Sprintf("%v", doJSON["title"]),
					Created: fmt.Sprintf("%v", doJSON["create_time"]),
				}

				doURI := fmt.Sprintf("%v", doJSON["uri"])
				bits := strings.Split(doURI, "/")
				out.ID = bits[len(bits)-1]

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
	svc.logInfo(js, fmt.Sprintf("Generate IIIF manifest for metadata %s", tgtMetadata.PID))
	iiifURL := fmt.Sprintf("%s/pid/%s?refresh=true", svc.IIIF.ManifestURL, tgtMetadata.PID)
	_, errResp := svc.getRequest(iiifURL)
	if errResp != nil {
		return fmt.Errorf("Unable to generate IIIF manifest: %d: %s", errResp.StatusCode, errResp.Message)
	}

	svc.logInfo(js, "IIIF manifest successfully generated, get cached manifest URL")
	iiifURL = fmt.Sprintf("%s/pid/%s/exist", svc.IIIF.ManifestURL, tgtMetadata.PID)
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
	uri := fmt.Sprintf("%s/pid/%s", svc.IIIF.ManifestURL, tgtMetadata.PID)
	payload := doPayload{DigitalObjectID: tgtMetadata.PID, Title: tgtMetadata.Title, Publish: true, FileVersions: make([]doFileVersion, 0)}
	payload.FileVersions = append(payload.FileVersions, doFileVersion{UseStatement: "image-service-manifest", FileURI: uri, Publish: true})
	resp, asErr := svc.sendASPostRequest(fmt.Sprintf("/repositories/%s/digital_objects", repoID), payload)
	if asErr != nil {
		return fmt.Errorf("ArchivesSpace create DigitalObject request failed: %d:%s", asErr.StatusCode, asErr.Message)
	}
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

func (svc *ServiceContext) getASDetails(js *jobStatus, asURL *asURLInfo) (asObjectDetails, error) {
	svc.logInfo(js, fmt.Sprintf("Get details for /repositories/%s/%s/%s", asURL.RepositoryID, asURL.ParentType, asURL.ParentID))
	var respBytes []byte
	if asURL.ParentType == "resources" {
		svc.logInfo(js, fmt.Sprintf("Looking up parent resource %s in repo %s...", asURL.ParentID, asURL.RepositoryID))
		url := fmt.Sprintf("/repositories/%s/resources/%s", asURL.RepositoryID, asURL.ParentID)
		resp, err := svc.sendASGetRequest(url)
		if err != nil {
			return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
		}
		respBytes = resp
	} else if asURL.ParentType == "archival_objects" {
		svc.logInfo(js, fmt.Sprintf("Looking up parent archival object %s in repo %s...", asURL.ParentID, asURL.RepositoryID))
		url := fmt.Sprintf("/repositories/%s/archival_objects/%s", asURL.RepositoryID, asURL.ParentID)
		resp, err := svc.sendASGetRequest(url)
		if err != nil {
			return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
		}
		respBytes = resp
	} else {
		return nil, fmt.Errorf("Unsupported parent type: %s", asURL.ParentType)
	}
	log.Printf("%s", respBytes)

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

func (svc *ServiceContext) lookupASObjectSlug(repoID, tgtObjType, slug string) (string, error) {
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

	// sample response: [{URI:/repositories/3/resources/886} {URI:/repositories/3/classification_terms/4}]}
	out := ""
	for _, r := range parsed.Results {
		uriParts := strings.Split(r.URI, "/")
		objType := uriParts[len(uriParts)-2]
		if objType == tgtObjType {
			out = uriParts[len(uriParts)-1]
			break
		}
	}
	if out == "" {
		return "", fmt.Errorf("no match found for %s", slug)
	}
	return out, nil
}

func parsePublicASURL(asURL string) *asURLInfo {
	//public AS urls look like this:
	//   https://archives.lib.virginia.edu/repositories/3/archival_objects/62839
	//OR Relative:
	//   /repositories/3/archival_objects/62839
	//only care about the repoID, object type and objID
	bits := strings.Split(asURL, "/")
	if len(bits) >= 4 {
		out := asURLInfo{
			RepositoryID: bits[len(bits)-3],
			ParentType:   bits[len(bits)-2],
			ParentID:     bits[len(bits)-1],
		}
		return &out
	}
	return nil
}
