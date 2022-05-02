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
	ID     int64
	Name   string
	APIURL string `gorm:"column:api_url"`
}

func (svc *ServiceContext) archivesSpaceMiddleware(c *gin.Context) {
	log.Printf("INFO: ensure archivesspace auth token exists for %s", c.Request.URL)
	now := time.Now()
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
		exp := time.Now()
		exp.Add(30 * time.Minute)
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

type asRepository struct {
	Name string
	Slug string
	URI  string
}

func (r *asRepository) ID() string {
	parts := strings.Split(r.URI, "/")
	return parts[len(parts)-1]
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
