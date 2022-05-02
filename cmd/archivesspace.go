package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
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
		var es externalSystem
		err := svc.GDB.Where("name=?", "ArchivesSpace").Find(&es).Error
		if err != nil {
			log.Printf("ERROR: unable to get archivesSpace system info: %s", err.Error())
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		authURL := fmt.Sprintf("%s/users/%s/login", es.APIURL, svc.ArchivesSpace.User)
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
		err = json.Unmarshal(resp, &jsonResp)
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
	c.String(http.StatusNotImplemented, "not yet")
}
