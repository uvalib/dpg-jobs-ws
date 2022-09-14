package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/publicsuffix"
)

type jstorResult struct {
	ID                   string   `json:"id"`
	ArtstorID            string   `json:"artstorid"`
	Date                 string   `json:"date"`
	CollectionTypeNameID []string `json:"collectiontypenameid"`
}

type jstorResponse struct {
	Total   int           `json:"total"`
	Results []jstorResult `json:"results"`
}

type jstorMetadataField struct {
	Count      int    `json:"count"`
	FieldName  string `json:"fieldName"`
	FieldValue string `json:"fieldValue"`
}

type jstorRawMetadata struct {
	SSID     string               `json:"SSID"`
	Width    int                  `json:"width"`
	Height   int                  `json:"height"`
	Metadata []jstorMetadataField `json:"metadata_json"`
}

type jstorMetadataResponse struct {
	Total    int                `json:"total"`
	Metadata []jstorRawMetadata `json:"metadata"`
}

type jstorMetadata struct {
	ID           string `json:"id"`
	SSID         string `json:"ssid"`
	CollectionID string `json:"collectionID"`
	Collection   string `json:"collection"`
	Title        string `json:"title"`
	Description  string `json:"desc"`
	Creator      string `json:"creator"`
	Date         string `json:"date"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
}

func (svc *ServiceContext) lookupJstorMetadata(c *gin.Context) {
	tgtFilename := c.Query("filename")
	log.Printf("INFO: lookup details for jstor filename %s", tgtFilename)
	var extSys struct {
		APIURL string `gorm:"column:api_url" json:"apiURL"`
	}
	err := svc.GDB.Table("external_systems").Where("name=?", "JSTOR Forum").Select("api_url").First(&extSys).Error
	if err != nil {
		log.Printf("ERROR: unable to get jstor api url: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if svc.JSTORCookies == nil {
		err := svc.startJstorSession(extSys.APIURL)
		if err != nil {
			log.Printf("ERROR: unable to start jstor session: %s", err.Error())
			c.String(http.StatusInternalServerError, fmt.Sprintf("unable to start jstor session: %s", err.Error()))
			return
		}
	}
	out, err := svc.lookupJstorFilename(extSys.APIURL, tgtFilename, true)
	if err != nil {
		log.Printf("ERROR: unable to lookup details for %s: %s", tgtFilename, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, out)
}

func (svc *ServiceContext) lookupJstorFilename(apiURL, filename string, retry bool) (*jstorMetadata, error) {
	log.Printf("INFO: looking up [%s] in jstor", filename)
	jsonStr := fmt.Sprintf(`{"limit":1,"start":0,"content_types":["art"],"query":"%s"}`, filename)
	URL := fmt.Sprintf("%s/search/v1.0/search", apiURL)
	timeout := time.Duration(10 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	apiReq, _ := http.NewRequest("POST", URL, bytes.NewBuffer([]byte(jsonStr)))
	apiReq.Header.Set("Content-Type", "application/json")
	apiReq.Header.Set("authority", "library.artstor.org")
	for _, cookie := range svc.JSTORCookies {
		apiReq.AddCookie(cookie)
	}

	rawResp, rawErr := client.Do(apiReq)
	resp, respErr := handleAPIResponse(URL, rawResp, rawErr)
	if respErr != nil {
		if (respErr.StatusCode == 401 || respErr.StatusCode == 403) && retry {
			log.Printf("INFO: jstor request unauthorized; restart session...")
			err := svc.startJstorSession(apiURL)
			if err != nil {
				return nil, fmt.Errorf("unable to start jstor session: %s", err.Error())
			}
			log.Printf("INFO: retry lookup %s", filename)
			return svc.lookupJstorFilename(apiURL, filename, false)
		}
		return nil, fmt.Errorf("%d: %s", respErr.StatusCode, respErr.Message)
	}

	var parsed jstorResponse
	err := json.Unmarshal(resp, &parsed)
	if err != nil {
		return nil, fmt.Errorf("unable to parse jstor response: %s", err.Error())
	}
	if len(parsed.Results) > 1 {
		return nil, fmt.Errorf("%d matches found", len(parsed.Results))
	}
	if len(parsed.Results) == 0 {
		return nil, fmt.Errorf("no matches found")
	}

	tgtHit := parsed.Results[0]
	out := jstorMetadata{ID: tgtHit.ArtstorID, Date: tgtHit.Date}
	out.Collection = strings.Split(tgtHit.CollectionTypeNameID[0], "|")[1]
	out.CollectionID = strings.Split(tgtHit.CollectionTypeNameID[0], "|")[2]

	log.Printf("INFO: find metadata for %s:%s", filename, out.ID)
	URL = fmt.Sprintf("%s/v1/metadata?object_ids=%s&legacy=false", apiURL, out.ID)
	getReq, _ := http.NewRequest("GET", URL, nil)
	getReq.Header.Set("Content-Type", "application/json")
	getReq.Header.Set("authority", "library.artstor.org")
	for _, cookie := range svc.JSTORCookies {
		getReq.AddCookie(cookie)
	}

	rawResp, rawErr = client.Do(getReq)
	mdResp, respErr := handleAPIResponse(URL, rawResp, rawErr)
	if respErr != nil {
		return nil, fmt.Errorf("%d: %s", respErr.StatusCode, respErr.Message)
	}

	var parsedMD jstorMetadataResponse
	err = json.Unmarshal(mdResp, &parsedMD)
	if err != nil {
		return nil, fmt.Errorf("unable to parse jstor metadata response: %s", err.Error())
	}
	if parsedMD.Total > 1 {
		return nil, fmt.Errorf("%d metadata matches found for %s", len(parsed.Results), out.ID)
	}
	if parsedMD.Total == 0 {
		return nil, fmt.Errorf("no metadata matches found for %s", out.ID)
	}
	tgtMD := parsedMD.Metadata[0]
	out.SSID = tgtMD.SSID
	out.Width = tgtMD.Width
	out.Height = tgtMD.Height
	for _, mdField := range tgtMD.Metadata {
		if mdField.FieldName == "Creator" {
			out.Creator = mdField.FieldValue
		}
		if mdField.FieldName == "Description" {
			out.Description = mdField.FieldValue
		}
		if mdField.FieldName == "Title" {
			if out.Title != "" {
				out.Title += " "
			}
			out.Title += mdField.FieldValue
		}
	}

	return &out, nil
}

func (svc *ServiceContext) startJstorSession(apiURL string) error {
	log.Printf("INFO: start jstor session")
	cookieJar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	timeout := time.Duration(10 * time.Second)
	client := http.Client{
		Timeout: timeout,
		Jar:     cookieJar,
	}
	reqURL := fmt.Sprintf("%s/secure/userinfo", apiURL)
	loginResp, err := client.Get(reqURL)
	if err != nil {
		return err
	}

	svc.JSTORCookies = loginResp.Cookies()
	log.Printf("INFO: jstor session started")
	return nil
}
