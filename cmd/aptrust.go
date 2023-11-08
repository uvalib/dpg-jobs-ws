package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strconv"

	"github.com/gin-gonic/gin"
)

type apTrustResult struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	ETag             string `json:"etag"`
	ObjectIdentifier string `json:"object_identifier"`
	AltIdentifier    string `json:"alt_identifier"`
	StorageOption    string `json:"storage"`
	Note             string `json:"note"`
	Status           string `json:"status"`
	QueuedAt         string `json:"queued_at"`
	ProcessedAt      string `json:"date_processed"`
}

type apTrustResponse struct {
	Count   int64           `json:"count"`
	Results []apTrustResult `json:"results,omitempty"`
}

func (svc *ServiceContext) getAPTrustStatus(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	if mdID == 0 {
		log.Printf("INFO: imvalid id %s passed to aptrust request", c.Param("id"))
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

	objType := "sirsimetadata"
	if md.Type == "XmlMetadata" {
		objType = "xmlmetadata"
	}
	aptName := fmt.Sprintf("virginia.edu.tracksys-%s-%d.tar", objType, mdID)
	cmd := exec.Command("apt-cmd", "registry", "list", "workitems", fmt.Sprintf("name=%s", aptName))
	log.Printf("INFO: aptrust command: %+v", cmd)
	aptOut, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("ERROR: aptrust request failed: %s", aptOut)
		c.String(http.StatusInternalServerError, string(aptOut))
		return
	}
	log.Printf("INFO: raw aptrust response: %s", aptOut)

	var jsonResp apTrustResponse
	err = json.Unmarshal(aptOut, &jsonResp)
	if err != nil {
		log.Printf("ERROR: unable to parse response: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	if jsonResp.Count == 0 {
		log.Printf("INFO: metadata %d has no aptrust status", md.ID)
		c.String(http.StatusNotFound, fmt.Sprintf("%d has no aptrust status", md.ID))
	} else {
		c.JSON(http.StatusOK, jsonResp.Results[0])
	}
}
