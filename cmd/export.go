package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type exporData struct {
	ID               int64            `json:"id"`
	PID              string           `json:"pid"`
	CollectionID     string           `json:"collectionID,omitempty"`
	Type             string           `json:"type"`
	Title            string           `json:"title"`
	CreatorName      string           `json:"creatorName,omitempty"`
	CatalogKey       string           `json:"catalogKey,omitempty"`
	CallNumber       string           `json:"callNumber,omitempty"`
	Barcode          string           `json:"barcode,omitempty"`
	DescMetadata     string           `json:"descMetadata,omitempty"`
	Locations        []exportLocation `json:"locations,omitempty"`
	PreservationTier string           `json:"preservationTier,omitempty"`
	ExternalSystem   string           `json:"externalSystem,omitempty"`
	ExternalURI      string           `json:"externalURI,omitempty"`
	CreatedAt        time.Time        `json:"createdAt"`
	UpdatedAt        time.Time        `json:"updatedAt,omitempty"`
	Children         []*exporData     `json:"children,omitempty"`
}

type exportLocation struct {
	ContainerType string `json:"containerType"`
	ContainerID   string `json:"containerID"`
	FolderID      string `json:"folder"`
	Notes         string `json:"notes,omitempty"`
}

func (svc *ServiceContext) exportCollection(c *gin.Context) {
	collectionMetadataID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	log.Printf("INFO: export collection %d request", collectionMetadataID)

	js, err := svc.createJobStatus("CollectionExport", "Metadata", collectionMetadataID)
	if err != nil {
		log.Printf("ERROR: unable to create CollectionExport job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var collRec metadata
	err = svc.GDB.Preload("Locations").Preload("PreservationTier").Preload("ExternalSystem").First(&collRec, collectionMetadataID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Get collection %d failed: %s", collectionMetadataID, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	collectionID := collRec.CollectionID
	if collectionID == "" {
		collectionID = collRec.PID
	}
	collectionID = strings.ReplaceAll(collectionID, ":", "_")

	exportBaseDir := path.Join(svc.ProcessingDir, "exports", collectionID)
	svc.logInfo(js, fmt.Sprintf("Create base export directory %s", exportBaseDir))
	if pathExists(exportBaseDir) {
		err := os.RemoveAll(exportBaseDir)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to cleanup export dirctory %s: %s", exportBaseDir, err.Error()))
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
	}
	err = ensureDirExists(exportBaseDir, 0777)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to create export dirctory %s: %s", exportBaseDir, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered during collection %d export: %v", collectionMetadataID, r))
			}
		}()

		svc.logInfo(js, fmt.Sprintf("Export collection %s to %s", collectionID, exportBaseDir))
		out := metadataToJSON(&collRec)

		svc.logInfo(js, fmt.Sprintf("Load child records for %d to begin export process", collectionMetadataID))
		var collRecs []metadata
		err = svc.GDB.Preload("Locations").Preload("PreservationTier").Preload("ExternalSystem").Where("parent_metadata_id=?", collectionMetadataID).Find(&collRecs).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to load child metadata records for collection %d: %s", collectionMetadataID, err.Error()))
			return
		}

		svc.logInfo(js, fmt.Sprintf("Collection collection %s has %d children", collectionID, len(collRecs)))
		for _, md := range collRecs {
			svc.logInfo(js, fmt.Sprintf("Process child %s", md.PID))
			jsonRec := metadataToJSON(&md)
			out.Children = append(out.Children, jsonRec)

			err = svc.exportMasterFiles(js, &md, exportBaseDir)
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Download images for %s failed: %s", md.PID, err.Error()))
				return
			}
		}

		svc.logInfo(js, "Convert metadata to json")
		metadataBytes, err := json.MarshalIndent(out, "", "   ")
		if err != nil {
			svc.logFatal(js, err.Error())
			return
		}

		metadataFileName := path.Join(exportBaseDir, "metadata.json")
		svc.logInfo(js, fmt.Sprintf("Write json netadata to %s", metadataFileName))
		err = os.WriteFile(metadataFileName, metadataBytes, 0644)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to write metadata file %s: %s", metadataFileName, err.Error()))
			return
		}

		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) exportMasterFiles(js *jobStatus, md *metadata, exportBaseDir string) error {
	svc.logInfo(js, fmt.Sprintf("Download masterfiles for metadata %s", md.PID))
	mdSubdir := strings.ReplaceAll(md.PID, ":", "_")
	assembleImagesDir := path.Join(exportBaseDir, mdSubdir)
	err := ensureDirExists(assembleImagesDir, 0777)
	if err != nil {
		return fmt.Errorf("Unable to create image assemble dir %s: %s", assembleImagesDir, err.Error())
	}

	var masterFiles []masterFile
	err = svc.GDB.Joins("Unit").Where("Unit.metadata_id=? and Unit.intended_use_id=?", md.ID, 110).Find(&masterFiles).Error
	if err != nil {
		return err
	}
	if len(masterFiles) == 0 {
		// if that fails, see if this is a the special case where an image is assigned different metadata than the unit.
		// this is the case for individual images described by XML metadata that are generaly part of a larger collection
		svc.logInfo(js, fmt.Sprintf("no units directly found for metadata %d; searching master files...", md.ID))
		err = svc.GDB.Joins("Unit").Where("Unit.intended_use_id=?", 110).Where("master_files.metadata_id=?", md.ID).Find(&masterFiles).Error
		if err != nil {
			return err
		}
		if len(masterFiles) == 0 {
			return fmt.Errorf("no masterfiles qualify for export (intended use 110)")
		}
	}

	svc.logInfo(js, fmt.Sprintf("%d masterfiles found", len(masterFiles)))
	for _, mf := range masterFiles {
		svc.logInfo(js, fmt.Sprintf("Export masterfile %s to %s", mf.Filename, assembleImagesDir))
		archiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", mf.UnitID), mf.Filename)
		destFile := path.Join(assembleImagesDir, mf.Filename)
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
	}

	svc.logInfo(js, fmt.Sprintf("Generate metadata %s export tar file", md.PID))
	destTar := path.Join(exportBaseDir, fmt.Sprintf("%s.tar", mdSubdir))
	if pathExists(destTar) {
		svc.logInfo(js, fmt.Sprintf("Clean up pre-existing tar file %s", destTar))
		err := os.Remove(destTar)
		if err != nil {
			return fmt.Errorf("unable to cleanup prior tar file %s: %s", destTar, err.Error())
		}
	}
	cmdArray := []string{"cf", destTar, "-C", exportBaseDir, mdSubdir}
	cmd := exec.Command("tar", cmdArray...)
	svc.logInfo(js, fmt.Sprintf("%+v", cmd))
	_, err = cmd.Output()
	if err != nil {
		return fmt.Errorf("unable to create tag for metadata %s masterfiles: %s", md.PID, err.Error())
	}

	svc.logInfo(js, fmt.Sprintf("Clean up assembly directory %s", assembleImagesDir))
	err = os.RemoveAll(assembleImagesDir)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to clean up %s: %s", assembleImagesDir, err.Error()))
	}
	return nil
}

func metadataToJSON(md *metadata) *exporData {
	out := exporData{
		ID:           md.ID,
		PID:          md.PID,
		CollectionID: md.CollectionID,
		Type:         md.Type,
		Title:        md.Title,
		CreatorName:  md.CreatorName,
		CatalogKey:   md.CatalogKey,
		CallNumber:   md.CallNumber,
		Barcode:      md.Barcode,
		DescMetadata: md.DescMetadata,
		Locations:    make([]exportLocation, 0),
		CreatedAt:    md.CreatedAt,
		UpdatedAt:    md.UpdatedAt,
	}
	if md.ExternalSystem != nil {
		out.ExternalSystem = md.ExternalSystem.Name
		out.ExternalURI = fmt.Sprintf("%s%s", md.ExternalSystem.PublicURL, md.ExternalURI)
	}
	if md.PreservationTier != nil {
		out.PreservationTier = fmt.Sprintf("%s: %s", md.PreservationTier.Name, md.PreservationTier.Description)
	}
	if len(md.Locations) > 0 {
		for _, loc := range md.Locations {
			expLoc := exportLocation{
				ContainerType: loc.ContainerType.Name,
				ContainerID:   loc.ContainerID,
				FolderID:      loc.FolderID,
				Notes:         loc.Notes,
			}
			out.Locations = append(out.Locations, expLoc)
		}
	}
	return &out
}
