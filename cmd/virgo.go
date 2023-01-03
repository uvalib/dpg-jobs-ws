package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) publishToVirgo(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("PublishToVirgo", "Metadata", mdID)
	if err != nil {
		log.Printf("ERROR: unable to create PublishToVirgo job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Publish metadata %d to Virgo", mdID))
	var md metadata
	err = svc.GDB.First(&md, mdID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to find metadata %d", mdID))
		c.String(http.StatusNotFound, err.Error())
		return
	}

	if md.Type != "XmlMetadata" && md.Type != "SirsiMetadata" {
		svc.logFatal(js, fmt.Sprintf("This metadata is [%s] and not a candidate for publication", md.Type))
		c.String(http.StatusBadRequest, "this item is not eligable for publication")
		return
	}

	var dlUnits []*unit
	err = svc.GDB.Preload("Metadata").Where("metadata_id=?", md.ID).Where("include_in_dl=?", true).Find(&dlUnits).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to find unit suitable for publicataion: %s", err.Error()))
		c.String(http.StatusBadRequest, "this item is not eligable for publication")
		return
	}

	if len(dlUnits) == 0 {
		svc.logInfo(js, "No DL units directly found. Trying masterfiles...")
		// No units but one master file is an indicator that descriptive XML
		// metadata was created specifically for the master file after initial ingest.
		// This is usually the case with image collections where each image has its own descriptive metadata.
		// In this case, there is no direct link from metadata to unit. Must find it by
		// going through the master file that this metadata describes
		var mfCnt int64
		err := svc.GDB.Table("master_files").Where("metadata_id=?", md.ID).Count(&mfCnt).Error
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Unable to get master file count for metadata: %s", err.Error()))
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		if mfCnt == 1 {
			svc.logInfo(js, "One masterfile found; look for a DL unit")
			var mf masterFile
			err = svc.GDB.Joins("inner join units u on u.id=unit_id").Preload("Unit").Preload("Unit.Metadata").
				Where("u.include_in_dl=?", true).Where("master_files.metadata_id=?", mdID).First(&mf).Error
			if err != nil {
				svc.logFatal(js, fmt.Sprintf("Unable to get master file unit for metadata: %s", err.Error()))
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			dlUnits = append(dlUnits, &mf.Unit)
		}
	}

	tgtUnit := dlUnits[0]
	err = svc.publishUnitToVirgo(js, tgtUnit)
	if err != nil {
		svc.logFatal(js, err.Error())
		c.String(http.StatusInternalServerError, err.Error())
	}
	c.String(http.StatusOK, "published")
}

func (svc *ServiceContext) publishUnitToVirgo(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "Publish unit to Virgo")
	if tgtUnit.Metadata.AvailabilityPolicyID == nil {
		return fmt.Errorf("Metadata %d for Unit is missing the required availability policy", *tgtUnit.MetadataID)
	}

	iiifURL := fmt.Sprintf("%s/pid/%s?refresh=true", svc.IIIF.URL, tgtUnit.Metadata.PID)
	svc.logInfo(js, fmt.Sprintf("Generating IIIF manifest with %s", iiifURL))
	_, errResp := svc.getRequest(iiifURL)
	if errResp != nil {
		return fmt.Errorf("Unable to generate IIIF manifest: %d: %s", errResp.StatusCode, errResp.Message)
	}
	svc.logInfo(js, "IIIF manifest successfully generated")

	// Flag metadata for ingest or update
	now := time.Now()
	if tgtUnit.Metadata.DateDlIngest == nil {
		svc.logInfo(js, fmt.Sprintf("Set DateDlIngest for unit metadata record %d", tgtUnit.MetadataID))
		tgtUnit.Metadata.DateDlIngest = &now
		err := svc.GDB.Model(tgtUnit.Metadata).Select("DateDlIngest").Updates(*tgtUnit.Metadata).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update DateDlIngest: %s", err.Error()))
		} else {
			svc.logInfo(js, fmt.Sprintf("Successfully updated DateDlIngest for metadata %d", tgtUnit.MetadataID))
		}
	} else {
		svc.logInfo(js, fmt.Sprintf("Set DateDlUpdate for unit metadata record %d", tgtUnit.MetadataID))
		tgtUnit.Metadata.DateDlUpdate = &now
		err := svc.GDB.Model(tgtUnit.Metadata).Select("DateDlUpdate").Updates(*tgtUnit.Metadata).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update DateDlUpdate: %s", err.Error()))
		} else {
			svc.logInfo(js, fmt.Sprintf("Successfully updated DateDlUpdate for metadata %d", tgtUnit.MetadataID))
		}
	}

	// now flag each master file for intest or update...
	svc.logInfo(js, "Getting up-to-date list of master files for publication")
	var masterfiles []masterFile
	err := svc.GDB.Where("unit_id=?", tgtUnit.ID).Find(&masterfiles).Error
	if err != nil {
		return fmt.Errorf("Unable to get masterfiles: %s", err.Error())
	}
	for _, mf := range masterfiles {
		if mf.DateDlIngest == nil {
			mf.DateDlIngest = &now
			svc.GDB.Model(&mf).Select("DateDlIngest").Updates(mf)
		} else {
			mf.DateDlUpdate = &now
			svc.GDB.Model(&mf).Select("DateDlUpdate").Updates(mf)
		}

		// if master file has its own metadata, set it too
		if *mf.MetadataID != *tgtUnit.MetadataID {
			var mfMD metadata
			err = svc.GDB.First(&mfMD, *mf.MetadataID).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to find masterfile %s metadata record: %s", mf.PID, err.Error()))
				continue
			}
			if mfMD.DateDlIngest == nil {
				mfMD.DateDlIngest = &now
				svc.GDB.Model(&mfMD).Select("DateDlIngest").Updates(mfMD)
			} else {
				mfMD.DateDlUpdate = &now
				svc.GDB.Model(&mfMD).Select("DateDlUpdate").Updates(mfMD)
			}
		}
	}

	//  Call the reindex API for sirsi items
	if tgtUnit.Metadata.Type == "SirsiMetadata" && tgtUnit.Metadata.CatalogKey != "" {
		svc.logInfo(js, fmt.Sprintf("Call the reindex service for %d - %s", *tgtUnit.MetadataID, tgtUnit.Metadata.CatalogKey))
		url := fmt.Sprintf("%s/api/reindex/%s", svc.ReindexURL, tgtUnit.Metadata.CatalogKey)
		_, resp := svc.putRequest(url)
		if resp != nil {
			svc.logError(js, fmt.Sprintf("%s reindex request failed: %d: %s", tgtUnit.Metadata.CatalogKey, resp.StatusCode, resp.Message))
		} else {
			svc.logInfo(js, fmt.Sprintf("%s reindex request successful", tgtUnit.Metadata.CatalogKey))
		}
	}

	// Lastly, flag the deliverables ready date if it is not already set
	if tgtUnit.DateDLDeliverablesReady == nil {
		tgtUnit.DateDLDeliverablesReady = &now
		svc.GDB.Model(tgtUnit).Select("DateDLDeliverablesReady").Updates(*tgtUnit)
		svc.logInfo(js, "Unit is ready for ingestion into the DL.")
	}
	svc.logInfo(js, fmt.Sprintf("Unit and %d master files have been flagged for an update in the DL", len(masterfiles)))

	return nil
}
