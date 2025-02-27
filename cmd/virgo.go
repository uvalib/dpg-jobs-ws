package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// publishToVirgo is the handler for calls to the publish API endpoint
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

	err = nil
	if md.Type == "XmlMetadata" {
		err = svc.publishXMLToVirgo(js, &md, nil)
	} else {
		err = svc.publishSirsiToVirgo(js, &md, nil)
	}

	if err != nil {
		svc.logFatal(js, "Publication failed")
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "published")
}

func (svc *ServiceContext) publishXMLToVirgo(js *jobStatus, xmlMetadata *metadata, srcUnit *unit) error {
	svc.logInfo(js, fmt.Sprintf("Call the XML reindex sevice for %s", xmlMetadata.PID))
	tgtUnit := srcUnit
	var tgtMasterFiles []*masterFile

	if tgtUnit != nil {
		svc.logInfo(js, fmt.Sprintf("Get master files for target XML unit %d", tgtUnit.ID))
		err := svc.GDB.Where("metadata_id=? and unit_id=?", xmlMetadata.ID, tgtUnit.ID).Find(&tgtMasterFiles).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to get masterfiles for unit %d, metadata %d: %s", tgtUnit.ID, xmlMetadata.ID, err.Error()))
			return fmt.Errorf("unable to get masterfiles for unit %d, metadata %d: %s", tgtUnit.ID, xmlMetadata.ID, err.Error())
		}
	} else {
		svc.logInfo(js, "Look for unit flagged for inclusion in DL")
		var dlUnits []*unit
		err := svc.GDB.Where("metadata_id=?", xmlMetadata.ID).Where("include_in_dl=?", true).Find(&dlUnits).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Error searching for a unit to publish: %s", err.Error()))
			return err
		}
		if len(dlUnits) > 1 {
			svc.logError(js, "Too many units flagged for inclusion in Virgo")
			return fmt.Errorf("too many units flagged for publication")
		}

		if len(dlUnits) == 0 {
			// if there are no units this means that descriptive XML
			// metadata was created specifically for one or more master files from a unit after initial ingest.
			// In this case, there is no direct link from metadata to unit. Find it by
			// going through the master file that this metadata describes. There can only be one such unit.
			svc.logInfo(js, fmt.Sprintf("No units found for metadata %d, looking for master files", xmlMetadata.ID))
			var candidateMasterFiles []*masterFile
			err := svc.GDB.Preload("Unit").Where("metadata_id=?", xmlMetadata.ID).Find(&candidateMasterFiles).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to get masterfiles for metadata %d: %s", xmlMetadata.ID, err.Error()))
				return fmt.Errorf("unable to get masterfiles for metadata %d: %s", xmlMetadata.ID, err.Error())
			}

			// iterate masterfies and make sure they are from a single unit and that unit is flagged for inclusion in dl
			svc.logInfo(js, "Look for units suitable for publication from the list of masterfiles")
			for _, mf := range candidateMasterFiles {
				if mf.Unit.IncludeInDL {
					if tgtUnit == nil {
						tgtUnit = &mf.Unit
					} else if tgtUnit.ID != mf.Unit.ID {
						svc.logError(js, "Too many units flagged for inclusion in DL found")
						return fmt.Errorf("too many units flagged for publication")
					}
					tgtMasterFiles = append(tgtMasterFiles, mf)
				}
			}
			if tgtUnit == nil {
				svc.logError(js, "No units suitable for publication found.")
				return fmt.Errorf("no units suitable for publication")
			}
		} else {
			// a single unit matches the XML metadata record. Get all masterfiles from that unit that also match the metadata
			tgtUnit = dlUnits[0]
			err := svc.GDB.Where("metadata_id=? and unit_id=?", xmlMetadata.ID, tgtUnit.ID).Find(&tgtMasterFiles).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to get masterfiles for unit %d, metadata %d: %s", tgtUnit.ID, xmlMetadata.ID, err.Error()))
				return fmt.Errorf("unable to get masterfiles for unit %d, metadata %d: %s", tgtUnit.ID, xmlMetadata.ID, err.Error())
			}
		}
	}
	svc.logInfo(js, fmt.Sprintf("%d masterfiles from unit %d will be published to DL", len(tgtMasterFiles), tgtUnit.ID))

	// be sure there is a current IIIF manifest by forcing a regenerate
	iiifURL := fmt.Sprintf("%s/pid/%s?refresh=true", svc.IIIF.ManifestURL, xmlMetadata.PID)
	svc.logInfo(js, fmt.Sprintf("Generating IIIF manifest with %s", iiifURL))
	_, errResp := svc.getRequest(iiifURL)
	if errResp != nil {
		svc.logError(js, fmt.Sprintf("Unable generate IIIF manifest: %d:%s", errResp.StatusCode, errResp.Message))
		return fmt.Errorf("Unable to generate IIIF manifest: %d: %s", errResp.StatusCode, errResp.Message)
	}
	svc.logInfo(js, "IIIF manifest successfully generated")

	// Flag metadata DL ingest / update time
	now := time.Now()
	if xmlMetadata.DateDlIngest == nil {
		svc.logInfo(js, fmt.Sprintf("Set DateDlIngest for metadata record %d", xmlMetadata.ID))
		xmlMetadata.DateDlIngest = &now
		err := svc.GDB.Model(xmlMetadata).Select("DateDlIngest").Updates(xmlMetadata).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update DateDlIngest: %s", err.Error()))
		} else {
			svc.logInfo(js, fmt.Sprintf("Successfully updated DateDlIngest for metadata %d", xmlMetadata.ID))
		}
	} else {
		svc.logInfo(js, fmt.Sprintf("Set DateDlUpdate for unit metadata record %d", xmlMetadata.ID))
		xmlMetadata.DateDlUpdate = &now
		err := svc.GDB.Model(xmlMetadata).Select("DateDlUpdate").Updates(xmlMetadata).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update DateDlUpdate: %s", err.Error()))
		} else {
			svc.logInfo(js, fmt.Sprintf("Successfully updated DateDlUpdate for metadata %d", xmlMetadata.ID))
		}
	}

	// now flag each master file
	for _, mf := range tgtMasterFiles {
		if mf.DateDlIngest == nil {
			mf.DateDlIngest = &now
			svc.GDB.Model(mf).Select("DateDlIngest").Updates(mf)
		} else {
			mf.DateDlUpdate = &now
			svc.GDB.Model(mf).Select("DateDlUpdate").Updates(mf)
		}
	}

	// reindex the metadata
	url := fmt.Sprintf("%s/%s", svc.XMLReindexURL, xmlMetadata.PID)
	_, resp := svc.putRequest(url)
	if resp != nil {
		svc.logError(js, fmt.Sprintf("XML %s reindex request failed: %d: %s", xmlMetadata.PID, resp.StatusCode, resp.Message))
		return fmt.Errorf("reindex request failed %d:%s", resp.StatusCode, resp.Message)
	}

	// Lastly, flag the deliverables ready date if it is not already set
	if tgtUnit.DateDLDeliverablesReady == nil {
		svc.logInfo(js, "Set date unit deliverables ready")
		tgtUnit.DateDLDeliverablesReady = &now
		err := svc.GDB.Model(tgtUnit).Select("DateDLDeliverablesReady").Updates(*tgtUnit).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to set DateDLDeliverablesReady: %s", err.Error()))
		}
	}

	svc.logInfo(js, fmt.Sprintf("XML %s reindex request successful", xmlMetadata.PID))
	return nil
}

func (svc *ServiceContext) publishSirsiToVirgo(js *jobStatus, sirsiMetadata *metadata, srcUnit *unit) error {
	svc.logInfo(js, "Publish Sirsi metadata to Virgo")

	svc.logInfo(js, "Validate metadata settings")
	if sirsiMetadata.CatalogKey == "" {
		svc.logError(js, fmt.Sprintf("Publish to Virgo failed: metadata %d is missing a catalog key.", sirsiMetadata.ID))
		return fmt.Errorf("metadata %d is missing a catalog key", sirsiMetadata.ID)
	}
	if sirsiMetadata.AvailabilityPolicyID == 0 {
		svc.logError(js, fmt.Sprintf("Publish to Virgo failed: metadata %d is missing the required availability policy.", sirsiMetadata.ID))
		return fmt.Errorf("metadata %d is missing availability policy", sirsiMetadata.ID)
	}

	tgtUnit := srcUnit
	if tgtUnit == nil {
		svc.logInfo(js, "Find the single unit flagged for inclusion in Virgo")
		var dlUnits []*unit
		err := svc.GDB.Where("metadata_id=?", sirsiMetadata.ID).Where("include_in_dl=?", true).Find(&dlUnits).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to find unit suitable for publicataion: %s", err.Error()))
			return err
		}
		if len(dlUnits) > 1 {
			svc.logError(js, "Too many units flagged for inclusion in Virgo")
			return fmt.Errorf("too many units flagged for publication")
		}

		if len(dlUnits) == 0 {
			// if there are no units, it may mean a single unit has master files from more than one sirsi record.
			// find the single unit that is flagged for DL that has master files tied to the target metadata record
			svc.logInfo(js, fmt.Sprintf("No units found for metadata %d, looking for another unit with master files using that metadata", sirsiMetadata.ID))
			var tgtUnits []*unit
			q := "select u.* from units u inner join master_files m on m.unit_id = u.id "
			q += "where m.metadata_id = ? and u.include_in_dl = ? GROUP by(u.id)"
			uErr := svc.GDB.Raw(q, 109034, 1).Scan(&tgtUnits).Error
			if uErr != nil {
				svc.logError(js, fmt.Sprintf("Unable to get other units for metadata %d: %s", sirsiMetadata.ID, uErr.Error()))
				return fmt.Errorf("unable to get publishable unit for metadata %d: %s", sirsiMetadata.ID, uErr.Error())
			}

			if len(tgtUnits) == 0 {
				svc.logError(js, "No units suitable for publication found.")
				return fmt.Errorf("no units suitable for publication")
			}
			tgtUnit = tgtUnits[0]
		} else {
			tgtUnit = dlUnits[0]
		}
	}
	svc.logInfo(js, fmt.Sprintf("Unit %d will be published to DL", tgtUnit.ID))

	// be sure there is a current IIIF manifest by forcing a regenerate
	iiifURL := fmt.Sprintf("%s/pid/%s?refresh=true", svc.IIIF.ManifestURL, sirsiMetadata.PID)
	svc.logInfo(js, fmt.Sprintf("Generating IIIF manifest with %s", iiifURL))
	_, errResp := svc.getRequest(iiifURL)
	if errResp != nil {
		svc.logError(js, fmt.Sprintf("Unable generate IIIF manifest: %d:%s", errResp.StatusCode, errResp.Message))
		return fmt.Errorf("Unable to generate IIIF manifest: %d: %s", errResp.StatusCode, errResp.Message)
	}
	svc.logInfo(js, "IIIF manifest successfully generated")

	// Flag metadata DL ingest / update time
	now := time.Now()
	if sirsiMetadata.DateDlIngest == nil {
		svc.logInfo(js, fmt.Sprintf("Set DateDlIngest for unit metadata record %d", sirsiMetadata.ID))
		sirsiMetadata.DateDlIngest = &now
		err := svc.GDB.Model(sirsiMetadata).Select("DateDlIngest").Updates(*sirsiMetadata).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update DateDlIngest: %s", err.Error()))
		} else {
			svc.logInfo(js, fmt.Sprintf("Successfully updated DateDlIngest for metadata %d", sirsiMetadata.ID))
		}
	} else {
		svc.logInfo(js, fmt.Sprintf("Set DateDlUpdate for unit metadata record %d", sirsiMetadata.ID))
		sirsiMetadata.DateDlUpdate = &now
		err := svc.GDB.Model(sirsiMetadata).Select("DateDlUpdate").Updates(*sirsiMetadata).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update DateDlUpdate: %s", err.Error()))
		} else {
			svc.logInfo(js, fmt.Sprintf("Successfully updated DateDlUpdate for metadata %d", sirsiMetadata.ID))
		}
	}

	// now flag each master file
	svc.logInfo(js, "Getting up-to-date list of master files for publication")
	var masterfiles []masterFile
	err := svc.GDB.Where("unit_id=? and metadata_id=?", tgtUnit.ID, sirsiMetadata.ID).Find(&masterfiles).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to get list of master files to mark as published: %s", err.Error()))
	} else {
		for _, mf := range masterfiles {
			if mf.DateDlIngest == nil {
				mf.DateDlIngest = &now
				svc.GDB.Model(&mf).Select("DateDlIngest").Updates(mf)
			} else {
				mf.DateDlUpdate = &now
				svc.GDB.Model(&mf).Select("DateDlUpdate").Updates(mf)
			}
		}
	}

	//  Call the reindex API for sirsi items
	svc.logInfo(js, fmt.Sprintf("Call the reindex service for %d - %s", sirsiMetadata.ID, sirsiMetadata.CatalogKey))
	url := fmt.Sprintf("%s/api/reindex/%s", svc.ReindexURL, sirsiMetadata.CatalogKey)
	_, resp := svc.putRequest(url)
	if resp != nil {
		svc.logError(js, fmt.Sprintf("%s reindex request failed: %d: %s", sirsiMetadata.CatalogKey, resp.StatusCode, resp.Message))
		return fmt.Errorf("reindex request failed %d:%s", resp.StatusCode, resp.Message)
	}
	svc.logInfo(js, fmt.Sprintf("%s reindex request successful", sirsiMetadata.CatalogKey))

	// Lastly, flag the deliverables ready date if it is not already set
	if tgtUnit.DateDLDeliverablesReady == nil {
		svc.logInfo(js, "Set date unit deliverables ready")
		tgtUnit.DateDLDeliverablesReady = &now
		err = svc.GDB.Model(tgtUnit).Select("DateDLDeliverablesReady").Updates(*tgtUnit).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to set DateDLDeliverablesReady: %s", err.Error()))
		}
	}

	svc.logInfo(js, fmt.Sprintf("Unit and %d master files have been published to the DL", len(masterfiles)))
	return nil
}
