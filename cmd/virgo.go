package main

import (
	"fmt"
	"time"
)

func (svc *ServiceContext) publishToVirgo(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "Publish unit to Virgo")
	if tgtUnit.Metadata.AvailabilityPolicyID == nil {
		return fmt.Errorf("Metadata %d for Unit is missing the required availability policy", tgtUnit.MetadataID)
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
		tgtUnit.Metadata.DateDlIngest = &now
		svc.GDB.Model(tgtUnit).Select("DateDlIngest").Updates(*tgtUnit)
	} else {
		tgtUnit.Metadata.DateDlUpdate = &now
		svc.GDB.Model(tgtUnit).Select("DateDlUpdate").Updates(*tgtUnit)
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
		if mf.MetadataID != tgtUnit.MetadataID {
			var mfMD metadata
			err = svc.GDB.First(&mfMD, mf.MetadataID).Error
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
		svc.logInfo(js, fmt.Sprintf("Call the reindex service for %d - %s", tgtUnit.MetadataID, tgtUnit.Metadata.CatalogKey))
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
