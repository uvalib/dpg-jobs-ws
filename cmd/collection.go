package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) collectionBulkAdd(c *gin.Context) {
	collectionMetadataID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("CollectionAdd", "Metadata", collectionMetadataID)
	if err != nil {
		log.Printf("ERROR: unable to create CollectionAdd job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var req struct {
		MetadataIDs []int64 `json:"items"`
	}
	err = c.BindJSON(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Invalid add collection %d items request: %s", collectionMetadataID, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Add metadata records %v to collection %d", req.MetadataIDs, collectionMetadataID))
	go func() {
		defer func() {
			if r := recover(); r != nil {
				svc.logFatal(js, fmt.Sprintf("Panic recovered %v", r))
				debug.PrintStack()
			}
		}()
		for _, mdID := range req.MetadataIDs {
			svc.logInfo(js, fmt.Sprintf("Processing metadata %d; get all associated digitial collection building units", mdID))

			// get all DIGITAL COLLECTION BUILDING units that are associated with this record
			var mdUnits []unit
			err = svc.GDB.Where("metadata_id=? and intended_use_id=?", mdID, 110).Find(&mdUnits).Error
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to load units for target metadata %d; skipping. Error: %s", mdID, err.Error()))
				continue
			}
			svc.logInfo(js, fmt.Sprintf("Found %d units; processing each", len(mdUnits)))

			for _, tgtUnit := range mdUnits {
				// see if the master files that are owned by this unit have different metadata than the unit
				svc.logInfo(js, fmt.Sprintf("Check masterfiles for unit %d to see if all have the same metadata", tgtUnit.ID))
				var mdIDs []int64
				err = svc.GDB.Table("master_files").Where("unit_id=?", tgtUnit.ID).Distinct("metadata_id").Scan(&mdIDs).Error
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to determine if masterfiles of unit %d have one or more metadata records: %s", mdID, err.Error()))
					continue
				}

				if len(mdIDs) == 1 {
					svc.logInfo(js, fmt.Sprintf("Master files of unit %d all have the same metadata record %d; adding it to collection %d", tgtUnit.ID, *tgtUnit.MetadataID, collectionMetadataID))
					err = svc.GDB.Table("metadata").Where("id = ?", mdIDs[0]).Update("parent_metadata_id", collectionMetadataID).Error
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to update parent metadata of metadata %d: %s", mdIDs[0], err.Error()))
						continue
					}
				} else {
					svc.logInfo(js, fmt.Sprintf("Update %d distinct metadata records for master files of unit %d to point to metadata %d as their parent collection",
						len(mdIDs), tgtUnit.ID, collectionMetadataID))
					err = svc.GDB.Debug().Table("metadata").Where("id in ?", mdIDs).Update("parent_metadata_id", collectionMetadataID).Error
					if err != nil {
						svc.logError(js, fmt.Sprintf("Unable to batch update parent metadata for %v: %s", mdIDs, err.Error()))
						continue
					}
				}
			}

			svc.logInfo(js, "Collection has successfully been updated")
			svc.jobDone(js)
		}
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}