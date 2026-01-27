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
	if err := c.BindJSON(&req); err != nil {
		svc.logFatal(js, fmt.Sprintf("Invalid add collection %d items request: %s", collectionMetadataID, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	svc.logInfo(js, "Check for selected metadata records that are already part of a collection")
	var inCollectionIDs []int64
	err = svc.GDB.Raw("select id from metadata where id in ? and parent_metadata_id  != ?", req.MetadataIDs, 0).Scan(&inCollectionIDs).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to find records already in a collection: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if len(inCollectionIDs) > 0 {
		svc.logFatal(js, fmt.Sprintf("Metadata records %v are already part of a collection", inCollectionIDs))
		c.String(http.StatusBadRequest, "Metadata records %v are already part of a collection", inCollectionIDs)
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
			svc.logInfo(js, fmt.Sprintf("Processing metadata %d; get all associated units", mdID))
			var mdUnits []unit
			if err := svc.GDB.Where("metadata_id=?", mdID).Find(&mdUnits).Error; err != nil {
				svc.logError(js, fmt.Sprintf("Unable to load units for target metadata %d; skipping. Error: %s", mdID, err.Error()))
				continue
			}

			if len(mdUnits) == 0 {
				svc.logInfo(js, fmt.Sprintf("No units directly found for metadata %d; searching master files...", mdID))
				if err := svc.GDB.Joins("inner join master_files mf on mf.unit_id = units.id").
					Joins("inner join metadata m on m.id = mf.metadata_id").
					Where("m.id=?", mdID).Distinct("units.id").Find(&mdUnits).Error; err != nil {
					svc.logError(js, fmt.Sprintf("Unable to load units for target metadata %d; skipping. Error: %s", mdID, err.Error()))
					continue
				}

				// still no units; just add the parent metadata to this record and move on to the next, regardless of success/fail
				if len(mdUnits) == 0 {
					if err := svc.GDB.Table("metadata").Where("id = ?", mdID).Update("parent_metadata_id", collectionMetadataID).Error; err != nil {
						svc.logError(js, fmt.Sprintf("Unable to update parent metadata of metadata %d: %s", mdID, err.Error()))
					}
					continue
				}
			}

			svc.logInfo(js, fmt.Sprintf("Found %d unit(s) for metadata %d; processing each", len(mdUnits), mdID))
			for _, tgtUnit := range mdUnits {
				// see if the master files that are owned by this unit have different metadata than the unit
				svc.logInfo(js, fmt.Sprintf("Check masterfiles for unit %d to see if all have the same metadata", tgtUnit.ID))
				var mdIDs []int64
				if err := svc.GDB.Table("master_files").Where("unit_id=?", tgtUnit.ID).Distinct("metadata_id").Scan(&mdIDs).Error; err != nil {
					svc.logError(js, fmt.Sprintf("Unable to determine if masterfiles of unit %d have one or more metadata records: %s", mdID, err.Error()))
					continue
				}

				if len(mdIDs) == 1 {
					// if it is only 1 metadata record, it must be the same as the one specified in the request (mdID)
					svc.logInfo(js, fmt.Sprintf("Master files of unit %d all have the same metadata record %d; adding it to collection %d", tgtUnit.ID, *tgtUnit.MetadataID, collectionMetadataID))
					if err := svc.GDB.Table("metadata").Where("id = ?", mdID).Update("parent_metadata_id", collectionMetadataID).Error; err != nil {
						svc.logError(js, fmt.Sprintf("Unable to update parent metadata of metadata %d: %s", mdID, err.Error()))
						continue
					}
				} else {
					svc.logInfo(js, fmt.Sprintf("Update %d distinct metadata records for master files of unit %d to point to metadata %d as their parent collection",
						len(mdIDs), tgtUnit.ID, collectionMetadataID))
					if err := svc.GDB.Table("metadata").Where("id in ?", mdIDs).Update("parent_metadata_id", collectionMetadataID).Error; err != nil {
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
