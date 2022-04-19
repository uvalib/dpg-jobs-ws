package main

import (
	"fmt"
	"log"
	"net/http"
	"path"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) finalizeUnit(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("FinalizeUnit", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create FinalizeUnit job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var tgtUnit unit
	err = svc.GDB.Preload("Metadata").Preload("IntendedUse").First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if tgtUnit.Reorder {
		svc.logFatal(js, "Unit is a re-order and should not be finalized.")
		c.String(http.StatusBadRequest, "unit is a reorder and cannot be finalized")
		return
	}

	act := "begins"
	if tgtUnit.UnitStatus == "error" {
		act = "restarts"
	}
	if tgtUnit.ProjectID != nil {
		svc.logInfo(js, fmt.Sprintf("Project %d, unit %d %s finalization.", *tgtUnit.ProjectID, unitID, act))
	} else {
		svc.logInfo(js, fmt.Sprintf("Unit %d %s finalization  without project.", unitID, act))
	}

	go func() {
		srcDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", unitID))
		if pathExists(srcDir) == false {
			tgtUnit.UnitStatus = "error"
			tgtUnit.UpdatedAt = time.Now()
			svc.GDB.Model(&tgtUnit).Select("UnitStatus", "UpdatedAt").Updates(tgtUnit)
			svc.logFatal(js, fmt.Sprintf("Finalization directory %s does not exist", srcDir))
			return
		}

		svc.logFatal(js, "Fail finalize with incomplete logic")
		// svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}
