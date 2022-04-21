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
		svc.logInfo(js, fmt.Sprintf("Unit %d %s finalization without project.", unitID, act))
	}

	go func() {
		srcDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", unitID))
		if pathExists(srcDir) == false {
			svc.setUnitStatus(&tgtUnit, "error")
			svc.logFatal(js, fmt.Sprintf("Finalization directory %s does not exist", srcDir))
			return
		}

		// manage unit status
		if tgtUnit.UnitStatus == "finalizing" {
			svc.logFatal(js, "Unit is already finalizing.")
			return
		}

		if tgtUnit.UnitStatus == "approved" {
			svc.GDB.Model(order{ID: tgtUnit.OrderID}).Update("date_finalization_begun", time.Now())
			svc.logInfo(js, fmt.Sprintf("Date Finalization Begun updated for order %d", tgtUnit.OrderID))
		} else if tgtUnit.UnitStatus != "error" {
			svc.logFatal(js, "Unit has not been approved.")
			return
		}
		svc.setUnitStatus(&tgtUnit, "finalizing")

		err = svc.qaUnit(js, &tgtUnit)
		if err != nil {
			svc.setUnitStatus(&tgtUnit, "error")
			svc.logFatal(js, err.Error())
			return
		}

		svc.logFatal(js, "Fail finalize with incomplete logic")
		// svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) setUnitStatus(tgtUnit *unit, status string) {
	tgtUnit.UnitStatus = status
	svc.GDB.Model(&tgtUnit).Select("UnitStatus").Updates(tgtUnit)
}

func (svc *ServiceContext) qaUnit(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "QA unit data")

	// First, check if unit is assigned to metadata record. This is an immediate fail
	if tgtUnit.MetadataID == nil {
		return fmt.Errorf("Unit is not assigned to a metadata record")
	}

	if tgtUnit.IncludeInDL == false && tgtUnit.Reorder == false {
		//check_auto_publish TODO
	}

	hasFailures := false
	if tgtUnit.IncludeInDL && tgtUnit.Metadata.AvailabilityPolicyID == nil && tgtUnit.Metadata.Type != "ExternalMetadata" {
		svc.logError(js, "Availability policy must be set for all units flagged for inclusion in the DL")
		hasFailures = true
	}

	if tgtUnit.IntendedUseID == nil {
		svc.logError(js, "Unit has no intended use.  All units that participate in this workflow must have an intended use.")
		hasFailures = true
	}

	// # fail for no ocr hint or incompatible hint / ocr Settings
	// if tgtUnit.metadata.ocr_hint_id.nil?
	// 	log_failure "Unit metadata #{tgtUnit.metadata.id} has no OCR Hint. This is a required setting."
	// 	hasFailures = true
	// else
	// 	if tgtUnit.ocr_master_files
	// 		if !tgtUnit.metadata.ocr_hint.ocr_candidate
	// 			log_failure "Unit is flagged to perform OCR, but the metadata setting indicates OCR is not possible."
	// 			hasFailures = true
	// 		end
	// 		if tgtUnit.metadata.ocr_language_hint.nil?
	// 			log_failure "Unit is flagged to perform OCR, but the required language hint for metadata #{tgtUnit.metadata.id} is not set"
	// 			hasFailures = true
	// 		end
	// 	end
	// end

	// if tgtUnit.include_in_dl && tgtUnit.throw_away
	// 	log_failure "Throw away units cannot be flagged for publication to the DL."
	// 	hasFailures = true
	// end

	// order = tgtUnit.order
	// if not order.date_order_approved?
	// 	logger.info "Order #{order.id} is not marked as approved.  Since this unit is undergoing finalization, the workflow has automatically updated this value and changed the order_status to approved."
	// 	if !order.update(date_order_approved: Time.now, order_status: 'approved')
	// 		fatal_error( order.errors.full_messages.to_sentence )
	// 	end
	// end

	if hasFailures {
		return fmt.Errorf("Unit has failed the QA Unit Data Processor")
	}
	return nil
}
