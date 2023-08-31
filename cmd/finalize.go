package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
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

	// preconditions for starting job; unit exists and is in a valid state and not a reorder
	var tgtUnit unit
	err = svc.GDB.Preload("Metadata").Preload("Metadata.OcrHint").
		Preload("Order").Preload("IntendedUse").First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if tgtUnit.UnitStatus == "finalizing" {
		svc.logFatal(js, "Unit is already finalizing.")
		c.String(http.StatusBadRequest, "Unit is already finalizing")
		return
	}
	if tgtUnit.Reorder {
		svc.logFatal(js, "Unit is a re-order and should not be finalized.")
		c.String(http.StatusBadRequest, "Unit is a re-order and should not be finalized")
		return
	}

	// log the start / restart of finalization
	act := "begins"
	if tgtUnit.UnitStatus == "error" {
		act = "restarts"
	}
	svc.logInfo(js, fmt.Sprintf("Unit %d %s finalization", unitID, act))

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.setUnitFatal(js, &tgtUnit, fmt.Sprintf("%v", r))
			}
		}()

		svc.logInfo(js, "Check for presence of finalization directory")
		srcDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", unitID))
		if pathExists(srcDir) == false {
			svc.setUnitFatal(js, &tgtUnit, fmt.Sprintf("Finalization directory %s does not exist.", srcDir))
			return
		}

		if tgtUnit.UnitStatus == "approved" {
			svc.GDB.Model(order{ID: tgtUnit.OrderID}).Update("date_finalization_begun", time.Now())
			svc.logInfo(js, fmt.Sprintf("Date Finalization Begun updated for order %d", tgtUnit.OrderID))
		} else if tgtUnit.UnitStatus != "error" {
			svc.setUnitFatal(js, &tgtUnit, "Unit has not been approved.")
			return
		}
		svc.setUnitStatus(&tgtUnit, "finalizing")
		svc.logInfo(js, "Status set to finalizing")

		err = svc.qaUnit(js, &tgtUnit)
		if err != nil {
			svc.setUnitFatal(js, &tgtUnit, err.Error())
			return
		}

		err = svc.qaFilesystem(js, &tgtUnit, srcDir)
		if err != nil {
			svc.setUnitFatal(js, &tgtUnit, err.Error())
			return
		}

		// Create all of the master files, publish to IIIF then archive the unit
		err = svc.importImages(js, &tgtUnit, srcDir)
		if err != nil {
			svc.setUnitFatal(js, &tgtUnit, err.Error())
			return
		}

		// If OCR has been requested, do it AFTER archive (OCR requires tif to be in archive)
		// but before deliverable generation (deliverables require OCR text to be present)
		if tgtUnit.OcrMasterFiles {
			err = svc.requestUnitOCR(js, &tgtUnit)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to request OCR: %s", err.Error()))
			}
		}

		// Is this item flagged for publication into Virgo?
		if tgtUnit.IncludeInDL {
			// NOTE: neither publish method will trigger a logFatal, but will log and return
			// an error if one was encountered. Ignore it here as a problem publishing should
			// not cause finalization to fail; the error log is enough.
			if tgtUnit.Metadata.Type == "SirsiMetadata" {
				svc.publishSirsiToVirgo(js, tgtUnit.Metadata, &tgtUnit)
			} else if tgtUnit.Metadata.Type == "XmlMetadata" {
				svc.publishXMLToVirgo(js, tgtUnit.Metadata, &tgtUnit)
			} else {
				svc.logError(js, fmt.Sprintf("Unit is flagged for inclusion in DL, but metadata %d type %s is not supported", tgtUnit.Metadata.ID, tgtUnit.Metadata.Type))
			}
		}

		// If desc is not digital collection building, create patron deliverables regardless of any other settings
		if tgtUnit.IntendedUse.ID != 110 {
			if tgtUnit.DatePatronDeliverablesReady == nil {
				if tgtUnit.IntendedUse.DeliverableFormat == "pdf" {
					err = svc.createPatronPDF(js, &tgtUnit)
					if err != nil {
						svc.setUnitFatal(js, &tgtUnit, fmt.Sprintf("Unable to create patron PDF: %s", err.Error()))
						return
					}
				} else {
					err = svc.zipPatronDeliverables(js, &tgtUnit)
					if err != nil {
						svc.setUnitFatal(js, &tgtUnit, fmt.Sprintf("Unable to create patron ZIP: %s", err.Error()))
						return
					}
				}

				now := time.Now()
				tgtUnit.DatePatronDeliverablesReady = &now
				svc.GDB.Model(&tgtUnit).Select("DatePatronDeliverablesReady").Updates(tgtUnit)
				svc.logInfo(js, "All patron deliverables created")
			} else {
				svc.logInfo(js, "Patron deliverables already generated")
			}

			// check for completeness, fees and generate manifest PDF. Same for all patron deliverables
			err = svc.checkOrderReadyForDelivery(js, tgtUnit.OrderID)
			if err != nil {
				svc.setUnitFatal(js, &tgtUnit, err.Error())
				return
			}
		}

		// At this point, finalization has completed and project (if it exists) can be validated.
		// any project validation failures will fail the finalization job. for units that have no projects,
		// finalization is done now and will be marked as successful. Note that only the ID is passed in. This
		// forces the unit to be reloaded to pick up any changes that may have occurred during teh finalize
		err = svc.unitFinishedFinalization(js, tgtUnit.ID)
		if err != nil {
			svc.setUnitFatal(js, &tgtUnit, err.Error())
			return
		}

		// Cleanup any tmo directories and move unit to ready_to_delete, then end the outstanding job
		svc.cleanupWorkDirectories(js, tgtUnit.ID)
		svc.jobDone(js)
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func (svc *ServiceContext) setUnitFatal(js *jobStatus, tgtUnit *unit, errMsg string) {
	svc.setUnitStatus(tgtUnit, "error")
	svc.logFatal(js, errMsg)

	currProj, err := svc.getUnitProject(tgtUnit.ID)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Project lookup failed: %s", err.Error()))
		return
	}
	if currProj != nil {
		svc.projectFailedFinalization(js, currProj)
	}
}

func (svc *ServiceContext) unitFinishedFinalization(js *jobStatus, tgtUnitID int64) error {
	svc.logInfo(js, "Unit finished finalization; reloading details")
	var tgtUnit unit
	err := svc.GDB.Preload("Metadata").Preload("Metadata.OcrHint").
		Preload("Order").Preload("IntendedUse").First(&tgtUnit, tgtUnitID).Error
	if err != nil {
		return fmt.Errorf("error looking up finalized unit: %s", err.Error())
	}

	currProj, err := svc.getUnitProject(tgtUnit.ID)
	if err != nil {
		return fmt.Errorf("error looking up project: %s", err.Error())
	}

	if currProj == nil {
		svc.logInfo(js, "Set unit status to done")
		svc.setUnitStatus(&tgtUnit, "done")
		return nil
	}

	// a project is associated, walk through some final project validations. any errors will fail finalization.
	svc.logInfo(js, "Unit is associated with a project; call projectFinishedFinalization")
	return svc.projectFinishedFinalization(js, currProj, &tgtUnit)
}

func (svc *ServiceContext) setUnitStatus(tgtUnit *unit, status string) {
	tgtUnit.UnitStatus = status
	svc.GDB.Model(&tgtUnit).Select("UnitStatus").Updates(tgtUnit)
}

func (svc *ServiceContext) qaUnit(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "QA unit data")

	// First, check if unit is assigned to metadata record. This is an immediate fail
	svc.logInfo(js, "Verify metadata")
	if tgtUnit.MetadataID == nil {
		return fmt.Errorf("Unit is not assigned to a metadata record")
	}

	svc.logInfo(js, "Verify DL settings")
	if tgtUnit.IncludeInDL == false && tgtUnit.Reorder == false {
		svc.autoPublish(js, tgtUnit)
	}

	hasFailures := false
	svc.logInfo(js, "Verify availability policy")
	if tgtUnit.IncludeInDL && tgtUnit.Metadata.AvailabilityPolicyID == nil && tgtUnit.Metadata.Type != "ExternalMetadata" {
		svc.logError(js, "Availability policy must be set for all units flagged for inclusion in the DL")
		hasFailures = true
	}

	svc.logInfo(js, "Verify intended use")
	if tgtUnit.IntendedUseID == nil {
		svc.logError(js, "Unit has no intended use.  All units that participate in this workflow must have an intended use.")
		hasFailures = true
	}

	// fail for no ocr hint or incompatible hint / ocr Settings
	svc.logInfo(js, "Verify OCR settings")
	if tgtUnit.Metadata.OcrHintID == nil {
		svc.logError(js, fmt.Sprintf("Unit metadata %d has no OCR Hint. This is a required setting.", *tgtUnit.MetadataID))
		hasFailures = true
	} else {
		if tgtUnit.OcrMasterFiles {
			if tgtUnit.Metadata.OcrHint.OcrCandidate == false {
				svc.logError(js, "Unit is flagged to perform OCR, but the metadata setting indicates OCR is not possible.")
				hasFailures = true
			}
			if tgtUnit.Metadata.OcrLanguageHint == "" {
				svc.logError(js, "Unit is flagged to perform OCR, but the required language hint for metadata #{tgtUnit.metadata.id} is not set")
				hasFailures = true
			}
		}
	}

	if tgtUnit.IncludeInDL && tgtUnit.ThrowAway {
		svc.logError(js, "Throw away units cannot be flagged for publication to the DL.")
		hasFailures = true
	}

	svc.logInfo(js, "Verify order status")
	tgtOrder := tgtUnit.Order
	if tgtOrder.DateOrderApproved == nil {
		now := time.Now()
		svc.logInfo(js, fmt.Sprintf("Order %d is not marked as approved. Since this unit is undergoing finalization, the workflow has automatically changed the status to approved.", tgtOrder.ID))
		tgtOrder.OrderStatus = "approved"
		tgtOrder.DateOrderApproved = &now
		err := svc.GDB.Model(&tgtOrder).Select("OrderStatus", "DateOrderApproved").Updates(tgtOrder).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to approve order: %s", err.Error()))
			hasFailures = true
		}
	}

	if hasFailures {
		return fmt.Errorf("Unit has failed the QA Unit Data Processor")
	}
	svc.logInfo(js, "Unit QA tests passed")
	return nil
}

func (svc *ServiceContext) autoPublish(js *jobStatus, tgtUnit *unit) {
	svc.logInfo(js, "Checking unit for auto-publish")
	if tgtUnit.CompleteScan == false {
		svc.logInfo(js, "Unit is not a complete scan and cannot be auto-published")
		return
	}

	if tgtUnit.Metadata.IsManuscript || tgtUnit.Metadata.IsPersonalItem {
		svc.logInfo(js, "Unit is for a manuscript or personal item and cannot be auto-published")
		return
	}

	if tgtUnit.Metadata.Type != "SirsiMetadata" {
		svc.logInfo(js, "Unit metadata is not from Sirsi and cannot be auto-published")
		return
	}

	// Check publication year before 1923
	pubYear := svc.getMarcPublicationYear(tgtUnit.Metadata)
	if pubYear != 0 && pubYear < 1923 {
		svc.logInfo(js, "Unit is a candidate for auto-publishing")
		if tgtUnit.Metadata.AvailabilityPolicyID == nil {
			one := int64(1)
			tgtUnit.Metadata.AvailabilityPolicyID = &one
			svc.GDB.Model(tgtUnit.Metadata).Select("AvailabilityPolicyID").Updates(*tgtUnit.Metadata)
		}
		tgtUnit.IncludeInDL = true
		svc.GDB.Model(tgtUnit).Select("IncludeInDL").Updates(*tgtUnit)
	} else {
		svc.logInfo(js, "Unit has no date or a date after 1923 and cannot be auto-published")
	}
}

func (svc *ServiceContext) qaFilesystem(js *jobStatus, tgtUnit *unit, srcDir string) error {
	svc.logInfo(js, "QA filesystem")

	// Checking for:
	// 1. Existence of TIF files.
	// 2. The TIF sequence has no gaps and starts at 1.
	// 3. All TIF files conform to the naming convention.
	// 4. No file is less than 1MB (1MB being a size arbitrarily determined to represent a "too small" file)
	// 5. No non-tif / non-txt files present
	hasFailures := false
	minSize := int64(1024 * 1024)
	mfRegex := regexp.MustCompile(fmt.Sprintf(`^%09d_\w{4,}\.tif$`, tgtUnit.ID))
	tifFiles := make([]string, 0)
	err := filepath.Walk(srcDir, func(fPath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if f.IsDir() == false && f.Name() != ".DS_Store" {
			ext := filepath.Ext(f.Name())
			if ext == ".tif" {
				if mfRegex.MatchString(f.Name()) == false {
					hasFailures = true
					svc.logError(js, fmt.Sprintf("Incorrectly named .tif file found: %s", path.Join(fPath, f.Name())))
				} else {
					tifFiles = append(tifFiles, f.Name())
				}
				if f.Size() < minSize {
					hasFailures = true
					svc.logError(js, fmt.Sprintf("%s filesize is less than %d and is very likely an incorrect file.", path.Join(fPath, f.Name()), minSize))
				}
			} else if ext != ".txt" {
				hasFailures = true
				svc.logError(js, fmt.Sprintf("Unexpected file found: %s", path.Join(fPath, f.Name())))
			}
		}
		return nil
	})

	if err != nil {
		return err
	}
	if len(tifFiles) == 0 {
		svc.logError(js, fmt.Sprintf("No .tif files found in %s", srcDir))
		hasFailures = true
	}

	sort.Strings(tifFiles)
	seq := 0
	for _, fn := range tifFiles {
		mfPageNum := getMasterFilePageNum(fn)
		if seq+1 != mfPageNum {
			hasFailures = true
			svc.logError(js, fmt.Sprintf("Out of sequence .tif file found: %s", fn))
		}
		seq++
	}

	if hasFailures {
		return fmt.Errorf("Unit  has failed the Filesystem QA")
	}
	svc.logInfo(js, "Filesystem QA tests passed")
	return nil
}
