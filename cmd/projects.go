package main

import (
	"fmt"
	"log"
	"math"
	"path"
	"time"
)

type problem struct {
	ID    int64
	Name  string
	Label string
}

type note struct {
	ID            int64
	ProjectID     int64
	StepID        int64
	NoteType      uint
	Note          string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Problems      []problem `gorm:"many2many:notes_problems"  json:"problems"`
	StaffMemberID int64
}

type assignment struct {
	ID              int64
	ProjectID       int64
	StepID          int64
	StartedAt       *time.Time
	FinishedAt      *time.Time
	DurationMinutes uint
	Status          uint
}

type project struct {
	ID              int64
	OwnerID         *int64
	CurrentStepID   *int64
	ContainerTypeID *int64
	FinishedAt      *time.Time
	Notes           []*note `gorm:"foreignKey:ProjectID"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (svc *ServiceContext) projectFailedFinalization(js *jobStatus, currProj *project) {
	log.Printf("INFO: Project [%d] FAILED finalization", currProj.ID)
	var activeAssign assignment
	err := svc.GDB.Where("project_id=?", currProj.ID).Order("assigned_at DESC").Limit(1).First(&activeAssign).Error
	if err != nil {
		log.Printf("ERROR: unable to get  active assignment for failed project %d: %s", currProj.ID, err.Error())
		svc.logError(js, fmt.Sprintf("Unable to get active assignment for project %d: %s", currProj.ID, err.Error()))
		return
	}

	// Fail the step and increase time spent
	startTime := *js.StartedAt
	endTime := *js.EndedAt
	diff := endTime.Sub(startTime)
	processingMins := uint(math.Round(diff.Seconds() / 60.0))
	qaMins := activeAssign.DurationMinutes
	log.Printf("INFO: project [%d] finalization minutes: %d, prior minutes: %d", currProj.ID, processingMins, qaMins)
	activeAssign.DurationMinutes = processingMins + qaMins
	activeAssign.Status = 4 // error
	svc.GDB.Model(&activeAssign).Select("DurationMinutes", "Status").Updates(activeAssign)

	// Add a problem note with a summary of the issue
	msg := fmt.Sprintf("<p>%s</p>", js.Error)
	msg += "<p>Please manually correct the finalization problems. Once complete, press the Finish button to restart finalization.</p>"
	msg += fmt.Sprintf("<p>Error details <a href='%s/job_statuses/%d'>here</a></p>", svc.TrackSys.Admin, js.ID)
	svc.addFinalizeFailNote(currProj, msg)
}

func (svc *ServiceContext) projectFinishedFinalization(js *jobStatus, currProj *project, tgtUnit *unit) error {
	log.Printf("INFO: Project [%d] completed finalization", currProj.ID)
	startTime := *js.StartedAt
	endTime := time.Now()
	diff := endTime.Sub(startTime)
	processingMins := uint(math.Round(diff.Seconds() / 60.0))

	var activeAssign assignment
	err := svc.GDB.Where("project_id=?", currProj.ID).Order("assigned_at DESC").Limit(1).First(&activeAssign).Error
	if err != nil {
		return fmt.Errorf("unable to get finalization assignment: %s", err.Error())
	}

	var masterfiles []masterFile
	err = svc.GDB.Preload("ImageTechMeta").Where("unit_id=?", tgtUnit.ID).Find(&masterfiles).Error
	if err != nil {
		return fmt.Errorf("Unable to get updated list masterfiles: %s", err.Error())
	}

	svc.logInfo(js, "Validating finalized unit")
	if tgtUnit.ThrowAway == false {
		if tgtUnit.DateArchived == nil {
			svc.validationFailed(currProj, &activeAssign, "Unit was not archived")
			return fmt.Errorf("unit was not archived")
		}
		archiveDir := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", tgtUnit.ID))
		tifFiles, _ := getTifFiles(archiveDir, tgtUnit.ID)
		if len(tifFiles) == 0 {
			svc.validationFailed(currProj, &activeAssign, "No tif files found in archive")
			return fmt.Errorf("No tif files found in archive")
		}
		if len(tifFiles) != len(masterfiles) {
			svc.validationFailed(currProj, &activeAssign, fmt.Sprintf("MasterFile / tif count mismatch. %d tif files vs %d MasterFiles", len(tifFiles), len(masterfiles)))
			return fmt.Errorf("Archived file count different from unit masterfiles count")
		}
	}

	for _, mf := range masterfiles {
		if mf.MetadataID == nil {
			reason := fmt.Sprintf("Masterfile %s missing desc metadata", mf.Filename)
			svc.validationFailed(currProj, &activeAssign, reason)
			return fmt.Errorf(reason)
		}
		if mf.ImageTechMeta.ID == 0 {
			reason := fmt.Sprintf("Masterfile %s missing desc tech metadata", mf.Filename)
			svc.validationFailed(currProj, &activeAssign, reason)
			return fmt.Errorf(reason)
		}
	}

	// deliverables ready (patron or dl)
	if tgtUnit.IntendedUse.ID == 110 {
		if tgtUnit.IncludeInDL && tgtUnit.DateDLDeliverablesReady == nil {
			svc.validationFailed(currProj, &activeAssign, "DL deliverables ready date not set")
			return fmt.Errorf("DL deliverables ready date not set")
		}
	} else {
		if tgtUnit.DatePatronDeliverablesReady == nil {
			svc.validationFailed(currProj, &activeAssign, "Patron deliverables ready date not set")
			return fmt.Errorf("Patron deliverables ready date not set")
		}
	}

	// all validations have passed. the project finalization has successfully completed
	now := time.Now()
	currProj.FinishedAt = &now
	currProj.OwnerID = nil
	currProj.CurrentStepID = nil
	err = svc.GDB.Model(currProj).Select("FinishedAt", "OwnerID", "CurrentStepID").Updates(*currProj).Error
	if err != nil {
		return err
	}

	activeAssign.FinishedAt = &now
	activeAssign.Status = 2 // finished
	activeAssign.DurationMinutes = activeAssign.DurationMinutes + processingMins
	err = svc.GDB.Model(&activeAssign).Select("FinishedAt", "Status", "DurationMinutes").Updates(activeAssign).Error
	if err != nil {
		return err
	}

	log.Printf("INFO: project %d finalization minutes: %d", currProj.ID, processingMins)
	svc.logInfo(js, fmt.Sprintf("Total finalization minutes: %d", processingMins))
	return nil
}

func (svc *ServiceContext) addFinalizeFailNote(currProj *project, message string) {
	newNote := note{ProjectID: currProj.ID, NoteType: 2, Note: message}
	if currProj.CurrentStepID != nil {
		newNote.StepID = *currProj.CurrentStepID
	}
	if currProj.OwnerID != nil {
		newNote.StaffMemberID = *currProj.OwnerID
	}
	err := svc.GDB.Model(currProj).Association("Notes").Append(&newNote)
	if err != nil {
		log.Printf("ERROR: unable to add note to project %d: %s", currProj.ID, err.Error())
		return
	}

	pq := fmt.Sprintf("insert into notes_problems (note_id, problem_id) values (%d, 6)", newNote.ID) // 6 is finalize fail
	err = svc.GDB.Exec(pq).Error
	if err != nil {
		log.Printf("ERROR: unable to add problems to note: %s", err.Error())
	}
}

func (svc *ServiceContext) validationFailed(currProj *project, activeAssign *assignment, reason string) {
	msg := fmt.Sprintf("<p>Validation of finalization failed: %s</p>", reason)
	svc.addFinalizeFailNote(currProj, msg)
	activeAssign.Status = 4 // error
	svc.GDB.Model(activeAssign).Select("Status").Updates(*activeAssign)
}
