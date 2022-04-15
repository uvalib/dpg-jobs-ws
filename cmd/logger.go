package main

import (
	"log"
	"time"
)

type event struct {
	ID          int64
	JobStatusID int64
	Level       uint // rails enumerated type [:info, :warning, :error, :fatal] - warning is never used
	Text        string
	CreatedAt   time.Time
}

type jobStatus struct {
	ID             int64
	OriginatorID   int64
	OriginatorType string
	Name           string
	Status         string
	Failures       uint
	Error          string
	Events         []event `gorm:"foreignKey:JobStatusID"`
	StartedAt      *time.Time
	EndedAt        *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (svc *ServiceContext) createJobStatus(job string, origType string, origID int64) (*jobStatus, error) {
	log.Printf("INFO: create job status %s %s %d", job, origType, origID)
	now := time.Now()
	js := jobStatus{OriginatorID: origID, OriginatorType: origType, Name: job, Status: "running",
		StartedAt: &now, CreatedAt: now, UpdatedAt: now}
	err := svc.GDB.Create(&js).Error
	if err != nil {
		return nil, err
	}
	return &js, nil
}

func (svc *ServiceContext) jobDone(status *jobStatus) {
	if status.EndedAt == nil {
		e := event{JobStatusID: status.ID, Level: 0, Text: "job finished", CreatedAt: time.Now()}
		err := svc.GDB.Create(&e).Error
		if err != nil {
			log.Printf("ERROR: unable to log job %d done event: %s", status.ID, err.Error())
		}

		now := time.Now()
		svc.GDB.Model(&status).Select("ended_at", "status").Updates(jobStatus{EndedAt: &now, Status: "finished"})
	}
}

func (svc *ServiceContext) logInfo(status *jobStatus, text string) {
	log.Printf("INFO: [job %d info]: %s", status.ID, text)
	e := event{JobStatusID: status.ID, Level: 0, Text: text, CreatedAt: time.Now()}
	err := svc.GDB.Create(&e).Error
	if err != nil {
		log.Printf("ERROR: unable to log job %d info event [%s]: %s", status.ID, text, err.Error())
	}
}

func (svc *ServiceContext) logError(status *jobStatus, text string) {
	log.Printf("INFO: [job %d error]: %s", status.ID, text)
	e := event{JobStatusID: status.ID, Level: 2, Text: text, CreatedAt: time.Now()}
	err := svc.GDB.Create(&e).Error
	if err != nil {
		log.Printf("ERROR: unable to log job %d error event [%s]: %s", status.ID, text, err.Error())
	}
	svc.GDB.Model(status).Select("failures").Updates(jobStatus{Failures: status.Failures + 1})
}

func (svc *ServiceContext) logFatal(status *jobStatus, text string) {
	if status.EndedAt == nil {
		log.Printf("INFO: [job %d fatal]: %s", status.ID, text)
		e := event{JobStatusID: status.ID, Level: 3, Text: text, CreatedAt: time.Now()}
		err := svc.GDB.Create(&e).Error
		if err != nil {
			log.Printf("ERROR: unable to log job %d fatal event [%s]: %s", status.ID, text, err.Error())
		}
		now := time.Now()
		svc.GDB.Model(status).Select("ended_at", "status", "error").Updates(jobStatus{EndedAt: &now, Status: "failure", Error: text})
	}
}
