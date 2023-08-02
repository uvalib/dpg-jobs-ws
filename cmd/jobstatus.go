package main

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type eventLevel uint

// Event levels for job status reporting from rails enum:[:info, :warning, :error, :fatal] - warning is never used
const (
	Info  eventLevel = 0
	Warn  eventLevel = 1
	Error eventLevel = 2
	Fatal eventLevel = 3
)

type event struct {
	ID          int64
	JobStatusID int64
	Level       eventLevel
	Text        string
	CreatedAt   time.Time
}

type jobStatus struct {
	ID             int64      `json:"id"`
	OriginatorID   int64      `json:"-"`
	OriginatorType string     `json:"-"`
	Name           string     `json:"name"`
	Status         string     `json:"status"`
	Failures       uint       `json:"failures"`
	Error          string     `json:"error"`
	Events         []event    `gorm:"foreignKey:JobStatusID" json:"-"`
	StartedAt      *time.Time `json:"startedAt"`
	EndedAt        *time.Time `json:"endedAt"`
	CreatedAt      time.Time  `json:"-"`
	UpdatedAt      time.Time  `json:"-"`
}

func (svc *ServiceContext) createJobStatus(job string, origType string, origID int64) (*jobStatus, error) {
	log.Printf("INFO: create job status %s %s %d", job, origType, origID)
	now := time.Now()
	js := jobStatus{OriginatorID: origID, OriginatorType: origType, Name: job, Status: "running", StartedAt: &now}
	err := svc.GDB.Create(&js).Error
	if err != nil {
		return nil, err
	}
	return &js, nil
}

func (svc *ServiceContext) jobDone(status *jobStatus) {
	if status.EndedAt == nil {
		e := event{JobStatusID: status.ID, Level: Info, Text: "job finished"}
		err := svc.GDB.Create(&e).Error
		if err != nil {
			log.Printf("ERROR: unable to log job %d done event: %s", status.ID, err.Error())
		}

		now := time.Now()
		svc.GDB.Model(&status).Select("ended_at", "status").Updates(jobStatus{EndedAt: &now, Status: "finished"})
		log.Printf("INFO: [job %d finished] %s", status.ID, status.Name)
	}
}

func (svc *ServiceContext) logInfo(status *jobStatus, text string) {
	if status == nil {
		log.Printf("INFO: %s", text)
		return
	}
	log.Printf("INFO: [job %d info]: %s", status.ID, text)
	e := event{JobStatusID: status.ID, Level: Info, Text: text}
	err := svc.GDB.Create(&e).Error
	if err != nil {
		log.Printf("ERROR: unable to log job %d info event [%s]: %s", status.ID, text, err.Error())
	}
}

func (svc *ServiceContext) logError(status *jobStatus, text string) {
	if status == nil {
		log.Printf("WARNING: %s", text)
		return
	}
	log.Printf("INFO: [job %d error]: %s", status.ID, text)
	e := event{JobStatusID: status.ID, Level: Error, Text: text}
	err := svc.GDB.Create(&e).Error
	if err != nil {
		log.Printf("ERROR: unable to log job %d error event [%s]: %s", status.ID, text, err.Error())
	}
	svc.GDB.Model(status).Select("failures").Updates(jobStatus{Failures: status.Failures + 1})
}

func (svc *ServiceContext) logFatal(status *jobStatus, text string) {
	if status.EndedAt == nil {
		log.Printf("INFO: [job %d fatal]: %s", status.ID, text)
		e := event{JobStatusID: status.ID, Level: Fatal, Text: text}
		err := svc.GDB.Create(&e).Error
		if err != nil {
			log.Printf("ERROR: unable to log job %d fatal event [%s]: %s", status.ID, text, err.Error())
		}
		now := time.Now()
		svc.GDB.Model(status).Select("ended_at", "status", "error").Updates(jobStatus{EndedAt: &now, Status: "failure", Error: text})
	}
}

func (svc *ServiceContext) getJobStatus(c *gin.Context) {
	jID := c.Param("id")
	var js jobStatus
	err := svc.GDB.Find(&js, jID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.String(http.StatusNotFound, "not found")
		} else {
			c.String(http.StatusInternalServerError, err.Error())
		}
		return
	}
	c.JSON(http.StatusOK, js)
}
