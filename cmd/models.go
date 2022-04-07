package main

import (
	"database/sql"
	"time"
)

type academicStatus struct {
	ID   int64
	Name string
}

type intendedUse struct {
	ID                    int64
	DeliverableFormat     string
	DeliverableResolution string
}

type customer struct {
	ID               int64
	FirstName        string
	LastName         string
	Email            string
	AcademicStatusID uint
	AcademicStatus   academicStatus `gorm:"foreignKey:AcademicStatusID"`
}

type invoice struct {
	ID          int64
	OrderID     int64
	DateFeePaid *time.Time
}

type metadata struct {
	ID          int64
	PID         string `gorm:"column:pid"`
	Type        string
	Title       string
	CreatorName string
	CallNumber  string
	Barcoode    string
}

type masterFile struct {
	ID          int64
	PID         string `gorm:"column:pid"`
	UnitID      int64
	Filename    string
	Title       string
	Description string
}

type unit struct {
	ID                          int64
	OrderID                     int64
	MetadataID                  uint
	Metadata                    metadata `gorm:"foreignKey:MetadataID"`
	UnitStatus                  string
	IntendedUseID               uint
	IntendedUse                 intendedUse `gorm:"foreignKey:IntendedUseID"`
	RemoveWatermark             bool
	Reorder                     bool
	CommpleteScan               bool
	ThrowAway                   bool
	OCRMasterFiles              bool         `gorm:"column:ocr_master_files"`
	MasterFiles                 []masterFile `gorm:"foreignKey:UnitID"`
	DateArchived                *time.Time
	DatePatronDeliverablesReady *time.Time
	DateDLDeliverablesReady     *time.Time `gorm:"column:date_dl_deliverables_ready"`
}

type order struct {
	ID                             int64
	OrderStatus                    string
	CustomerID                     uint
	Customer                       customer `gorm:"foreignKey:CustomerID"`
	Fee                            sql.NullFloat64
	Invoices                       []invoice `gorm:"foreignKey:OrderID"`
	Units                          []unit    `gorm:"foreignKey:OrderID"`
	DateRequestSubmitted           time.Time
	DateCustomerNotified           *time.Time
	DatePatronDeliverablesComplete *time.Time
	DateArchivingComplete          *time.Time
	DateFinalizationBegun          *time.Time
	DateFeeEstimateSentToCustomer  *time.Time
}
