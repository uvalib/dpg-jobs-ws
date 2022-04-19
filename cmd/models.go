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
	DateInvoice time.Time
	DateFeePaid *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type metadata struct {
	ID             int64
	PID            string `gorm:"column:pid"`
	Type           string
	Title          string
	CreatorName    string
	CallNumber     string
	Barcoode       string
	IsPersonalItem bool
}

type containerType struct {
	ID         int64
	Name       string
	hasFolders bool
}

type location struct {
	ID              int64
	ContainerTypeID int64
	ContainerType   containerType `gorm:"foreignKey:ContainerTypeID"`
	FolderID        string        `gorm:"column:folder_id"`
}

type imageTechMeta struct {
	ID           int64
	MasterFileID int64
	ImageFormat  string
	Width        uint
	Height       uint
	Resolution   uint
	ColorSpace   string
	Depth        uint
	Compression  string
	ColorProfile string
	Equipment    string
	Software     string
	Model        string
	ExifVersion  string
	CaptureDate  *time.Time
	ISO          uint `gorm:"column:iso"`
	ExposureBias string
	ExposureTime string
	Aperture     string
	FocalLength  float64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type masterFile struct {
	ID                int64
	PID               string        `gorm:"column:pid"`
	MetadataID        *int64        `gorm:"column:metadata_id"`
	ComponentID       *int64        `gorm:"column:component_id"`
	ImageTechMeta     imageTechMeta `gorm:"foreignKey:MasterFileID"`
	UnitID            int64
	Filename          string
	Title             string
	Description       string
	Locations         []location `gorm:"many2many:master_file_locations"`
	Filesize          int64
	MD5               string `gorm:"column:md5"`
	OriginalMfID      *int64 `gorm:"column:original_mf_id"`
	DateArchived      *time.Time
	DeaccessionedAt   *time.Time
	DeaccessionedByID *int64 `gorm:"column:deaccessioned_by_id"`
	DeaccessionNote   string
	TranscriptionText string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (mf *masterFile) location() *location {
	if len(mf.Locations) == 0 {
		return nil
	}
	return &mf.Locations[0]
}

type attachment struct {
	ID          int64
	UnitID      int64
	Filename    string
	Description string
	MD5         string `gorm:"column:md5"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type unit struct {
	ID                          int64
	OrderID                     int64
	MetadataID                  *int64
	Metadata                    *metadata `gorm:"foreignKey:MetadataID"`
	UnitStatus                  string
	IntendedUseID               int64
	IntendedUse                 intendedUse `gorm:"foreignKey:IntendedUseID"`
	RemoveWatermark             bool
	Reorder                     bool
	CommpleteScan               bool
	ThrowAway                   bool
	OCRMasterFiles              bool         `gorm:"column:ocr_master_files"`
	MasterFiles                 []masterFile `gorm:"foreignKey:UnitID"`
	MasterFilesCount            uint
	DateArchived                *time.Time
	DatePatronDeliverablesReady *time.Time
	DateDLDeliverablesReady     *time.Time `gorm:"column:date_dl_deliverables_ready"`
	UpdatedAt                   time.Time
}

type order struct {
	ID                             int64
	OrderStatus                    string
	CustomerID                     uint
	Customer                       customer `gorm:"foreignKey:CustomerID"`
	Fee                            sql.NullFloat64
	Invoices                       []invoice `gorm:"foreignKey:OrderID"`
	Units                          []unit    `gorm:"foreignKey:OrderID"`
	Email                          string
	DateRequestSubmitted           time.Time
	DateCustomerNotified           *time.Time
	DatePatronDeliverablesComplete *time.Time
	DateArchivingComplete          *time.Time
	DateFinalizationBegun          *time.Time
	DateFeeEstimateSentToCustomer  *time.Time
}
