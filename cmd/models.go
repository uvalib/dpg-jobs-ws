package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type academicStatus struct {
	ID   int64
	Name string
}

type ocrHint struct {
	ID           int64
	Name         string
	OcrCandidate bool
}

type componentType struct {
	ID         int64
	Name       string
	Descrition string
}

type component struct {
	ID              int64
	Title           string
	ContentDesc     string
	Label           string
	Date            string
	Level           string
	ComponentTypeID int64
	ComponentType   componentType `gorm:"foreignKey:ComponentTypeID"`
}

func (c *component) Type() string {
	return strings.ToTitle(string(c.ComponentType.Name[0])) + c.ComponentType.Name[1:]
}
func (c *component) Name() string {
	// At this time there is no definitive field that can be used for "naming" purposes.
	// There are several candidates (title, content_desc, label) and until we make
	// a definitive choice, we must rely upon an aritifical method to provide the string.
	// Given the inconsistencies of input data, all newlines and sequences of two or more spaces
	// will be substituted.
	name := ""
	if c.Title != "" {
		name = c.Title
	} else if c.ContentDesc != "" {
		name = c.ContentDesc
	} else if c.Label != "" {
		name = c.Label
	} else if c.Date != "" {
		name = c.Date
	} else {
		name = fmt.Sprintf("%d", c.ID) // Everything has an id, so it is the LCD.
	}
	m := regexp.MustCompile("\\s+")
	name = strings.ReplaceAll(name, "\n", " ")
	name = m.ReplaceAllString(name, " ")
	return name
}

type intendedUse struct {
	ID                    int64
	DeliverableFormat     string
	DeliverableResolution string
}

type staffMember struct {
	ID          int64
	ComputingID string
	Email       string
	Role        uint // [:admin, :supervisor, :student, :viewer]
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
	ID                   int64
	PID                  string `gorm:"column:pid"`
	Type                 string
	Title                string
	CreatorName          string
	CatalogKey           string
	CallNumber           string
	Barcode              string
	DescMetadata         string
	IsPersonalItem       bool
	IsManuscript         bool
	AvailabilityPolicyID *int64
	OcrHintID            *int64
	OcrHint              *ocrHint `gorm:"foreignKey:OcrHintID"`
	OcrLanguageHint      string
	PreservationTierID   int64
	CollectionID         string
	DateDlIngest         *time.Time      `gorm:"column:date_dl_ingest"`
	DateDlUpdate         *time.Time      `gorm:"column:date_dl_update"`
	ExternalSystemID     *int64          `gorm:"column:external_system_id"`
	ExternalSystem       *externalSystem `gorm:"foreignKey:ExternalSystemID"`
	ExternalURI          string          `gorm:"column:external_uri"`
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type containerType struct {
	ID         int64
	Name       string
	hasFolders bool
}

type location struct {
	ID              int64
	MetadataID      int64 `gorm:"column:metadata_id"`
	ContainerTypeID int64
	ContainerType   containerType `gorm:"foreignKey:ContainerTypeID"`
	ContainerID     string        `gorm:"column:container_id"`
	FolderID        string        `gorm:"column:folder_id"`
	Notes           string
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
	Component         *component    `gorm:"foreignKey:ComponentID"`
	ImageTechMeta     imageTechMeta `gorm:"foreignKey:MasterFileID"`
	UnitID            int64         `gorm:"column:unit_id"`
	Unit              unit          `gorm:"foreignKey:UnitID"`
	Filename          string
	Title             string
	Description       string
	Locations         []location `gorm:"many2many:master_file_locations"`
	Filesize          int64
	MD5               string  `gorm:"column:md5"`
	PHash             *uint64 `gorm:"column:phash" json:"-"`
	OriginalMfID      *int64  `gorm:"column:original_mf_id"`
	DateArchived      *time.Time
	DeaccessionedAt   *time.Time
	DeaccessionedByID *int64 `gorm:"column:deaccessioned_by_id"`
	DeaccessionNote   string
	TranscriptionText string
	DateDlIngest      *time.Time `gorm:"column:date_dl_ingest"`
	DateDlUpdate      *time.Time `gorm:"column:date_dl_update"`
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
	Order                       order `gorm:"foreignKey:OrderID"`
	MetadataID                  *int64
	Metadata                    *metadata `gorm:"foreignKey:MetadataID"`
	UnitStatus                  string
	IntendedUseID               *int64
	IntendedUse                 *intendedUse `gorm:"foreignKey:IntendedUseID"`
	IncludeInDL                 bool         `gorm:"column:include_in_dl"`
	RemoveWatermark             bool
	Reorder                     bool
	CompleteScan                bool
	ThrowAway                   bool
	OcrMasterFiles              bool         `gorm:"column:ocr_master_files"`
	MasterFiles                 []masterFile `gorm:"foreignKey:UnitID"`
	StaffNotes                  string
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
	FeeWaived                      bool
	Invoices                       []invoice `gorm:"foreignKey:OrderID"`
	Units                          []unit    `gorm:"foreignKey:OrderID"`
	Email                          string
	StaffNotes                     string
	DateRequestSubmitted           time.Time
	DateOrderApproved              *time.Time
	DateCustomerNotified           *time.Time
	DatePatronDeliverablesComplete *time.Time
	DateArchivingComplete          *time.Time
	DateFinalizationBegun          *time.Time
	DateFeeEstimateSentToCustomer  *time.Time
	UpdatedAt                      time.Time
}
