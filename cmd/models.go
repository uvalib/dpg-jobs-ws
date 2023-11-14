package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"gorm.io/gorm"
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

type staffRole uint

// Staff roles from rails enum: [:admin, :supervisor, :student, :viewer]
const (
	Admin      staffRole = 0
	Supervisor staffRole = 1
	Student    staffRole = 2
	Viewer     staffRole = 3
)

type staffMember struct {
	ID          int64
	ComputingID string
	Email       string
	Role        staffRole
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

// apTrustSubmissionis a TrackSys DB record generated when a metadata record is submitted to APTrust
type apTrustSubmission struct {
	ID          int64      `json:"-"`
	MetadataID  int64      `gorm:"column:metadata_id" json:"-"`
	Bag         string     `json:"etag"`
	RequestedAt time.Time  `json:"requestedAt"`
	SubmittedAt *time.Time `json:"submittedAt"`
	ProcessedAt *time.Time `json:"processedAt"`
	Success     bool       `json:"success"`
}

type preservationTier struct {
	ID          uint   `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
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
	IsCollection         bool
	ParentMetadataID     int64
	AvailabilityPolicyID *int64
	OcrHintID            *int64
	OcrHint              *ocrHint `gorm:"foreignKey:OcrHintID"`
	OcrLanguageHint      string
	Locations            []location        `gorm:"foreignKey:MetadataID"`
	HathiTrustStatus     *hathitrustStatus `gorm:"foreignKey:MetadataID" json:"hathiTrustStatus,omitempty"`
	PreservationTierID   int64
	PreservationTier     *preservationTier  `gorm:"foreignKey:PreservationTierID" json:"preservationTier"`
	APTrustSubmission    *apTrustSubmission `gorm:"foreignKey:MetadataID" json:"apTrustSubmission,omitempty"`
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
	MD5               string `gorm:"column:md5"`
	Sensitive         bool
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

func (mf *masterFile) AfterCreate(tx *gorm.DB) (err error) {
	return tx.Model(mf).Update("pid", fmt.Sprintf("tsm:%d", mf.ID)).Error
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
	SpecialInstructions         string
	DateArchived                *time.Time
	DatePatronDeliverablesReady *time.Time
	DateDLDeliverablesReady     *time.Time `gorm:"column:date_dl_deliverables_ready"`
	UpdatedAt                   time.Time
}

type order struct {
	ID                             int64
	OrderTitle                     string
	OrderStatus                    string
	CustomerID                     uint
	Customer                       customer `gorm:"foreignKey:CustomerID"`
	Fee                            *float64
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
