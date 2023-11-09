package main

import (
	"flag"
	"log"
	"os"
)

// DBConfig wraps up all of the DB configuration
type DBConfig struct {
	Host string
	Port int
	User string
	Pass string
	Name string
}

// SMTPConfig wraps up all of the smpt configuration
type SMTPConfig struct {
	Host     string
	Port     int
	User     string
	Pass     string
	Sender   string
	FakeSMTP bool
}

// IIIFConfig contains the configuration data for the IIIF server
type IIIFConfig struct {
	Dir string
	URL string
}

// ArchivesSpaceConfig contains the configuration data for AS
type ArchivesSpaceConfig struct {
	User string
	Pass string
}

// HathiTrustConfig contains the configuration data for HathiTrust submissions
type HathiTrustConfig struct {
	FTPS         string
	User         string
	Pass         string
	RCloneRemote string
	RCloneBin    string
	RCloneConfig string
	RemoteDir    string
}

// TrackSysConfig contains the configuration data for tracksys endpoints
type TrackSysConfig struct {
	API   string
	Admin string
}

// APTrustConfig contains the cpmfiguration params for the APTrust S3 bucket
type APTrustConfig struct {
	AWSHost   string
	AWSBucket string
}

// ServiceConfig defines all of the JRML pool configuration parameters
type ServiceConfig struct {
	Port          int
	SMTP          SMTPConfig
	DB            DBConfig
	ArchiveDir    string
	IIIF          IIIFConfig
	ProcessingDir string
	DeliveryDir   string
	APTrust       APTrustConfig
	HathiTrust    HathiTrustConfig
	ArchivesSpace ArchivesSpaceConfig
	TrackSys      TrackSysConfig
	ReindexURL    string
	XMLReindexURL string
	OcrURL        string
	ServiceURL    string
}

// LoadConfiguration will load the service configuration from the commandline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {
	log.Printf("INFO: loading configuration...")
	var cfg ServiceConfig
	flag.IntVar(&cfg.Port, "port", 8080, "API service port (default 8080)")
	flag.StringVar(&cfg.ServiceURL, "service", "", "This service base URL")

	// working directories
	flag.StringVar(&cfg.ArchiveDir, "archive", "", "Archive directory")
	flag.StringVar(&cfg.DeliveryDir, "delivery", "", "Delivery directory")
	flag.StringVar(&cfg.ProcessingDir, "work", "", "Processing directory")

	// other external services
	flag.StringVar(&cfg.XMLReindexURL, "xmlreindex", "https://virgo4-image-tracksys-reprocess-ws.internal.lib.virginia.edu/api/reindex", "XML reindex webhook")
	flag.StringVar(&cfg.ReindexURL, "reindex", "https://virgo4-sirsi-cache-reprocess-ws.internal.lib.virginia.edu", "Reindex URL")
	flag.StringVar(&cfg.OcrURL, "ocr", "http://docker1.lib.virginia.edu:8389/ocr", "OCR service URL")

	// ArchivesSpace
	flag.StringVar(&cfg.ArchivesSpace.User, "asuser", "", "ArchivesSpace user")
	flag.StringVar(&cfg.ArchivesSpace.Pass, "aspass", "", "ArchivesSpace password")

	// HathiTrust FTPS
	flag.StringVar(&cfg.HathiTrust.FTPS, "htftps", "", "HathiTrust FTPS")
	flag.StringVar(&cfg.HathiTrust.User, "htuser", "", "HathiTrust user")
	flag.StringVar(&cfg.HathiTrust.Pass, "htpass", "", "HathiTrust pass")
	flag.StringVar(&cfg.HathiTrust.RCloneBin, "rcbin", "", "Path to rclone binary")
	flag.StringVar(&cfg.HathiTrust.RCloneConfig, "rccfg", "", "Path to rclone config")
	flag.StringVar(&cfg.HathiTrust.RCloneRemote, "rcremote", "hathitrust", "Name of the rclone remote")
	flag.StringVar(&cfg.HathiTrust.RemoteDir, "rcdir", "virginia", "Remote submission directory for HathiTrust")

	// TrackSys
	flag.StringVar(&cfg.TrackSys.API, "tsapi", "https://tracksys-api-ws.internal.lib.virginia.edu", "URL for TrackSys API")
	flag.StringVar(&cfg.TrackSys.Admin, "tsadmin", "https://tracksys.lib.virginia.edu/admin", "URL for TrackSys ADMIN interface")

	// APTrust
	flag.StringVar(&cfg.APTrust.AWSHost, "apthost", "s3.amazonaws.com", "APTrust S3 host")
	flag.StringVar(&cfg.APTrust.AWSBucket, "aptbucket", "", "APTrust S3 bucket")

	// IIIF
	flag.StringVar(&cfg.IIIF.Dir, "iiif", "", "IIIF directory")
	flag.StringVar(&cfg.IIIF.URL, "iiifman", "https://iiifman.lib.virginia.edu", "IIIF manifest URL")

	// SMTP
	flag.BoolVar(&cfg.SMTP.FakeSMTP, "stubsmtp", false, "Log email insted of sending (dev mode)")
	flag.StringVar(&cfg.SMTP.Host, "smtphost", "", "SMTP Host")
	flag.IntVar(&cfg.SMTP.Port, "smtpport", 0, "SMTP Port")
	flag.StringVar(&cfg.SMTP.User, "smtpuser", "", "SMTP User")
	flag.StringVar(&cfg.SMTP.Pass, "smtppass", "", "SMTP Password")
	flag.StringVar(&cfg.SMTP.Sender, "smtpsender", "digitalservices@virginia.edu", "SMTP sender email")

	// DB connection params
	flag.StringVar(&cfg.DB.Host, "dbhost", "", "Database host")
	flag.IntVar(&cfg.DB.Port, "dbport", 3306, "Database port")
	flag.StringVar(&cfg.DB.Name, "dbname", "", "Database name")
	flag.StringVar(&cfg.DB.User, "dbuser", "", "Database user")
	flag.StringVar(&cfg.DB.Pass, "dbpass", "", "Database password")

	flag.Parse()

	if cfg.DB.Host == "" {
		log.Fatal("Parameter dbhost is required")
	}
	if cfg.DB.Name == "" {
		log.Fatal("Parameter dbname is required")
	}
	if cfg.DB.User == "" {
		log.Fatal("Parameter dbuser is required")
	}
	if cfg.DB.Pass == "" {
		log.Fatal("Parameter dbpass is required")
	}
	if cfg.ArchiveDir == "" {
		log.Fatal("Parameter archive is required")
	}
	if cfg.DeliveryDir == "" {
		log.Fatal("Parameter delivery is required")
	}
	if cfg.IIIF.Dir == "" {
		log.Fatal("Parameter iiif is required")
	}
	if cfg.ProcessingDir == "" {
		log.Fatal("Parameter work is required")
	}
	if cfg.ServiceURL == "" {
		log.Fatal("Parameter service is required")
	}
	if cfg.ArchivesSpace.User == "" {
		log.Fatal("Parameter asuser is required")
	}
	if cfg.ArchivesSpace.Pass == "" {
		log.Fatal("Parameter aspass is required")
	}
	if cfg.HathiTrust.FTPS == "" {
		log.Fatal("Parameter htftps is required")
	}
	if cfg.HathiTrust.User == "" {
		log.Fatal("Parameter htuser is required")
	}
	if cfg.HathiTrust.Pass == "" {
		log.Fatal("Parameter htpass is required")
	}

	log.Printf("[CONFIG] port          = [%d]", cfg.Port)
	log.Printf("[CONFIG] service       = [%s]", cfg.ServiceURL)
	log.Printf("[CONFIG] dbhost        = [%s]", cfg.DB.Host)
	log.Printf("[CONFIG] dbport        = [%d]", cfg.DB.Port)
	log.Printf("[CONFIG] dbname        = [%s]", cfg.DB.Name)
	log.Printf("[CONFIG] dbuser        = [%s]", cfg.DB.User)
	log.Printf("[CONFIG] archive       = [%s]", cfg.ArchiveDir)
	log.Printf("[CONFIG] delivery      = [%s]", cfg.DeliveryDir)
	log.Printf("[CONFIG] iiif          = [%s]", cfg.IIIF.Dir)
	log.Printf("[CONFIG] iiifman       = [%s]", cfg.IIIF.URL)
	log.Printf("[CONFIG] work          = [%s]", cfg.ProcessingDir)
	log.Printf("[CONFIG] reindex       = [%s]", cfg.ReindexURL)
	log.Printf("[CONFIG] xmlreindex    = [%s]", cfg.XMLReindexURL)
	log.Printf("[CONFIG] ocr           = [%s]", cfg.OcrURL)
	log.Printf("[CONFIG] tsadmin       = [%s]", cfg.TrackSys.Admin)
	log.Printf("[CONFIG] tsapi         = [%s]", cfg.TrackSys.API)
	log.Printf("[CONFIG] asuser        = [%s]", cfg.ArchivesSpace.User)
	log.Printf("[CONFIG] htftps        = [%s]", cfg.HathiTrust.FTPS)
	log.Printf("[CONFIG] htuser        = [%s]", cfg.HathiTrust.User)
	log.Printf("[CONFIG] rcbin         = [%s]", cfg.HathiTrust.RCloneBin)
	log.Printf("[CONFIG] rccfg         = [%s]", cfg.HathiTrust.RCloneConfig)
	log.Printf("[CONFIG] rcremote      = [%s]", cfg.HathiTrust.RCloneRemote)
	log.Printf("[CONFIG] rcdir         = [%s]", cfg.HathiTrust.RemoteDir)
	log.Printf("[CONFIG] aptrust_url   = [%s]", os.Getenv("APTRUST_REGISTRY_URL"))
	log.Printf("[CONFIG] apthost       = [%s]", cfg.APTrust.AWSHost)
	log.Printf("[CONFIG] aptbucket     = [%s]", cfg.APTrust.AWSBucket)

	if cfg.SMTP.FakeSMTP {
		log.Printf("[CONFIG] fakesmtp      = [true]")
	} else {
		log.Printf("[CONFIG] smtphost      = [%s]", cfg.SMTP.Host)
		log.Printf("[CONFIG] smtpport      = [%d]", cfg.SMTP.Port)
		log.Printf("[CONFIG] smtpuser      = [%s]", cfg.SMTP.User)
		log.Printf("[CONFIG] smtppass      = [%s]", cfg.SMTP.Pass)
		log.Printf("[CONFIG] smtpsender    = [%s]", cfg.SMTP.Sender)
	}

	return &cfg
}
