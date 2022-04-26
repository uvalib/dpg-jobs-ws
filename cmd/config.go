package main

import (
	"flag"
	"log"
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

// TrackSysConfig contains the configuration data for tracksys endpoints
type TrackSysConfig struct {
	API   string
	Admin string
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
	TrackSys      TrackSysConfig
	ReindexURL    string
}

// LoadConfiguration will load the service configuration from the commandline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {
	log.Printf("INFO: loading configuration...")
	var cfg ServiceConfig
	flag.IntVar(&cfg.Port, "port", 8080, "API service port (default 8080)")

	flag.StringVar(&cfg.ArchiveDir, "archive", "", "Archive directory")
	flag.StringVar(&cfg.DeliveryDir, "delivery", "", "Delivery directory")
	flag.StringVar(&cfg.ProcessingDir, "work", "", "Processing directory")
	flag.StringVar(&cfg.ReindexURL, "reindex", "https://virgo4-sirsi-cache-reprocess-ws.internal.lib.virginia.edu", "Reindex URL")

	// TrackSys
	flag.StringVar(&cfg.TrackSys.API, "tsapi", "https://tracksys-api-ws.internal.lib.virginia.edu", "URL for TrackSys API")
	flag.StringVar(&cfg.TrackSys.Admin, "tsadmin", "https://tracksys.lib.virginia.edu/admin", "URL for TrackSys ADMIN interface")

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
		log.Fatal("Parameter iif is required")
	}
	if cfg.ProcessingDir == "" {
		log.Fatal("Parameter work is required")
	}

	log.Printf("[CONFIG] port          = [%d]", cfg.Port)
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
	log.Printf("[CONFIG] tsadmin       = [%s]", cfg.TrackSys.Admin)
	log.Printf("[CONFIG] tsapi         = [%s]", cfg.TrackSys.API)

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
