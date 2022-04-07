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

// DevConfig specifies configuration params specific to development mode
type DevConfig struct {
	AuthUser string
	FakeSMTP bool
}

// ServiceConfig defines all of the JRML pool configuration parameters
type ServiceConfig struct {
	Port          int
	DevConfig     bool
	DB            DBConfig
	ArchiveDir    string
	IIIFDir       string
	ProcessingDir string
	DeliveryDir   string
	TrackSysURL   string
}

// LoadConfiguration will load the service configuration from the commandline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {
	log.Printf("INFO: loading configuration...")
	var cfg ServiceConfig
	flag.IntVar(&cfg.Port, "port", 8080, "API service port (default 8080)")

	flag.StringVar(&cfg.ArchiveDir, "archive", "", "Archive directory")
	flag.StringVar(&cfg.DeliveryDir, "delivery", "", "Delivery directory")
	flag.StringVar(&cfg.IIIFDir, "iiif", "", "IIIF directory")
	flag.StringVar(&cfg.ProcessingDir, "work", "", "Processing directory")
	flag.StringVar(&cfg.TrackSysURL, "tsapi", "https://tracksys-api-ws.internal.lib.virginia.edu", "URL for TrackSys API")

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
	if cfg.IIIFDir == "" {
		log.Fatal("Parameter iif is required")
	}
	if cfg.ProcessingDir == "" {
		log.Fatal("Parameter work is required")
	}

	log.Printf("[CONFIG] port          = [%d]", cfg.Port)
	log.Printf("[CONFIG] tsapi         = [%s]", cfg.TrackSysURL)
	log.Printf("[CONFIG] dbhost        = [%s]", cfg.DB.Host)
	log.Printf("[CONFIG] dbport        = [%d]", cfg.DB.Port)
	log.Printf("[CONFIG] dbname        = [%s]", cfg.DB.Name)
	log.Printf("[CONFIG] dbuser        = [%s]", cfg.DB.User)
	log.Printf("[CONFIG] archive       = [%s]", cfg.ArchiveDir)
	log.Printf("[CONFIG] delivery      = [%s]", cfg.DeliveryDir)
	log.Printf("[CONFIG] iiif          = [%s]", cfg.IIIFDir)
	log.Printf("[CONFIG] work          = [%s]", cfg.ProcessingDir)

	return &cfg
}
