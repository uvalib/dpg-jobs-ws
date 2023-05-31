package main

import (
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"log"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"github.com/corona10/goimagehash"
	"github.com/gin-gonic/gin"
	"golang.org/x/image/tiff"
	"gorm.io/gorm"
)

type phashGenerateStats struct {
	StartedAt           string
	FinishedAt          string
	Year                string
	MasterFileCount     int64
	MissingArchiveCount int64
	FailedPHashCount    int64
	DBErrorCount        int64
	SuccessCount        int64
}

type phashRequest struct {
	Email string `json:"email"`
	Year  string `json:"year"`
	Limit int    `json:"limit"`
}

func (svc *ServiceContext) generateMasterFilesPHash(c *gin.Context) {
	log.Printf("INFO: received phash generation request")
	var req phashRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse phash request: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.Email == "" {
		log.Printf("ERROR: email is required for phash request")
		c.String(http.StatusBadRequest, "missing emiil")
		return
	}
	if req.Year == "" {
		log.Printf("ERROR: year is required for phash request")
		c.String(http.StatusBadRequest, "missing year")
		return
	}

	log.Printf("INFO: %s requests pHash generation for year %s with limit %d", req.Email, req.Year, req.Limit)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered during phash generation: %v", r)
				debug.PrintStack()
			}
		}()

		var masterFiles []masterFile
		batchSize := 1000
		stats := phashGenerateStats{StartedAt: time.Now().Format("2006-01-02 03:04:05 PM"), Year: req.Year}
		pHashQ := svc.GDB.Where("phash is null and year(created_at) = ? and date_archived is not null and original_mf_id is null", req.Year)
		if req.Limit > 0 {
			pHashQ = pHashQ.Limit(req.Limit)
		}
		err := pHashQ.FindInBatches(&masterFiles, batchSize, func(tx *gorm.DB, batch int) error {
			log.Printf("INFO: generate phash for batch %d of master files; total processed: %d", batch, stats.MasterFileCount)
			for _, mf := range masterFiles {
				stats.MasterFileCount++
				archiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", mf.UnitID), mf.Filename)
				if strings.Contains(mf.Filename, "ARCH") || strings.Contains(mf.Filename, "AVRN") || strings.Contains(mf.Filename, "VRC") {
					if strings.Contains(mf.Filename, "_") {
						overrideDir := strings.Split(mf.Filename, "_")[0]
						archiveFile = path.Join(svc.ArchiveDir, overrideDir, mf.Filename)
					}
				}

				if pathExists(archiveFile) == false {
					log.Printf("ERROR: archive %s not found for phash", archiveFile)
					stats.MissingArchiveCount++
					continue
				}

				pHash, err := calculatePHash(archiveFile)
				if err != nil {
					log.Printf("ERROR: %s", err.Error())
					stats.FailedPHashCount++
					continue
				}

				mf.PHash = &pHash
				err = svc.GDB.Model(&mf).Select("PHash").Updates(mf).Error
				if err != nil {
					stats.DBErrorCount++
					log.Printf("ERROR: unable to update phash for %s: %s", mf.PID, err.Error())
					continue
				}

				stats.SuccessCount++
			}

			// return error will stop future batches
			return nil
		}).Error

		if err != nil {
			log.Printf("ERROR: unable to get master file bach for phash generation: %s", err.Error())
			stats.DBErrorCount++
		} else {
			stats.FinishedAt = time.Now().Format("2006-01-02 03:04:05 PM")
			log.Printf("INFO: phash generation complete; statistics %+v", stats)
		}
		svc.sendPHashResultsEmail(req.Email, stats)
	}()

	c.String(http.StatusOK, "generation started")
}

func calculatePHash(filePath string) (uint64, error) {
	imgFile, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("unable to open %s for phash generation: %s", filePath, err.Error())
	}

	defer imgFile.Close()
	fileType := strings.ToUpper(path.Ext(filePath))
	fileType = strings.Replace(fileType, ".", "", 1)
	var imgData image.Image

	if fileType == "TIF" {
		imgData, err = tiff.Decode(imgFile)
	} else if fileType == "JPG" {
		imgData, err = jpeg.Decode(imgFile)
	} else if fileType == "PNG" {
		imgData, err = png.Decode(imgFile)
	} else if fileType == "GIF" {
		imgData, err = gif.Decode(imgFile)
	}
	if err != nil {
		return 0, fmt.Errorf("unable to decode %s for phash generation: %s", filePath, err.Error())
	}

	imgHash, err := goimagehash.DifferenceHash(imgData)
	if err != nil {
		return 0, fmt.Errorf("unable to calculate phash for %s: %s", filePath, err.Error())
	}

	pHash := imgHash.GetHash()
	return pHash, nil
}
