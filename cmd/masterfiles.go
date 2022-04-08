package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) addMasterFiles(c *gin.Context) {
	unitID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("AddMasterFiles", "Unit", unitID)
	if err != nil {
		log.Printf("ERROR: unable to create AddMasterFiles job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, "Load unit and masterfiles")
	var tgtUnit unit
	err = svc.GDB.Preload("MasterFiles").
		Preload("MasterFiles.ImageTechMeta").
		Preload("MasterFiles.Locations").
		Preload("MasterFiles.Locations.ContainerType").
		First(&tgtUnit, unitID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	unitDir := padLeft(c.Param("id"), 9)
	srcDir := path.Join(svc.ProcessingDir, "finalization", "unit_update", unitDir)
	svc.logInfo(js, fmt.Sprintf("Looking for new *.tif files in %s", srcDir))
	files, err := ioutil.ReadDir(srcDir)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to read %s: %s", srcDir, err.Error()))
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	type tifInfo struct {
		filename string
		path     string
		size     int64
	}
	tifFiles := make([]tifInfo, 0)
	newPage := -1
	prevPage := -1
	mfRegex := regexp.MustCompile(fmt.Sprintf(`^%s_\w{4,}\.tif$`, unitDir))
	for _, fi := range files {
		fName := fi.Name()
		if strings.Index(fName, ".tif") > -1 {
			if !mfRegex.Match([]byte(fName)) {
				svc.logFatal(js, fmt.Sprintf("Invalid master file name: %s", fName))
				c.String(http.StatusBadRequest, fmt.Sprintf("Invalid master file name: %s", fName))
				return
			}

			pageNum := getMasterFileNumber(fName)
			if newPage == -1 {
				newPage = pageNum
				prevPage = newPage
			} else {
				if pageNum > prevPage+1 {
					svc.logFatal(js, "Gap in sequence number of new master files")
					c.String(http.StatusBadRequest, "gap in sequence")
					return
				}
				prevPage = pageNum
			}
			tifFiles = append(tifFiles, tifInfo{path: path.Join(srcDir, fName), filename: fName, size: fi.Size()})
		}
	}

	if len(tifFiles) == 0 {
		svc.logFatal(js, "No tif files found")
		c.String(http.StatusBadRequest, "no files")
		return
	}

	lastPageNum := getMasterFileNumber(tgtUnit.MasterFiles[len(tgtUnit.MasterFiles)-1].Filename)
	if newPage > lastPageNum+1 {
		svc.logFatal(js, fmt.Sprintf("New master file sequence number gap (from %d to %d)", lastPageNum, newPage))
		c.String(http.StatusBadRequest, "gap in sequence")
		return
	}

	if newPage <= lastPageNum {
		// rename/rearchive files to make room for new files to be inserted
		// If components are involved, return the ID of the component at the insertion point
		// TODO
		//make_gap_for_insertion(unit, archive_dir, tif_files)
	}

	// grab the first existing master file and see if it has location data.
	// if it does, the cotainer type for all will be the same. pull it
	var containerType *containerType
	var componentID *int64
	if len(tgtUnit.MasterFiles) > 0 {
		if tgtUnit.MasterFiles[0].location() != nil {
			containerType = &tgtUnit.MasterFiles[0].location().ContainerType
		}
		componentID = tgtUnit.MasterFiles[0].ComponentID
	}

	// Create new master files for the tif file found in the src dir
	svc.logInfo(js, fmt.Sprintf("Adding %d new master files...", len(tifFiles)))
	for _, tf := range tifFiles {
		// create MF and tech metadata
		md5 := md5Checksum(tf.path)
		pgNum := getMasterFileNumber(tf.filename)
		newMF := masterFile{Filename: tf.filename, Title: fmt.Sprintf("%d", pgNum), Filesize: tf.size,
			MD5: md5, UnitID: tgtUnit.ID, ComponentID: componentID, MetadataID: tgtUnit.MetadataID}
		log.Printf("%+v", newMF)

		if containerType != nil {
			// use it!
		}
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}
