package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type tifMetadata struct {
	Title       string
	Description string
	ComponentID int64
}

func (svc *ServiceContext) importImages(js *jobStatus, tgtUnit *unit, srcDir string) error {
	svc.logInfo(js, fmt.Sprintf("Import images from %s", srcDir))
	if tgtUnit.ThrowAway {
		svc.logInfo(js, "This unit is a throw away and will not be archived.")
	}

	assembleDir := ""
	callNumber := ""
	location := ""
	if tgtUnit.Reorder == false && tgtUnit.IntendedUse.ID != 110 {
		svc.logInfo(js, "This unit requires patron deliverables. Setting up working directories.")
		assembleDir = path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", tgtUnit.ID))
		err := ensureDirExists(assembleDir, 0755)
		if err != nil {
			return err
		}
		svc.logInfo(js, fmt.Sprintf("Deliverables will be generated in %s", assembleDir))
		if tgtUnit.Metadata.Type == "SirsiMetadata" {
			callNumber = tgtUnit.Metadata.CallNumber
			location = svc.getMarcLocation(tgtUnit.Metadata)
			svc.logInfo(js, fmt.Sprintf("Metadata for Sirsi deliverables: call number: %s, location: %s", callNumber, location))
		}
	}

	// iterate through all of the .tif files in the unit directory
	mfCount := 0
	tifFiles, err := getTifFiles(srcDir, tgtUnit.ID)
	if err != nil {
		return err
	}
	for _, fi := range tifFiles {
		svc.logInfo(js, fmt.Sprintf("Import %s", fi.path))
		mfCount++

		// See if this masterfile has already been created...
		var newMF masterFile
		err = svc.GDB.Where("filename=?", fi.filename).Limit(1).Find(&newMF).Error
		if err != nil {
			return err
		}
		if newMF.ID == 0 {
			svc.logInfo(js, fmt.Sprintf("Create new master file %s", fi.filename))
			newMD5 := md5Checksum(fi.path)
			tifMD, err := extractTifMetadata(fi.path)
			if err != nil {
				return err
			}
			newMF = masterFile{UnitID: tgtUnit.ID, MetadataID: tgtUnit.MetadataID, Filename: fi.filename,
				Filesize: fi.size, MD5: newMD5, Title: tifMD.Title, Description: tifMD.Description}
			if tgtUnit.Metadata.IsManuscript && tifMD.ComponentID != 0 {
				var cnt int64
				svc.GDB.Table("components").Where("id=?", tifMD.ComponentID).Count(&cnt)
				if cnt == 0 {
					svc.logError(js, fmt.Sprintf("Could not find component %d to link to master file %s", tifMD.ComponentID, fi.filename))
				} else {
					svc.logInfo(js, fmt.Sprintf("Link to master file %s to component %d", fi.filename, tifMD.ComponentID))
					newMF.ComponentID = &tifMD.ComponentID
				}
			}

			err = svc.GDB.Create(&newMF).Error
			if err != nil {
				return err
			}
			newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
			svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
			svc.logInfo(js, fmt.Sprintf("Master file %s created", fi.filename))
		} else {
			svc.logInfo(js, fmt.Sprintf("Master file %s already exists", fi.filename))
			if newMF.PID == "" {
				newMF.PID = fmt.Sprintf("tsm:%d", newMF.ID)
				svc.GDB.Model(&newMF).Select("PID").Updates(newMF)
			}
		}

		if newMF.ImageTechMeta.ID == 0 {
			svc.logInfo(js, "Create image tech metadata")
			err = svc.createImageTechMetadata(&newMF, fi.path)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to create image tech metadata: %s", err.Error()))
			} else {
				svc.logInfo(js, fmt.Sprintf("%+v", newMF.ImageTechMeta))
			}
		}

		if tgtUnit.Reorder == false {
			err = svc.publishToIIIF(js, &newMF, fi.path, false)
			if err != nil {
				return fmt.Errorf("IIIF publish failed: %s", err.Error())
			}

			if tgtUnit.ThrowAway == false && tgtUnit.DateArchived == nil {
				archiveMD5, err := svc.archiveFile(js, fi.path, tgtUnit.ID, fi.filename)
				if err != nil {
					return fmt.Errorf("Archive failed: %s", err.Error())
				}
				if archiveMD5 != newMF.MD5 {
					svc.logError(js, "Archive MD5 does not match source MD5")
				}
			}

			if tgtUnit.IntendedUse.ID != 110 && tgtUnit.IntendedUse.DeliverableFormat != "pdf" {
				err = svc.createPatronDeliverable(js, tgtUnit, &newMF, fi.path, assembleDir, callNumber, location)
				if err != nil {
					return fmt.Errorf("Create patron deliverable failed: %s", err.Error())
				}
			}

			// check for transcription text file
			baseFN := strings.TrimSuffix(newMF.Filename, filepath.Ext(newMF.Filename))
			textFilePath := path.Join(srcDir, fmt.Sprintf("%s.txt", baseFN))
			if pathExists(textFilePath) {
				svc.logInfo(js, fmt.Sprintf("Add transcription text for %s", fi.filename))
				bytes, err := ioutil.ReadFile(textFilePath)
				if err != nil {
					svc.logError(js, fmt.Sprintf("Unable to read txt file %s: %s", textFilePath, err.Error()))
				} else {
					newMF.TranscriptionText = string(bytes)
					svc.GDB.Model(&newMF).Select("TranscriptionText").Updates(newMF)
				}
			}

			// FIXME!!
			// // mf_path is the full path to the image. Strip off the base
			// // in_process dir. The remaining bit will be the subdirectory or nothing.
			// // use this info to know if there is box/folder info encoded in the filename
			// subdir_str = File.dirname( mf_path )[unit_path.length+1..-1]
			// if !subdir_str.blank? && master_file.location.nil? && !unit.project.nil?
			//    # subdir structure: [box|oversize|tray].{box_name}/{folder_name}
			//    logger.info "Creating location metadata based on subdirs [#{subdir_str}]"
			//    if unit.project.container_type.nil?
			//       unit.project.container_type = ContainerType.first
			//       unit.project.save!
			//       logger.warn "Location data available, but container type not set. Defaulting to #{unit.project.container_type.name}"
			//    end
			//    location = Location.find_or_create(unit.metadata, unit.project.container_type, unit_path, subdir_str)
			//    master_file.set_location(location)
			//    logger.info "Created location metadata for [#{subdir_str}]"
			// end
		}
	}

	svc.logInfo(js, fmt.Sprintf("%d master files ingested", mfCount))
	now := time.Now()
	tgtUnit.UnitExtentActual = uint(mfCount)
	tgtUnit.MasterFilesCount = uint(mfCount)
	tgtUnit.DateArchived = &now
	svc.GDB.Model(tgtUnit).Select("UnitExtentActual", "MasterFilesCount", "DateArchived").Updates(*tgtUnit)
	svc.checkOrderArchiveComplete(js, tgtUnit.OrderID)

	svc.logInfo(js, "Images for Unit successfully imported.")
	return nil
}

func extractTifMetadata(tifPath string) (*tifMetadata, error) {
	cmdArray := []string{"-json", "-iptc:OwnerID", "-iptc:headline", "-iptc:caption-abstract", tifPath}
	stdout, err := exec.Command("exiftool", cmdArray...).Output()
	if err != nil {
		return nil, err
	}

	type exifData struct {
		Title       interface{} `json:"Headline"`
		Description interface{} `json:"Caption-Abstract"`
		OwnerID     interface{} `json:"OwnerID"`
	}

	var parsedExif []exifData
	err = json.Unmarshal(stdout, &parsedExif)
	if err != nil {
		return nil, err
	}
	out := tifMetadata{}
	if parsedExif[0].OwnerID != nil {
		strID := fmt.Sprintf("%v", parsedExif[0].OwnerID)
		out.ComponentID, _ = strconv.ParseInt(strID, 10, 64)
	}
	if parsedExif[0].Title != nil {
		out.Title = fmt.Sprintf("%v", parsedExif[0].Title)
	} else {
		return nil, fmt.Errorf("missing required Headline in tif metadata for %s", tifPath)
	}
	if parsedExif[0].Description != nil {
		out.Description = fmt.Sprintf("%v", parsedExif[0].Description)
	}
	return &out, nil
}
