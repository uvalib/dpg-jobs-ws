package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// this is a stripped down representation of mets metadata that contains only the
// fields needed to add issue metadata to the unit: date issued, volume, issue, edition
type mets struct {
	XMLName xml.Name `xml:"mets"`
	DmdSec  []struct {
		ID     string `xml:"ID,attr"`
		MdWrap struct {
			XMLData struct {
				Mods struct {
					RelatedItem struct {
						Text       string `xml:",chardata"`
						Type       string `xml:"type,attr"`
						Identifier struct {
							Text string `xml:",chardata"`
							Type string `xml:"type,attr"`
						} `xml:"identifier"`
						Part struct {
							Text   string `xml:",chardata"`
							Detail []struct {
								Text   string `xml:",chardata"`
								Type   string `xml:"type,attr"`
								Number string `xml:"number"`
							} `xml:"detail"`
						} `xml:"part"`
					} `xml:"relatedItem"`
					OriginInfo struct {
						DateIssued struct {
							Text string `xml:",chardata"`
						} `xml:"dateIssued"`
					} `xml:"originInfo"`
				} `xml:"mods"`
			} `xml:"xmlData"`
		} `xml:"mdWrap"`
	} `xml:"dmdSec"`
}

// setupTribuneQA will pull imges from the tribune volume info dpg_imaging and setup unit / project
// params:
//   - orderID: order units will be attached to
//   - directory: the base directory for the issues
//   - lccn: the lccn directory where iussues can be found
//   - year: pull all issues for this year
//   - month: get issues for this month (optional)
//   - day: get issues for this day (optional)
//
// EXAMPLE:
//
//	curl -X POST https://dpg-jobs.lib.virginia.edu/script -H "Content-Type: application/json" \
//		--data '{"computeID": "lf6f", "name": "tribuneSetup", "dev": true \
//		"params": {"orderID": 12826, \
//	   "directory": "/Users/lf6f/dev/tracksys-dev/sandbox/digiserv-production/tribune_data", \
//	   "lccn": "sn95079521", "year": "1950"}}'
func (svc *ServiceContext) setupTribuneQA(c *gin.Context, js *jobStatus, params map[string]any) error {
	// first grab orderID and ensure the order exists
	orderF, ok := params["orderID"].(float64)
	if !ok {
		return fmt.Errorf("invalid orderID param: %s", params["orderID"])
	}
	orderID := int64(orderF)
	var tribOrder order
	if err := svc.GDB.First(&tribOrder, orderID).Error; err != nil {
		return fmt.Errorf("get order %d failed: %s", orderID, err.Error())
	}

	// pull remaining params
	baseDir := fmt.Sprintf("%s", params["directory"])
	lccnDir := fmt.Sprintf("%s", params["lccn"])
	printPath := path.Join(baseDir, lccnDir, "print")
	year := fmt.Sprintf("%s", params["year"])
	tgtIssues := year
	if params["month"] != nil {
		tgtIssues += fmt.Sprintf("%s", params["month"])
		if params["day"] != nil {
			tgtIssues += fmt.Sprintf("%s", params["day"])
		}
	}

	// validate lccn dir and pick metadata
	var metadataID int64
	switch lccnDir {
	case "sn95079521":
		metadataID = 15419
	case "sn95079529":
		metadataID = 14119
	}
	if metadataID == 0 {
		return fmt.Errorf("invalid LCCN %s", lccnDir)
	}

	svc.logInfo(js, fmt.Sprintf("Setup tribune %s from directory %s into order %d using metadata %d", tgtIssues, printPath, orderID, metadataID))

	allIssues, err := os.ReadDir(printPath)
	if err != nil {
		return fmt.Errorf("unable to get %s issues", lccnDir)
	}

	issueDirs := slices.DeleteFunc(allIssues, func(entry os.DirEntry) bool {
		return strings.Index(entry.Name(), tgtIssues) != 0
	})

	if len(issueDirs) == 0 {
		return fmt.Errorf("no issues matching %s found in %s", tgtIssues, printPath)
	}

	for _, dir := range issueDirs {
		svc.logInfo(js, fmt.Sprintf("Process issue directory %s", dir.Name()))

		issueDir := path.Join(printPath, dir.Name())
		metsPath := path.Join(issueDir, fmt.Sprintf("%s.xml", dir.Name()))
		svc.logInfo(js, fmt.Sprintf("Extract metadata from %s", metsPath))
		xmlBytes, err := os.ReadFile(metsPath)
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to read mets file %s: %s", metsPath, err.Error()))
			continue
		}
		var parsedMETS mets
		if err := xml.Unmarshal(xmlBytes, &parsedMETS); err != nil {
			svc.logError(js, fmt.Sprintf("Unable to parse mets data from file %s: %s", metsPath, err.Error()))
			continue
		}

		volume := "unknown"
		issue := "unknown"
		edition := "unknown"
		dateIssued := "unknown"
		for _, dmdSec := range parsedMETS.DmdSec {
			if dmdSec.ID == "issueModsBib" {
				mods := dmdSec.MdWrap.XMLData.Mods
				dateIssued = mods.OriginInfo.DateIssued.Text
				for _, md := range mods.RelatedItem.Part.Detail {
					switch md.Type {
					case "volume":
						volume = md.Number
					case "issue":
						issue = md.Number
					case "edition":
						edition = md.Number
					}
				}
				break
			}
		}
		metsMetadata := fmt.Sprintf("Date Issued: %s, Volume: %s, Issue: %s, Edition: %s", dateIssued, volume, issue, edition)
		svc.logInfo(js, fmt.Sprintf("Extracted issue metadata [%s]", metsMetadata))

		svc.logInfo(js, fmt.Sprintf("Get or create unit for issue %s", dir.Name()))
		si := fmt.Sprintf("Ingest from %s %s", lccnDir, dir.Name())
		svc.logInfo(js, fmt.Sprintf("special instructions for issue unit: %s", si))
		var issueUnit unit
		if err := svc.GDB.Where("order_id=? and special_instructions=?", orderID, si).First(&issueUnit).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				svc.logInfo(js, fmt.Sprintf("Unit for %s does not exist; create one", dir.Name()))
				intendedUse := int64(110)
				issueUnit.CompleteScan = true
				issueUnit.IntendedUseID = &intendedUse
				issueUnit.SpecialInstructions = si
				issueUnit.MetadataID = &metadataID
				issueUnit.StaffNotes = metsMetadata
				issueUnit.OrderID = orderID
				issueUnit.UnitStatus = "approved"
				if err := svc.GDB.Create(&issueUnit).Error; err != nil {
					svc.logError(js, fmt.Sprintf("Create unit for %s failed: %s", dir.Name(), err.Error()))
					continue
				}
				svc.logInfo(js, fmt.Sprintf("Created new unit %d for %s", issueUnit.ID, dir.Name()))
			} else {
				svc.logError(js, fmt.Sprintf("Search for existing unit for %s failed: %s", dir.Name(), err.Error()))
				continue
			}
		} else {
			svc.logInfo(js, fmt.Sprintf("Use existing unit %d for %s", issueUnit.ID, dir.Name()))
			// TODO check if unit has master files already. if so, skip it
		}

		svc.logInfo(js, fmt.Sprintf("Get or create project for unit %d for %s", issueUnit.ID, dir.Name()))
		var proj project
		if err := svc.GDB.Where("unit_id=?", issueUnit.ID).First(&proj).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				svc.logInfo(js, fmt.Sprintf("Project for unit %d %s does not exist; create it", issueUnit.ID, dir.Name()))
				qaStepID := int64(62)
				boundID := int64(4)
				proj.AddedAt = time.Now()
				proj.UnitID = issueUnit.ID
				proj.WorkflowID = 7             // vendor
				proj.ItemCondition = 0          // 0=good, 1=bad
				proj.CategoryID = 5             // special
				proj.ContainerTypeID = &boundID // bound
				proj.CurrentStepID = &qaStepID
				if err := svc.GDB.Create(&proj).Error; err != nil {
					svc.logError(js, fmt.Sprintf("Create project for %s failed: %s", dir.Name(), err.Error()))
					continue
				}
			} else {
				svc.logError(js, fmt.Sprintf("Search for existing project for unit %d failed: %s", issueUnit.ID, err.Error()))
				continue
			}
		} else {
			if proj.StartedAt != nil || proj.FinishedAt != nil {
				svc.logInfo(js, fmt.Sprintf("Project %d exists for %s and it is in progress or finished. Skipping", proj.ID, dir.Name()))
				continue
			}
			svc.logInfo(js, fmt.Sprintf("Use existing project %d for %s", proj.ID, dir.Name()))
		}

		allFiles, err := os.ReadDir(issueDir)
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to list files in issue directory %s: %s", issueDir, err.Error()))
			continue
		}

		tifFiles := slices.DeleteFunc(allFiles, func(entry os.DirEntry) bool {
			return path.Ext(entry.Name()) != ".tif"
		})

		// create a unit directory in dpg_imaging to contain copies of the images for QA processing
		unitDir := path.Join(svc.ProcessingDir, "dpg_imaging", fmt.Sprintf("%09d", issueUnit.ID)) // process dir is digiserv-production
		if err := ensureDirExists(unitDir, 0775); err != nil {
			svc.logError(js, fmt.Sprintf("Unable to create qa directory %s: %s", unitDir, err.Error()))
			continue
		}

		svc.logInfo(js, fmt.Sprintf("move all page images in %s into %s", issueDir, unitDir))
		for seq, tif := range tifFiles {
			pageNum := seq + 1
			svc.logInfo(js, fmt.Sprintf("process page %d: %s", pageNum, tif.Name()))

			// 1 copy to unit directory in dpg_imaging following normal naming conventions
			origPath := path.Join(issueDir, tif.Name())
			destName := fmt.Sprintf("%09d_%04d.tif", issueUnit.ID, pageNum)
			destPath := path.Join(unitDir, destName)

			if pathExists(destPath) {
				svc.logInfo(js, fmt.Sprintf("%s exists. Skipping", destPath))
				continue
			}

			_, err := copyFile(origPath, destPath, 0664)
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to copy %s to %s: %s", origPath, destPath, err.Error()))
				continue
			}

			// 2 set original file name to EXIF header iptc:MasterDocumentID
			cmd := make([]string, 0)
			cmd = append(cmd, fmt.Sprintf("-iptc:MasterDocumentID=%s", tif.Name()))
			cmd = append(cmd, destPath)
			_, err = exec.Command("exiftool", cmd...).Output()
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to set original filename in exifheaders: %s", err.Error()))
			} else {
				// remove temp files generated by exiftool
				dupPath := fmt.Sprintf("%s_original", destPath)
				os.Remove(dupPath)
				dupPath = fmt.Sprintf("%s_exiftool_tmp", destPath)
				os.Remove(dupPath)
			}
		}
	}

	return nil
}
