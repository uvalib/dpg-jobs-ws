package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) bagCreateRequested(c *gin.Context) {
	mdID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	js, err := svc.createJobStatus("Baggit", "Metadata", mdID)
	if err != nil {
		log.Printf("ERROR: unable to create Baggit job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var md metadata
	err = svc.GDB.Find(&md, mdID).Error
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to get metadata %d", mdID))
		return
	}

	if md.PreservationTierID < 2 {
		svc.logFatal(js, "Preservation Tier must be set and greater than 1")
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ERROR: Panic recovered: %v", r)
				debug.PrintStack()
				svc.logFatal(js, fmt.Sprintf("Panic recovered while generating bag: %v", r))
			}
		}()

		tarFile, err := svc.createBag(js, &md)
		if err != nil {
			svc.logFatal(js, fmt.Sprintf("Baggit failed: %s", err.Error()))
		} else {
			svc.logInfo(js, fmt.Sprintf("Baggit tar file created here: %s", tarFile))
			svc.jobDone(js)
		}
	}()

	c.String(http.StatusOK, fmt.Sprintf("%d", js.ID))
}

func getBagDirectoryName(md *metadata) string {
	return fmt.Sprintf("virginia.edu.tracksys-%s-%d", strings.ToLower(md.Type), md.ID)
}
func getBagFileName(md *metadata) string {
	return fmt.Sprintf("%s.tar", getBagDirectoryName(md))
}

func (svc *ServiceContext) createBag(js *jobStatus, md *metadata) (string, error) {
	svc.logInfo(js, fmt.Sprintf("Create bag for metadata %s", md.PID))
	access := "Consortia"
	storage := "Standard"
	if md.PreservationTierID == 2 {
		storage = "Glacier-VA"
	}
	svc.logInfo(js, fmt.Sprintf("Create new bag flagged for %s storage", storage))
	bagDirName := getBagDirectoryName(md)
	bagBaseDir := path.Join(svc.ProcessingDir, "bags")
	bagAssembleDir := path.Join(bagBaseDir, bagDirName)
	if pathExists(bagAssembleDir) {
		svc.logInfo(js, fmt.Sprintf("Clean up pre-existing bag assembly directory %s", bagAssembleDir))
		err := os.RemoveAll(bagAssembleDir)
		if err != nil {
			return "", fmt.Errorf("unable to cleanup prior bag %s: %s", bagAssembleDir, err.Error())
		}
	}
	dataDir := path.Join(bagAssembleDir, "data")
	svc.logInfo(js, fmt.Sprintf("Create bag data directory %s", dataDir))
	err := ensureDirExists(dataDir, 0777)
	if err != nil {
		return "", fmt.Errorf("unable to create bag dir %s: %s", dataDir, err.Error())
	}

	destTar := path.Join(bagBaseDir, getBagFileName(md))
	if pathExists(destTar) {
		svc.logInfo(js, fmt.Sprintf("Clean up pre-existing bag %s", destTar))
		err := os.Remove(destTar)
		if err != nil {
			return "", fmt.Errorf("unable to cleanup prior bag %s: %s", destTar, err.Error())
		}
	}

	// Add the aptrust-info.txt, bag-info.txt, bagit.txt. NOTE: bagit.txt is not a tag file
	svc.logInfo(js, "Adding baggit.txt")
	bagit := []byte("BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8")
	err = os.WriteFile(path.Join(bagAssembleDir, "bagit.txt"), bagit, 0744)
	if err != nil {
		return "", fmt.Errorf("unable to create bagit.txt: %s", err.Error())
	}

	svc.logInfo(js, "Adding aptrust-info.txt")
	apt := []byte(fmt.Sprintf("Title: %s\nDescription: \nAccess: %s\nStorage-Option: %s", md.Title, access, storage))
	err = os.WriteFile(path.Join(bagAssembleDir, "aptrust-info.txt"), apt, 0744)
	if err != nil {
		return "", fmt.Errorf("unable to create aptrust-info.txt: %s", err.Error())
	}

	svc.logInfo(js, "Adding bag-info.txt")
	timeNow := time.Now()
	info := fmt.Sprintf("Source-Organization: virginia.edu\nBagging-Date: %s\nBag-Count: 1 of 1\n", timeNow.Format("2006-01-02"))
	info += fmt.Sprintf("Internal-Sender-Description: \nInternal-Sender-Identifier: %s\nBag-Group-Identifier: %s", md.PID, md.CollectionID)
	err = os.WriteFile(path.Join(bagAssembleDir, "bag-info.txt"), []byte(info), 0744)
	if err != nil {
		return "", fmt.Errorf("unable to create bag-info.txt: %s", err.Error())
	}

	svc.logInfo(js, "Add XML metadata")
	var bagFiles []string
	mods, err := svc.getModsMetadata(md)
	if err != nil {
		return "", fmt.Errorf("unable to get mods metadata: %s", err.Error())
	}
	err = os.WriteFile(path.Join(dataDir, fmt.Sprintf("%s.xml", md.PID)), mods, 0744)
	if err != nil {
		return "", fmt.Errorf("unable to create %s.xml: %s", md.PID, err.Error())
	}
	bagFiles = append(bagFiles, fmt.Sprintf("%s.xml", md.PID))

	// first try the normal case: units with matching metadata (images in a collection, book-like items)
	svc.logInfo(js, "Adding master files to bag")
	var masterFiles []masterFile
	err = svc.GDB.Joins("Unit").Where("Unit.metadata_id=? and Unit.intended_use_id=?", md.ID, 110).Find(&masterFiles).Error
	if err != nil {
		return "", fmt.Errorf("unable to get master files: %s", err.Error())
	}
	if len(masterFiles) == 0 {
		// if that fails, see if this is a the special case where an image is assigned different metadata than the unit.
		// this is the case for individual images described by XML metadata that are generaly part of a larger collection
		svc.logInfo(js, fmt.Sprintf("no units directly found for metadata %d; searching master files...", md.ID))
		err = svc.GDB.Joins("Unit").Where("Unit.intended_use_id=?", 110).Where("master_files.metadata_id=?", md.ID).Find(&masterFiles).Error
		if err != nil {
			return "", fmt.Errorf("unable to get master files: %s", err.Error())
		}
		if len(masterFiles) == 0 {
			return "", fmt.Errorf("no masterfiles qualify for APTrust (intended use 110)")
		}
	}

	svc.logInfo(js, fmt.Sprintf("%d masterfiles found", len(masterFiles)))
	for _, mf := range masterFiles {
		svc.logInfo(js, fmt.Sprintf("Adding masterfile %s to bag", mf.Filename))
		archiveFile := path.Join(svc.ArchiveDir, fmt.Sprintf("%09d", mf.UnitID), mf.Filename)
		destFile := path.Join(dataDir, mf.Filename)
		if pathExists(archiveFile) == false {
			return "", fmt.Errorf("%s not found", archiveFile)
		}
		origMD5 := md5Checksum(archiveFile)
		md5, err := copyFile(archiveFile, destFile, 0744)
		if err != nil {
			return "", fmt.Errorf("copy %s to %s failed: %s", archiveFile, destFile, err.Error())
		}
		if md5 != origMD5 {
			return "", fmt.Errorf("copy %s MD5 checksum %s does not match original %s", destFile, md5, origMD5)
		}
		bagFiles = append(bagFiles, mf.Filename)
	}

	err = svc.generateBaggitManifests(js, bagAssembleDir, bagFiles)
	if err != nil {
		return "", fmt.Errorf("unable to generate manifests: %s", err.Error())
	}

	svc.logInfo(js, "Generate baggit tar file")
	cmdArray := []string{"cf", destTar, "-C", bagBaseDir, bagDirName}
	cmd := exec.Command("tar", cmdArray...)
	svc.logInfo(js, fmt.Sprintf("%+v", cmd))
	_, err = cmd.Output()
	if err != nil {
		return "", fmt.Errorf("unable to create tar of bag: %s", err.Error())
	}

	svc.logInfo(js, fmt.Sprintf("Clean up bag assembly directory %s", bagAssembleDir))
	err = os.RemoveAll(bagAssembleDir)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to clean up %s: %s", bagAssembleDir, err.Error()))
	}
	return destTar, nil
}

func (svc *ServiceContext) generateBaggitManifests(js *jobStatus, bagDir string, bagFiles []string) error {
	svc.logInfo(js, fmt.Sprintf("Generate manifests for %s", bagDir))
	md5FileName := path.Join(bagDir, "manifest-md5.txt")
	sha256FileName := path.Join(bagDir, "manifest-sha256.txt")
	md5Data := ""
	sha256Data := ""
	for _, bf := range bagFiles {
		src := path.Join(bagDir, "data", bf)
		md5 := md5Checksum(src)
		md5Data += fmt.Sprintf("%s %s\n", md5, path.Join("data", bf))
		sha256 := sha256Checksum(src)
		sha256Data += fmt.Sprintf("%s %s\n", sha256, path.Join("data", bf))
	}
	os.WriteFile(md5FileName, []byte(md5Data), 0744)
	os.WriteFile(sha256FileName, []byte(sha256Data), 0744)

	tagMd5FileName := path.Join(bagDir, "tagmanifest-md5.txt")
	tagSha256FileName := path.Join(bagDir, "tagmanifest-sha256.txt")
	tagFiles := []string{"aptrust-info.txt", "bag-info.txt", "manifest-md5.txt", "manifest-sha256.txt"}
	md5Data = ""
	sha256Data = ""
	for _, tf := range tagFiles {
		src := path.Join(bagDir, tf)
		md5 := md5Checksum(src)
		md5Data += fmt.Sprintf("%s %s\n", md5, tf)
		sha256 := sha256Checksum(src)
		sha256Data += fmt.Sprintf("%s %s\n", sha256, tf)
	}
	os.WriteFile(tagMd5FileName, []byte(md5Data), 0744)
	os.WriteFile(tagSha256FileName, []byte(sha256Data), 0744)
	return nil
}
