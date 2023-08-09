package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

func (svc *ServiceContext) publishToIIIF(js *jobStatus, mf *masterFile, srcPath string, overwrite bool) error {
	svc.logInfo(js, fmt.Sprintf("Publish master file %s from %s to IIIF; overwrite %t", mf.PID, srcPath, overwrite))

	svc.logInfo(js, "Validate file type is TIF or JP2")
	workPath := srcPath
	fileType := strings.ToLower(mf.ImageTechMeta.ImageFormat)
	if fileType != "tiff" && fileType != "jp2" {
		svc.logError(js, fmt.Sprintf("Unsupported image format for %s: %s", mf.PID, mf.ImageTechMeta.ImageFormat))
		return fmt.Errorf("unsupported image format for %s: %s", mf.PID, mf.ImageTechMeta.ImageFormat)
	}

	jp2kInfo := svc.iiifPath(mf.PID)
	if overwrite == false && pathExists(jp2kInfo.absolutePath) {
		svc.logInfo(js, fmt.Sprintf("MasterFile %s already has JP2k file at %s; skipping creation", mf.PID, jp2kInfo.absolutePath))
		return nil
	}

	err := ensureDirExists(jp2kInfo.basePath, 0777)
	if err != nil {
		return err
	}

	if fileType == "jp2" {
		copyFile(workPath, jp2kInfo.absolutePath, 0664)
		svc.logInfo(js, fmt.Sprintf("Copied JPEG-2000 image using '%s' as input file for the creation of deliverable '%s'", workPath, jp2kInfo.basePath))
	} else if fileType == "tiff" {
		svc.logInfo(js, fmt.Sprintf("Compressing %s to %s using imagemagick...", workPath, jp2kInfo.absolutePath))
		firstPage := fmt.Sprintf("%s[0]", workPath) // need the [0] as some tifs have multiple pages. only want the first.
		cmdArray := []string{firstPage, "-define", "jp2:rate=50", "-define", "jp2:progression-order=RPCL", "-define", "jp2 :number-resolutions=7", jp2kInfo.absolutePath}
		startTime := time.Now()
		cmd := exec.Command("convert", cmdArray...)
		svc.logInfo(js, fmt.Sprintf("%+v", cmd))
		_, err = cmd.Output()
		if err != nil {
			return err
		}
		elapsed := time.Since(startTime)
		svc.logInfo(js, fmt.Sprintf("...compression complete; tif size %.2fM, elapsed time %.2f seconds", float64(mf.Filesize)/1000000.0, elapsed.Seconds()))
	}

	svc.logInfo(js, fmt.Sprintf("%s has been published to IIIF", mf.PID))
	return nil
}

func (svc *ServiceContext) unpublishIIIF(js *jobStatus, mf *masterFile) {
	iiifInfo := svc.iiifPath(mf.PID)
	svc.logInfo(js, fmt.Sprintf("Removing file published to IIIF: %s", iiifInfo.absolutePath))
	if pathExists(iiifInfo.absolutePath) {
		os.Remove(iiifInfo.absolutePath)
		files, _ := os.ReadDir(iiifInfo.basePath)
		if len(files) == 0 {
			os.Remove(iiifInfo.basePath)
		}
	} else {
		svc.logError(js, fmt.Sprintf("No IIIF file found for %s", mf.Filename))
	}
}

type iiifPathInfo struct {
	basePath     string
	fileName     string
	absolutePath string
}

func (svc *ServiceContext) iiifPath(mfPID string) iiifPathInfo {
	pidParts := strings.Split(mfPID, ":")
	base := pidParts[1]
	jp2kFilename := fmt.Sprintf("%s.jp2", base)
	parts := make([]string, 0)
	for len(base) > 2 {
		part := base[0:2]
		base = base[2:]
		parts = append(parts, part)
	}
	if len(base) > 0 {
		parts = append(parts, base)
	}

	pidDirs := strings.Join(parts, "/")
	return iiifPathInfo{
		fileName:     jp2kFilename,
		basePath:     path.Join(svc.IIIF.Dir, pidParts[0], pidDirs),
		absolutePath: path.Join(svc.IIIF.Dir, pidParts[0], pidDirs, jp2kFilename),
	}
}
