package main

import (
	"errors"
	"fmt"
	"log"
	"os/exec"
	"path"
	"strings"
)

func (svc *ServiceContext) publishToIIIF(js *jobStatus, mf *masterFile, path string, overwrite bool) error {
	svc.logInfo(js, fmt.Sprintf("Publish %s to IIIF", mf.PID))

	if strings.Index(mf.Filename, ".tif") > -1 {
		// kakadu cant handle compression. remove it if detected
		if mf.ImageTechMeta.Compression != "Uncompressed" {
			cmdArray := []string{"-compress", "none", "-quiet", path, path}
			_, err := exec.Command("convert", cmdArray...).Output()
			if err != nil {
				svc.logError(js, fmt.Sprintf("Unable to remove compression on %s: %s", mf.PID, err.Error()))
			} else {
				svc.logInfo(js, fmt.Sprintf("MasterFile %s is compressed. This has been corrected automatically.", mf.PID))
			}
		}
	}

	jp2kInfo := svc.iiifPath(mf)
	err := ensureDirExists(jp2kInfo.basePath, 0777)
	if err != nil {
		return err
	}

	if strings.Index(mf.Filename, ".jp2") > -1 {
		copyFile(path, jp2kInfo.absolutePath, 0664)
		svc.logInfo(js, fmt.Sprintf("Copied JPEG-2000 image using '%s' as input file for the creation of deliverable '%s'", path, jp2kInfo.basePath))
	} else if strings.Index(mf.Filename, ".tif") > -1 {
		if overwrite == false && pathExists(jp2kInfo.absolutePath) {
			svc.logInfo(js, fmt.Sprintf("MasterFile %s already has JP2k file at %s; skipping creation", mf.PID, jp2kInfo.absolutePath))
			return nil
		}
		svc.logInfo(js, fmt.Sprintf("Compressing %s to %s...", path, jp2kInfo.absolutePath))
		_, err := exec.LookPath("kdu_compress")
		if err != nil {
			return errors.New("kdu_compress is not available")
		}
		cmdArray := []string{"-i", path, "-o", jp2kInfo.absolutePath, "-rate", "0.5",
			"Clayers=1", "Clevels=7", "Cuse_sop=yes", "-quiet", "-num_threads", "8",
			"Cprecincts={256,256},{256,256},{256,256},{128,128},{128,128},{64,64},{64,64},{32,32},{16,16}",
			"Corder=RPCL", "ORGgen_plt=yes", "ORGtparts=R", "Cblk={64,64}",
		}
		cmd := exec.Command("kdu_compress", cmdArray...)
		log.Printf("%+v", cmd)
		_, err = cmd.Output()
		if err != nil {
			return err
		}
		svc.logInfo(js, "...compression complete.")
	} else {
		return fmt.Errorf("%s is not a .tif or .jp2", path)
	}

	return nil
}

type iiifPathInfo struct {
	basePath     string
	fileName     string
	absolutePath string
}

func (svc *ServiceContext) iiifPath(mf *masterFile) iiifPathInfo {
	pidParts := strings.Split(mf.PID, ":")
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
		basePath:     path.Join(svc.IIIFDir, pidParts[0], pidDirs),
		absolutePath: path.Join(svc.IIIFDir, pidParts[0], pidDirs, jp2kFilename),
	}
}