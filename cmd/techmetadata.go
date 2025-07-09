package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func (svc *ServiceContext) createImageTechMetadata(mf *masterFile, mfPath string) error {
	cmdArray := []string{"-json", mfPath}
	cmd := exec.Command("exiftool", cmdArray...)
	log.Printf("INFO: get %s tech metadata with: %+v", mf.PID, cmd)
	stdout, err := cmd.Output()
	if err != nil {
		log.Printf("ERROR: unable to get tech metadata: %s: %s", stdout, err.Error())
		return err
	}

	var jsonDataArray []map[string]any
	err = json.Unmarshal(stdout, &jsonDataArray)
	if err != nil {
		return err
	}
	if len(jsonDataArray) == 0 {
		return errors.New("no metadata returned")
	}
	jsonMD := jsonDataArray[0]
	md := imageTechMeta{
		MasterFileID: mf.ID,
		ImageFormat:  fmt.Sprintf("%v", jsonMD["FileType"]),
		Width:        getUInt(jsonMD, "ImageWidth"),
		Height:       getUInt(jsonMD, "ImageHeight"),
		Depth:        getDepth(jsonMD),
		Resolution:   getUInt(jsonMD, "XResolution"),
	}

	if jsonMD["Compression"] != nil {
		md.Compression = fmt.Sprintf("%v", jsonMD["Compression"])
	}
	if jsonMD["ProfileDescription"] != nil {
		md.ColorProfile = fmt.Sprintf("%v", jsonMD["ProfileDescription"])
	}
	if jsonMD["ColorSpace"] != nil {
		md.ColorSpace = fmt.Sprintf("%v", jsonMD["ColorSpace"])
		if md.ColorSpace == "Uncalibrated" {
			if jsonMD["ColorMode"] != nil {
				md.ColorSpace = fmt.Sprintf("%v", jsonMD["ColorMode"])
			} else if jsonMD["ColorSpaceData"] != nil {
				md.ColorSpace = fmt.Sprintf("%v", jsonMD["ColorSpaceData"])
			}
		}
	} else if jsonMD["ColorSpaceData"] != nil {
		md.ColorSpace = fmt.Sprintf("%v", jsonMD["ColorSpaceData"])
	}
	if jsonMD["Make"] != nil {
		md.Equipment = fmt.Sprintf("%v", jsonMD["Make"])
	}
	if jsonMD["Software"] != nil {
		md.Software = fmt.Sprintf("%v", jsonMD["Software"])
	}
	if jsonMD["Model"] != nil {
		md.Model = fmt.Sprintf("%v", jsonMD["Model"])
	}
	if jsonMD["ExifVersion"] != nil {
		md.ExifVersion = fmt.Sprintf("%v", jsonMD["ExifVersion"])
	}
	if jsonMD["DateCreated"] != nil {
		md.CaptureDate = getDate(jsonMD, "DateCreated")
	}
	if jsonMD["ISO"] != nil {
		md.ISO = getUInt(jsonMD, "ISO")
	}
	if jsonMD["ExposureCompensation"] != nil {
		md.ExposureBias = fmt.Sprintf("%v", jsonMD["ExposureCompensation"])
	}
	if jsonMD["ExposureTime"] != nil {
		md.ExposureTime = fmt.Sprintf("%v", jsonMD["ExposureTime"])
	}
	if jsonMD["ApertureValue"] != nil {
		md.Aperture = fmt.Sprintf("%v", jsonMD["ApertureValue"])
	}
	if jsonMD["FocalLength"] != nil {
		md.FocalLength = getFocalLength(jsonMD)
	}

	log.Printf("INFO: %s tech metadata: %+v", mf.PID, md)
	err = svc.GDB.Create(&md).Error
	if err != nil {
		return err
	}
	mf.ImageTechMeta = md
	return nil
}

func getUInt(data map[string]any, fieldName string) uint {
	raw := data[fieldName]
	floatVal, ok := raw.(float64)
	if ok {
		return uint(floatVal)
	}
	return 0
}

func getFocalLength(data map[string]any) float64 {
	raw, ok := data["FocalLength"].(string)
	if ok == false {
		return 0.0
	}
	// format: 120.0 mm
	fvStr := strings.Split(raw, " ")[0]
	out, err := strconv.ParseFloat(fvStr, 64)
	if err != nil {
		return 0.0
	}
	return out
}

func getDepth(data map[string]any) uint {
	samplePerPixel, ok := data["SamplesPerPixel"].(float64)
	if ok == true {
		bitPerSampleStr, ok := data["BitsPerSample"].(string)
		if ok == true {
			bitsPerSampleStr := strings.Split(bitPerSampleStr, " ")[0]
			bitsPerSample, _ := strconv.ParseFloat(bitsPerSampleStr, 64)
			return uint(bitsPerSample * samplePerPixel)
		}
		bitsPerSample := data["BitsPerSample"].(float64)
		return uint(bitsPerSample * samplePerPixel)
	}
	return 0
}

func getDate(data map[string]any, field string) *time.Time {
	// format: 2016:03:14
	dateStr := fmt.Sprintf("%v", data[field])
	if dateStr == "" {
		return nil
	}
	t, err := time.Parse("2006:01:02", dateStr)
	if err != nil {
		return nil
	}
	return &t
}
