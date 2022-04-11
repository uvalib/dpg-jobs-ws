package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func (svc *ServiceContext) createImageTechMetadata(mf *masterFile, mfPath string) error {
	cmdArray := []string{"-json", mfPath}
	stdout, err := exec.Command("exiftool", cmdArray...).Output()
	if err != nil {
		return err
	}

	var jsonDataArray []map[string]interface{}
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
		Compression:  fmt.Sprintf("%v", jsonMD["Compression"]),
		ColorProfile: fmt.Sprintf("%v", jsonMD["ProfileDescription"]),
		ColorSpace:   fmt.Sprintf("%v", jsonMD["ColorSpaceData"]),
		Depth:        getDepth(jsonMD),
		Resolution:   getUInt(jsonMD, "XResolution"),
		Equipment:    fmt.Sprintf("%v", jsonMD["Make"]),
		Software:     fmt.Sprintf("%v", jsonMD["Software"]),
		Model:        fmt.Sprintf("%v", jsonMD["Model"]),
		ExifVersion:  fmt.Sprintf("%v", jsonMD["ExifVersion"]),
		CaptureDate:  getDate(jsonMD, "DateCreated"),
		ISO:          getUInt(jsonMD, "ISO"),
		ExposureBias: fmt.Sprintf("%v", jsonMD["ExposureCompensation"]),
		ExposureTime: fmt.Sprintf("%v", jsonMD["ExposureTime"]),
		Aperture:     fmt.Sprintf("%v", jsonMD["ApertureValue"]),
		FocalLength:  getFocalLength(jsonMD),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = svc.GDB.Create(&md).Error
	if err != nil {
		return err
	}
	return nil
}

func getUInt(data map[string]interface{}, fieldName string) uint {
	raw := data[fieldName]
	floatVal, ok := raw.(float64)
	if ok {
		return uint(floatVal)
	}
	return 0
}

func getFocalLength(data map[string]interface{}) float64 {
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

func getDepth(data map[string]interface{}) uint {
	bitPerSampleStr := data["BitsPerSample"].(string)
	bitsPerSampleStr := strings.Split(bitPerSampleStr, " ")[0]
	bitsPerSample, _ := strconv.ParseFloat(bitsPerSampleStr, 64)
	samplePerPixel := data["SamplesPerPixel"].(float64)
	return uint(bitsPerSample * samplePerPixel)
}

func getDate(data map[string]interface{}, field string) *time.Time {
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
