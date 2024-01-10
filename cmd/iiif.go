package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type iiifPathInfo struct {
	FileName     string
	BucketPrefix string
	StagePath    string
}

func (iifp *iiifPathInfo) S3Key() string {
	return fmt.Sprintf("%s/%s", iifp.BucketPrefix, iifp.FileName)
}

func (svc *ServiceContext) publishToIIIF(js *jobStatus, mf *masterFile, srcPath string, overwrite bool) error {
	svc.logInfo(js, fmt.Sprintf("Publish master file %s from %s to IIIF; overwrite %t", mf.PID, srcPath, overwrite))

	svc.logInfo(js, "Validate file type is TIF or JP2")
	fileType := strings.ToLower(mf.ImageTechMeta.ImageFormat)
	if fileType != "tiff" && fileType != "jp2" {
		return fmt.Errorf("unsupported image format for %s: %s", mf.PID, mf.ImageTechMeta.ImageFormat)
	}

	jp2kInfo := svc.iiifPath(mf.PID)
	iiifExist, err := svc.iiifExists(jp2kInfo)
	if err != nil {
		return fmt.Errorf("unable to check for existance of for %s: %s", mf.PID, err.Error())
	}

	if overwrite == false && iiifExist {
		svc.logInfo(js, fmt.Sprintf("MasterFile %s already has JP2k file at S3:%s; skipping creation", mf.PID, svc.IIIF.Bucket, jp2kInfo.S3Key()))
		return nil
	}

	if fileType == "jp2" {
		svc.logInfo(js, fmt.Sprintf("Master file %s is already jp2; send directly to IIIF staging: %s", mf.PID, jp2kInfo.StagePath))
		copyFile(srcPath, jp2kInfo.StagePath, 0664)
	} else if fileType == "tiff" {
		svc.logInfo(js, fmt.Sprintf("Compressing %s to %s using imagemagick...", srcPath))
		rawFileInfo, _ := os.Stat(srcPath)
		firstPage := fmt.Sprintf("%s[0]", srcPath) // need the [0] as some tifs have multiple pages. only want the first.
		cmdArray := []string{firstPage, "-define", "jp2:rate=50 jp2:progression-order=RPCL jp2:number-resolutions=7", jp2kInfo.StagePath}
		startTime := time.Now()
		cmd := exec.Command("magick", cmdArray...)
		svc.logInfo(js, fmt.Sprintf("%+v", cmd))
		_, err = cmd.Output()
		if err != nil {
			return err
		}
		elapsed := time.Since(startTime)
		svc.logInfo(js, fmt.Sprintf("...compression complete; tif size %.2fM, elapsed time %.2f seconds", float64(rawFileInfo.Size())/1000000.0, elapsed.Seconds()))
	}

	svc.logInfo(js, fmt.Sprintf("Upload staged  jp2 file %s to S3 IIIF bucket %s", jp2kInfo.StagePath, svc.IIIF.Bucket))
	err = svc.uploadToS3(jp2kInfo)

	svc.logInfo(js, fmt.Sprintf("%s has been published to IIIF", mf.PID))
	return nil
}
func (svc *ServiceContext) uploadToS3(iiifInfo iiifPathInfo) error {
	// NOTE: the JP2 files are small so no need to use the large object put options
	jp2File, err := os.Open(iiifInfo.StagePath)
	if err != nil {
		return err
	}
	_, err = svc.IIIF.S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(svc.IIIF.Bucket),
		Key:    aws.String(iiifInfo.S3Key()),
		Body:   jp2File,
	})
	if err != nil {
		return err
	}
	return nil
}

func (svc *ServiceContext) iiifExists(iiifInfo iiifPathInfo) (bool, error) {
	out, err := svc.IIIF.S3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(svc.IIIF.Bucket),
		Prefix:  aws.String(iiifInfo.BucketPrefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, err
	}
	if len(out.Contents) == 0 {
		return false, nil
	}
	return true, nil
}

func (svc *ServiceContext) unpublishIIIF(js *jobStatus, mf *masterFile) {
	iiifInfo := svc.iiifPath(mf.PID)
	svc.logInfo(js, fmt.Sprintf("Removing masterfile %s published to IIIF %s", mf.PID, iiifInfo.S3Key()))
	var objectIds []types.ObjectIdentifier
	objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(iiifInfo.S3Key())})
	_, err := svc.IIIF.S3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(svc.IIIF.Bucket),
		Delete: &types.Delete{Objects: objectIds},
	})
	if err != nil {
		svc.logError(js, fmt.Sprintf("unable to delete %s: %s", mf.PID, err.Error()))
	}
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
		FileName:     jp2kFilename,
		BucketPrefix: path.Join(pidParts[0], pidDirs),
		StagePath:    path.Join(svc.IIIF.StagingDir, jp2kFilename),
	}
}
