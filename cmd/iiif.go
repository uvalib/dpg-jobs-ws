package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type iiifContext struct {
	FileName     string
	BucketPrefix string
	StagePath    string
}

func (iifp *iiifContext) S3Key() string {
	return fmt.Sprintf("%s/%s", iifp.BucketPrefix, iifp.FileName)
}

func (svc *ServiceContext) publishToIIIF(js *jobStatus, mf *masterFile, srcPath string, overwrite bool) error {
	svc.logInfo(js, fmt.Sprintf("Publish master file %s from %s to IIIF; overwrite %t", mf.PID, srcPath, overwrite))

	svc.logInfo(js, "Validate file type is TIF or JP2")
	fileType := strings.ToLower(mf.ImageTechMeta.ImageFormat)
	if fileType != "tiff" && fileType != "jp2" {
		return fmt.Errorf("unsupported image format for %s: %s", mf.PID, mf.ImageTechMeta.ImageFormat)
	}

	iiifInfo := svc.getIIIFContext(mf.PID)
	iiifExist, err := svc.iiifExists(iiifInfo)
	if err != nil {
		return fmt.Errorf("unable to check for existance of for %s: %s", mf.PID, err.Error())
	}

	if iiifExist {
		svc.logInfo(js, fmt.Sprintf("MasterFile %s already has a JP2k file on S3: %s/%s", mf.PID, svc.IIIF.Bucket, iiifInfo.S3Key()))
		if overwrite == false {
			svc.logInfo(js, "Overwrite not requested; nothing more to do")
			return nil
		}
		svc.logInfo(js, "Existing file will be overwritten")
	}

	if fileType == "jp2" {
		svc.logInfo(js, fmt.Sprintf("Master file %s is already jp2; send directly to IIIF staging: %s", mf.PID, iiifInfo.StagePath))
		copyFile(srcPath, iiifInfo.StagePath, 0664)
	} else if fileType == "tiff" {
		svc.logInfo(js, fmt.Sprintf("Compressing %s to %s using imagemagick...", srcPath, iiifInfo.StagePath))
		rawFileInfo, _ := os.Stat(srcPath)
		firstPage := fmt.Sprintf("%s[0]", srcPath) // need the [0] as some tifs have multiple pages. only want the first.
		cmdArray := []string{firstPage, "-define", "jp2:rate=50 jp2:progression-order=RPCL jp2:number-resolutions=7", iiifInfo.StagePath}
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

	svc.logInfo(js, fmt.Sprintf("Upload staged  jp2 file %s to S3 IIIF bucket %s:%s", iiifInfo.StagePath, svc.IIIF.Bucket, iiifInfo.S3Key()))
	err = svc.uploadToIIIF(iiifInfo.StagePath, iiifInfo.S3Key())

	svc.logInfo(js, fmt.Sprintf("%s has been published to IIIF", mf.PID))
	return nil
}

func (svc *ServiceContext) uploadToIIIF(srcPath string, s3Key string) error {
	jp2File, err := os.Open(srcPath)
	if err != nil {
		return err
	}

	_, err = svc.IIIF.S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(svc.IIIF.Bucket),
		Key:    aws.String(s3Key),
		Body:   jp2File,
	})

	if err != nil {
		return err
	}
	return nil
}

func (svc *ServiceContext) iiifExists(iiifInfo iiifContext) (bool, error) {
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

func (svc *ServiceContext) downlodFromIIIF(js *jobStatus, s3Key string, destFileName string) error {
	svc.logInfo(js, fmt.Sprintf("Download masterfile %s from IIIF to %s", s3Key, destFileName))
	resp, err := svc.IIIF.S3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(svc.IIIF.Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	destFile, err := os.Create(destFileName)
	if err != nil {
		return fmt.Errorf("unable to create destination %s for iiif download: %s", destFileName, err.Error())
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read image data from %s: %s", s3Key, err.Error())
	}
	_, err = destFile.Write(body)
	if err != nil {
		return fmt.Errorf("unable to write image data from %s to %s: %s", s3Key, destFileName, err.Error())
	}
	svc.logInfo(js, fmt.Sprintf("Masterfile %s downloaded from IIIF to %s", s3Key, destFileName))
	return nil
}

func (svc *ServiceContext) unpublishIIIF(js *jobStatus, s3Key string) error {
	svc.logInfo(js, fmt.Sprintf("Removing masterfile published to IIIF as %s", s3Key))
	var objectIds []types.ObjectIdentifier
	objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(s3Key)})
	_, err := svc.IIIF.S3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(svc.IIIF.Bucket),
		Delete: &types.Delete{Objects: objectIds},
	})
	if err != nil {
		return err
	}
	return nil
}

func (svc *ServiceContext) getIIIFContext(mfPID string) iiifContext {
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
	return iiifContext{
		FileName:     jp2kFilename,
		BucketPrefix: path.Join(pidParts[0], pidDirs),
		StagePath:    path.Join(svc.IIIF.StagingDir, jp2kFilename),
	}
}
