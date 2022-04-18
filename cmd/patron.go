package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
)

func (svc *ServiceContext) createPatronPDF(js *jobStatus, tgtUnit *unit) error {
	svc.logInfo(js, "Setting up assemble delivery directory to be used to build the PDF...")
	assembleDir := path.Join(svc.ProcessingDir, "finalization", "tmp", fmt.Sprintf("%09d", tgtUnit.ID))
	err := ensureDirExists(assembleDir, 0755)
	if err != nil {
		return err
	}

	pdfFileName := fmt.Sprintf("%d.pdf", tgtUnit.ID)
	pdfPath := path.Join(assembleDir, pdfFileName)
	os.Remove(pdfPath)

	unitDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", tgtUnit.ID))
	tifFiles, err := getTifFiles(unitDir, tgtUnit.ID)
	if err != nil {
		return fmt.Errorf("Unable to read tif files from %s: %s", unitDir, err.Error())
	}

	// process each tif one at a time. convert to scaled down jpg for PDF
	for _, tf := range tifFiles {
		svc.logInfo(js, fmt.Sprintf("Covert %s to scaled down JPG...", tf.filename))
		cmdArray := []string{"-quiet", "-resize", "1024x", "-density", "150",
			"-format", "jpg", "-path", assembleDir, fmt.Sprintf("%s[0]", tf.path)}
		_, err := exec.Command("mogrify", cmdArray...).Output()
		if err != nil {
			return fmt.Errorf("Unable to downsize %s: %s", tf.path, err.Error())
		}
	}

	jpgFiles := path.Join(assembleDir, "*.jpg")
	svc.logInfo(js, fmt.Sprintf("Covert %s to %s...", jpgFiles, pdfPath))
	cmdArray := []string{jpgFiles, pdfPath}
	_, err = exec.Command("convert", cmdArray...).Output()
	if err != nil {
		return fmt.Errorf("Unable to convert jpg files to pdf: %s", err.Error())
	}

	if pathExists(pdfPath) == false {
		return fmt.Errorf("Target PDF %s was not created", pdfPath)
	}

	// Zip the PDF into the delivery directory
	deliveryDir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", tgtUnit.OrderID))
	ensureDirExists(deliveryDir, 0755)
	zipFile := path.Join(deliveryDir, fmt.Sprintf("%d.zip", tgtUnit.ID))
	svc.logInfo(js, fmt.Sprintf("Zip PDF to %s", zipFile))
	os.Remove(zipFile)

	zf, err := os.Create(zipFile)
	if err != nil {
		return fmt.Errorf("Unable to create zip file: %s", err.Error())
	}
	defer zf.Close()
	zipWriter := zip.NewWriter(zf)
	_, err = addFileToZip(zipFile, zipWriter, assembleDir, pdfFileName)
	if err != nil {
		return err
	}
	zipWriter.Close()

	svc.logInfo(js, "Zip deliverable of PDF created.")
	return nil
}

// add a file to the target zip and return the new zip filesize
func addFileToZip(zipFile string, zw *zip.Writer, filePath string, fileName string) (int64, error) {
	fileToZip, err := os.Open(path.Join(filePath, fileName))
	if err != nil {
		return 0, err
	}
	defer fileToZip.Close()
	zipFileWriter, err := zw.Create(fileName)
	if _, err := io.Copy(zipFileWriter, fileToZip); err != nil {
		return 0, err
	}
	fi, _ := os.Stat(zipFile)
	return fi.Size(), nil
}
