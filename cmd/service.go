package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"html/template"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type htmlTemplates struct {
	Fees            *template.Template
	OrderAvailable  *template.Template
	PDFOrderSummary *template.Template
}

// ServiceContext contains common data used by all handlers
type ServiceContext struct {
	Version       string
	SMTP          SMTPConfig
	GDB           *gorm.DB
	ArchiveDir    string
	IIIFDir       string
	ProcessingDir string
	DeliveryDir   string
	TrackSysURL   string
	HTTPClient    *http.Client
	Templates     htmlTemplates
}

// RequestError contains http status code and message for a failed HTTP request
type RequestError struct {
	StatusCode int
	Message    string
}

// InitializeService sets up the service context for all API handlers
func InitializeService(version string, cfg *ServiceConfig) *ServiceContext {
	ctx := ServiceContext{Version: version,
		SMTP:          cfg.SMTP,
		ArchiveDir:    cfg.ArchiveDir,
		IIIFDir:       cfg.IIIFDir,
		DeliveryDir:   cfg.DeliveryDir,
		ProcessingDir: cfg.ProcessingDir,
		TrackSysURL:   cfg.TrackSysURL,
	}

	log.Printf("INFO: connecting to DB...")
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
		cfg.DB.User, cfg.DB.Pass, cfg.DB.Host, cfg.DB.Name)
	gdb, err := gorm.Open(mysql.Open(connectStr), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	ctx.GDB = gdb
	log.Printf("INFO: DB Connection established")

	log.Printf("INFO: load html templates")
	ctx.Templates.Fees, err = template.New("fees.html").ParseFiles("./templates/fees.html")
	if err != nil {
		log.Fatal(err)
	}
	ctx.Templates.OrderAvailable, err = template.New("order.html").ParseFiles("./templates/order.html")
	if err != nil {
		log.Fatal(err)
	}
	ctx.Templates.PDFOrderSummary, err = template.New("pdf_ordersummary.html").ParseFiles("./templates/pdf_ordersummary.html")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("INFO: create HTTP client...")
	defaultTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 600 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	ctx.HTTPClient = &http.Client{
		Transport: defaultTransport,
		Timeout:   5 * time.Second,
	}
	log.Printf("INFO: HTTP Client created")

	return &ctx
}

// IgnoreFavicon is a dummy to handle browser favicon requests without warnings
func (svc *ServiceContext) ignoreFavicon(c *gin.Context) {
}

// GetVersion reports the version of the serivce
func (svc *ServiceContext) getVersion(c *gin.Context) {
	build := "unknown"
	// working directory is the bin directory, and build tag is in the root
	files, _ := filepath.Glob("../buildtag.*")
	if len(files) == 1 {
		build = strings.Replace(files[0], "../buildtag.", "", 1)
	}

	vMap := make(map[string]string)
	vMap["version"] = svc.Version
	vMap["build"] = build
	c.JSON(http.StatusOK, vMap)
}

// HealthCheck reports the health of the serivce
func (svc *ServiceContext) healthCheck(c *gin.Context) {
	type hcResp struct {
		Healthy bool   `json:"healthy"`
		Message string `json:"message,omitempty"`
	}
	hcMap := make(map[string]hcResp)
	hcMap["jobservice"] = hcResp{Healthy: true}

	hcMap["database"] = hcResp{Healthy: true}
	sqlDB, err := svc.GDB.DB()
	if err != nil {
		hcMap["database"] = hcResp{Healthy: false, Message: err.Error()}
	} else {
		err := sqlDB.Ping()
		if err != nil {
			hcMap["database"] = hcResp{Healthy: false, Message: err.Error()}
		}
	}

	c.JSON(http.StatusOK, hcMap)
}

func (svc *ServiceContext) getRequest(url string) ([]byte, *RequestError) {
	log.Printf("POST request: %s", url)
	startTime := time.Now()
	req, _ := http.NewRequest("GET", url, nil)
	httpClient := svc.HTTPClient
	rawResp, rawErr := httpClient.Do(req)
	resp, err := handleAPIResponse(url, rawResp, rawErr)
	elapsedNanoSec := time.Since(startTime)
	elapsedMS := int64(elapsedNanoSec / time.Millisecond)

	if err != nil {
		log.Printf("ERROR: Failed response from GET %s - %d:%s. Elapsed Time: %d (ms)",
			url, err.StatusCode, err.Message, elapsedMS)
	} else {
		log.Printf("Successful response from POST %s. Elapsed Time: %d (ms)", url, elapsedMS)
	}
	return resp, err
}

func handleAPIResponse(logURL string, resp *http.Response, err error) ([]byte, *RequestError) {
	if err != nil {
		status := http.StatusBadRequest
		errMsg := err.Error()
		if strings.Contains(err.Error(), "Timeout") {
			status = http.StatusRequestTimeout
			errMsg = fmt.Sprintf("%s timed out", logURL)
		} else if strings.Contains(err.Error(), "connection refused") {
			status = http.StatusServiceUnavailable
			errMsg = fmt.Sprintf("%s refused connection", logURL)
		}
		return nil, &RequestError{StatusCode: status, Message: errMsg}
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		defer resp.Body.Close()
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		status := resp.StatusCode
		errMsg := string(bodyBytes)
		return nil, &RequestError{StatusCode: status, Message: errMsg}
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	return bodyBytes, nil
}

func padLeft(str string, tgtLen int) string {
	for {
		if len(str) == tgtLen {
			return str
		}
		str = "0" + str
	}
}

func md5Checksum(filename string) string {
	if pathExists(filename) {
		data, _ := os.ReadFile(filename)
		md5 := fmt.Sprintf("%x", md5.Sum(data))
		return md5
	}
	return ""
}

func getMasterFilePageNum(filename string) int {
	noExt := strings.ReplaceAll(filename, ".tif", "")
	numStr := strings.Split(noExt, "_")[1]
	num, _ := strconv.ParseInt(numStr, 10, 0)
	return int(num)
}

func pathExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func ensureDirExists(dir string, mode fs.FileMode) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			return err
		}
	}
	return nil
}

// copy file from src to dest, set permissions and return the MD5 checksum of the copy
func copyFile(src string, dest string, mode fs.FileMode) (string, error) {
	origFile, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer origFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return "", err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, origFile)
	if err != nil {
		return "", err
	}
	destFile.Close()

	os.Chmod(dest, mode)
	return md5Checksum(dest), nil
}

func copyAll(srcDir string, destDir string) error {
	if _, err := os.Stat(srcDir); os.IsNotExist(err) {
		return err
	}
	files, err := ioutil.ReadDir(srcDir)
	if err != nil {
		return err
	}
	ensureDirExists(destDir, 0775)
	for _, fi := range files {
		srcFile := path.Join(srcDir, fi.Name())
		destFile := path.Join(destDir, fi.Name())
		_, err = copyFile(srcFile, destFile, 0664)
		if err != nil {
			return err
		}
	}
	return nil
}

type tifInfo struct {
	filename string
	path     string
	size     int64
}

func getTifFiles(srcDir string, unitID int64) ([]tifInfo, error) {
	tifFiles := make([]tifInfo, 0)
	files, err := ioutil.ReadDir(srcDir)
	if err != nil {
		return nil, fmt.Errorf("Unable to read %s: %s", srcDir, err.Error())
	}

	mfRegex := regexp.MustCompile(fmt.Sprintf(`^%09d_\w{4,}\.tif$`, unitID))
	for _, fi := range files {
		fName := fi.Name()
		if strings.Index(fName, ".tif") > -1 {
			if !mfRegex.Match([]byte(fName)) {
				return nil, fmt.Errorf("Invalid tif file name: %s for unit %d", fName, unitID)

			}
			tifFiles = append(tifFiles, tifInfo{path: path.Join(srcDir, fName), filename: fName, size: fi.Size()})
		}
	}

	return tifFiles, nil
}

func (svc *ServiceContext) ensureMD5(js *jobStatus, mf *masterFile, mfPath string) {
	if mf.MD5 == "" {
		svc.logInfo(js, fmt.Sprintf("Masterfile %s is missing MD5 checksum; calculating it now from %s", mf.PID, mfPath))
		mf.MD5 = md5Checksum(mfPath)
		mf.UpdatedAt = time.Now()
		err := svc.GDB.Model(mf).Select("UpdatedAt", "MD5").Updates(*mf).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update chacksum: %s", err.Error()))
		}
	}
}
