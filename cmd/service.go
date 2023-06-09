package main

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
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
	AuditResults    *template.Template
	PHashResults    *template.Template
	PDFOrderSummary *template.Template
}

type archivesSpaceContext struct {
	User      string
	Pass      string
	AuthToken string
	ExpiresAt time.Time
	APIURL    string
}

// ServiceContext contains common data used by all handlers
type ServiceContext struct {
	Version       string
	ServiceURL    string
	SMTP          SMTPConfig
	GDB           *gorm.DB
	ArchiveDir    string
	IIIF          IIIFConfig
	ProcessingDir string
	DeliveryDir   string
	TrackSys      TrackSysConfig
	ArchivesSpace archivesSpaceContext
	HathiTrust    HathiTrustConfig
	ReindexURL    string
	XMLReindexURL string
	OcrURL        string
	HTTPClient    *http.Client
	Templates     htmlTemplates
	OcrRequests   []int64
	JSTORCookies  []*http.Cookie
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
		IIIF:          cfg.IIIF,
		DeliveryDir:   cfg.DeliveryDir,
		ProcessingDir: cfg.ProcessingDir,
		HathiTrust:    cfg.HathiTrust,
		TrackSys:      cfg.TrackSys,
		ReindexURL:    cfg.ReindexURL,
		XMLReindexURL: cfg.XMLReindexURL,
		OcrURL:        cfg.OcrURL,
		ServiceURL:    cfg.ServiceURL,
		OcrRequests:   make([]int64, 0),
	}

	log.Printf("INFO: connecting to DB...")
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
		cfg.DB.User, cfg.DB.Pass, cfg.DB.Host, cfg.DB.Name)
	gdb, err := gorm.Open(mysql.Open(connectStr), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("INFO: configure db pool settings...")
	sqlDB, _ := gdb.DB()
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(10)
	ctx.GDB = gdb
	log.Printf("INFO: DB Connection established")

	log.Printf("INFO: initialize archivesSpace")
	var es externalSystem
	err = ctx.GDB.Where("name=?", "ArchivesSpace").Find(&es).Error
	if err != nil {
		log.Fatal(err)
	}
	ctx.ArchivesSpace.User = cfg.ArchivesSpace.User
	ctx.ArchivesSpace.Pass = cfg.ArchivesSpace.Pass
	ctx.ArchivesSpace.APIURL = es.APIURL

	log.Printf("INFO: load html templates")
	ctx.Templates.AuditResults, err = template.New("audit.html").ParseFiles("./templates/audit.html")
	if err != nil {
		log.Fatal(err)
	}
	ctx.Templates.PHashResults, err = template.New("phash.html").ParseFiles("./templates/phash.html")
	if err != nil {
		log.Fatal(err)
	}
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
		Timeout:   30 * time.Second,
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
	return svc.sendRequest("GET", url, nil)
}
func (svc *ServiceContext) putRequest(url string) ([]byte, *RequestError) {
	return svc.sendRequest("PUT", url, nil)
}
func (svc *ServiceContext) postFormRequest(url string, payload *url.Values) ([]byte, *RequestError) {
	return svc.sendRequest("POST", url, payload)
}

func (svc *ServiceContext) sendRequest(verb string, url string, payload *url.Values) ([]byte, *RequestError) {
	log.Printf("INFO: %s request: %s", verb, url)
	startTime := time.Now()

	var req *http.Request
	if verb == "POST" && payload != nil {
		req, _ = http.NewRequest("POST", url, strings.NewReader(payload.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req, _ = http.NewRequest(verb, url, nil)
	}

	// NOTE: this is required for the calls to getMarc
	req.Header.Add("User-Agent", "Golang_DPG_Jobs")

	rawResp, rawErr := svc.HTTPClient.Do(req)
	resp, err := handleAPIResponse(url, rawResp, rawErr)
	elapsedNanoSec := time.Since(startTime)
	elapsedMS := int64(elapsedNanoSec / time.Millisecond)

	if err != nil {
		log.Printf("ERROR: Failed response from %s %s - %d:%s. Elapsed Time: %d (ms)",
			verb, url, err.StatusCode, err.Message, elapsedMS)
	} else {
		log.Printf("INFO: Successful response from %s %s. Elapsed Time: %d (ms)", verb, url, elapsedMS)
	}
	return resp, err
}

func (svc *ServiceContext) sendASGetRequest(url string) ([]byte, *RequestError) {
	return svc.sendASRequest("GET", url, nil)
}
func (svc *ServiceContext) sendASPostRequest(url string, payload interface{}) ([]byte, *RequestError) {
	return svc.sendASRequest("POST", url, payload)
}
func (svc *ServiceContext) sendASRequest(verb string, url string, payload interface{}) ([]byte, *RequestError) {
	fullURL := fmt.Sprintf("%s%s", svc.ArchivesSpace.APIURL, url)
	log.Printf("INFO: archivesspace %s request: %s", verb, fullURL)
	startTime := time.Now()

	var req *http.Request
	if verb == "POST" {
		b, _ := json.Marshal(payload)
		req, _ = http.NewRequest("POST", fullURL, bytes.NewBuffer(b))
	} else {
		req, _ = http.NewRequest("GET", fullURL, nil)
	}

	req.Header.Add("Content-type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-ArchivesSpace-Session", svc.ArchivesSpace.AuthToken)
	rawResp, rawErr := svc.HTTPClient.Do(req)
	resp, err := handleAPIResponse(url, rawResp, rawErr)
	elapsedNanoSec := time.Since(startTime)
	elapsedMS := int64(elapsedNanoSec / time.Millisecond)

	if err != nil {
		log.Printf("ERROR: Failed response from %s %s - %d:%s. Elapsed Time: %d (ms)",
			verb, url, err.StatusCode, err.Message, elapsedMS)
	} else {
		log.Printf("INFO: Successful response from %s %s. Elapsed Time: %d (ms)", verb, url, elapsedMS)
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

func md5Checksum(filename string) string {
	if pathExists(filename) {
		data, _ := os.ReadFile(filename)
		md5 := fmt.Sprintf("%x", md5.Sum(data))
		return md5
	}
	return ""
}
func sha256Checksum(filename string) string {
	if pathExists(filename) {
		data, _ := os.ReadFile(filename)
		md5 := fmt.Sprintf("%x", sha256.Sum256(data))
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

// add a file to the target zip and return the new zip filesize
func addFileToZip(zipFilePath string, zw *zip.Writer, filePath string, fileName string) (int64, error) {
	fileToZip, err := os.Open(path.Join(filePath, fileName))
	if err != nil {
		return 0, err
	}
	defer fileToZip.Close()
	zipFileWriter, err := zw.Create(fileName)
	if _, err := io.Copy(zipFileWriter, fileToZip); err != nil {
		return 0, err
	}
	fi, _ := os.Stat(zipFilePath)
	return fi.Size(), nil
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

func (svc *ServiceContext) getTifFiles(js *jobStatus, srcDir string, unitID int64) ([]tifInfo, error) {
	svc.logInfo(js, fmt.Sprintf("Get all .tif files from %s", srcDir))
	tifFiles := make([]tifInfo, 0)
	mfRegex := regexp.MustCompile(fmt.Sprintf(`^%09d_\w{4,}\.tif$`, unitID))
	err := filepath.Walk(srcDir, func(fullPath string, entry os.FileInfo, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".tif" {
			return nil
		}
		if !mfRegex.Match([]byte(entry.Name())) {
			return fmt.Errorf("invalid file in %s: %s", srcDir, entry.Name())
		}
		tifFiles = append(tifFiles, tifInfo{path: fullPath, filename: entry.Name(), size: entry.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tifFiles, nil
}

func (svc *ServiceContext) ensureMD5(js *jobStatus, mf *masterFile, mfPath string) {
	if mf.MD5 == "" {
		svc.logInfo(js, fmt.Sprintf("Masterfile %s is missing MD5 checksum; calculating it now from %s", mf.PID, mfPath))
		mf.MD5 = md5Checksum(mfPath)
		err := svc.GDB.Model(mf).Select("MD5").Updates(*mf).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to update chacksum: %s", err.Error()))
		}
	}
}

func (svc *ServiceContext) cleanupWorkDirectories(js *jobStatus, unitID int64) {
	unitDir := fmt.Sprintf("%09d", unitID)
	svc.logInfo(js, fmt.Sprintf("Cleaning up unit %09d directories", unitID))
	tmpDir := path.Join(svc.ProcessingDir, "finalization", "tmp", unitDir)
	os.RemoveAll(tmpDir)

	srcDir := path.Join(svc.ProcessingDir, "finalization", unitDir)
	delDir := path.Join(svc.ProcessingDir, "ready_to_delete", unitDir)
	svc.logInfo(js, fmt.Sprintf("Moving %s to %s", srcDir, delDir))
	if pathExists(delDir) {
		svc.logInfo(js, fmt.Sprintf("%s already exists; cleaning it up", delDir))
		os.RemoveAll(delDir)
	}
	err := os.Rename(srcDir, delDir)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to move working file to %s: %s", delDir, err.Error()))
	}

	// now check for a lingering unit directory in scan...
	srcDir = path.Join(svc.ProcessingDir, "scan", unitDir)
	if pathExists(srcDir) {
		svc.logInfo(js, fmt.Sprintf("Cleaning up unit %09d scan directories", unitID))
		delDir = path.Join(svc.ProcessingDir, "ready_to_delete", "from_scan", unitDir)
		svc.logInfo(js, fmt.Sprintf("Moving %s to %s", srcDir, delDir))
		if pathExists(delDir) {
			svc.logInfo(js, fmt.Sprintf("%s already exists; cleaning it up", delDir))
			os.RemoveAll(delDir)
		}
		err := os.Rename(srcDir, delDir)
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to move scan directory: %s", err.Error()))
		}
	}
}
