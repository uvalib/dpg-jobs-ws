package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

// Version of the service
const version = "1.16.4"

func main() {
	log.Printf("===> DPG backend processing service starting up <===")

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()
	svc := InitializeService(version, cfg)

	log.Printf("INFO: setup routes...")
	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	router := gin.Default()
	router.Use(gzip.Gzip(gzip.DefaultCompression))
	corsCfg := cors.DefaultConfig()
	corsCfg.AllowAllOrigins = true
	corsCfg.AllowCredentials = true
	corsCfg.AddAllowHeaders("Authorization")
	router.Use(cors.New(corsCfg))

	router.GET("/", svc.getVersion)
	router.GET("/favicon.ico", svc.ignoreFavicon)
	router.GET("/version", svc.getVersion)
	router.GET("/healthcheck", svc.healthCheck)

	router.POST("/script", svc.runScript)

	router.POST("/audit", svc.auditMasterFiles)
	router.POST("/phash", svc.generateMasterFilesPHash)

	router.POST("/hathitrust/package", svc.createHathiTrustPackage)
	router.POST("/hathitrust/package/submit", svc.submitHathiTrustPackage)
	router.GET("/hathitrust/package/submitted", svc.listHathiTrustSubmissions)
	router.POST("/hathitrust/metadata", svc.submitHathiTrustMetadata)

	router.GET("/archivesspace/lookup", svc.archivesSpaceMiddleware, svc.lookupArchivesSpaceURL)
	router.GET("/archivesspace/validate", svc.archivesSpaceMiddleware, svc.validateArchivesSpaceURL)
	router.POST("/archivesspace/publish", svc.archivesSpaceMiddleware, svc.publishToArchivesSpace)

	router.POST("/collections/:id/add", svc.collectionBulkAdd)

	router.GET("/jobs/:id", svc.getJobStatus)

	router.GET("/jstor/lookup", svc.lookupJstorMetadata)

	router.POST("/metadata/:id/baggit", svc.createBag)
	router.POST("/metadata/:id/publish", svc.publishToVirgo)

	router.GET("/ocr/languages", svc.getOCRLanguages)
	router.POST("/ocr", svc.handleOCRRequest)

	router.POST("/orders/:id/check", svc.checkOrderReady)
	router.POST("/orders/:id/summary", svc.createOrderSummary)
	router.GET("/orders/:id/summary", svc.viewOrderSummary)
	router.POST("/orders/:id/email", svc.createOrderEmail)
	router.POST("/orders/:id/email/send", svc.sendOrderEmail)
	router.POST("/orders/:id/fees", svc.sendFeesEmail)

	router.GET("/archive/exist", svc.archiveExists)
	router.POST("/units/:id/copy", svc.downloadFromArchive)
	router.POST("/units/:id/deliverables", svc.createPatronDeliverables)
	router.POST("/units/:id/finalize", svc.finalizeUnit)
	router.POST("/units/:id/import", svc.importGuestImages)

	router.POST("/units/:id/attach", svc.attachFile)
	router.GET("/units/:id/attachments/:file", svc.getAttachment)
	router.DELETE("/units/:id/attachments/:file", svc.deleteAttachment)

	router.POST("/masterfiles/:id/deaccession", svc.deaccessionMasterFile)
	router.POST("/masterfiles/:id/iiif", svc.updateMasterFileIIIF)
	router.POST("/masterfiles/:id/sensitive", svc.setMasterFileSensitive)
	router.DELETE("/masterfiles/:id/sensitive", svc.clearMasterFileSensitive)
	router.GET("/masterfiles/:id/full_resolution", svc.getFullResolutionJP2)
	router.POST("/masterfiles/:id/techmeta", svc.updateMasterFileTechMetadata)

	router.POST("/units/:id/masterfiles/add", svc.addMasterFiles)
	router.POST("/units/:id/masterfiles/delete", svc.deleteMasterFiles)
	router.POST("/units/:id/masterfiles/renumber", svc.renumberMasterFiles)
	router.POST("/units/:id/masterfiles/metadata", svc.assignMasterFileMetadata)
	router.POST("/units/:id/masterfiles/component", svc.assignMasterFileComponent)
	router.POST("/units/:id/masterfiles/replace", svc.replaceMasterFiles)
	router.POST("/units/:id/masterfiles/clone", svc.cloneMasterFiles)

	router.POST("/callbacks/:jid/ocr", svc.ocrDoneCallback)

	portStr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("INFO: start service v%s on port %s", version, portStr)
	log.Fatal(router.Run(portStr))
}

type scriptParams struct {
	ComputeID string                 `json:"computeID"`
	Name      string                 `json:"name"`
	Params    map[string]interface{} `json:"params"`
}

func (svc *ServiceContext) runScript(c *gin.Context) {
	var req scriptParams
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Printf("ERROR: unable to parse script request params: %s", err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if req.ComputeID == "" {
		log.Printf("ERROR: missing required computeID param")
		c.String(http.StatusBadRequest, "missing required computeID param")
		return
	}

	var submitUser staffMember
	err = svc.GDB.Where("computing_id=?", req.ComputeID).First(&submitUser).Error
	if err != nil {
		log.Printf("ERROR: user %s not found: %s", req.ComputeID, err.Error())
		c.String(http.StatusBadRequest, fmt.Sprintf("%s not found", req.ComputeID))
		return
	}

	if submitUser.Role != Admin {
		log.Printf("ERROR: scripts can only be submitted by admin users")
		c.String(http.StatusBadRequest, "you do not have permission to make this request")
		return
	}

	log.Printf("INFO: script request %+v", req)
	scripts := map[string]interface{}{
		"createBondLocations": svc.createBondLocations,
		"createBondUnits":     svc.createBondUnits,
		"ingestBondImages":    svc.ingestBondImages,
		"generateBondMapping": svc.generateBondMapping,
	}

	tgtScript := scripts[req.Name]
	if tgtScript == nil {
		log.Printf("ERROR: unrecognized script name %s", req.Name)
		c.String(http.StatusBadRequest, fmt.Sprintf("%s is not a known script", req.Name))
		return
	}

	js, err := svc.createJobStatus(req.Name, "StaffMember", submitUser.ID)
	if err != nil {
		log.Printf("ERROR: unable to create HathiTrush submission job status: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error()+"\n")
		return
	}

	err = tgtScript.(func(*gin.Context, *jobStatus, map[string]interface{}) error)(c, js, req.Params)
	if err != nil {
		svc.logFatal(js, err.Error())
		c.String(http.StatusBadRequest, err.Error()+"\n")
	}
}

// func (svc *ServiceContext) hack(c *gin.Context) {
// 	unitID := int64(49877)
// 	js, err := svc.createJobStatus("RepublishIIIF", "Unit", unitID)
// 	if err != nil {
// 		log.Printf("ERROR: unable to create job js: %s", err.Error())
// 		c.String(http.StatusInternalServerError, err.Error())
// 		return
// 	}

// 	var tgtUnit unit
// 	err = svc.GDB.Preload("Metadata").Preload("Metadata.OcrHint").
// 		Preload("Order").Preload("IntendedUse").First(&tgtUnit, unitID).Error
// 	if err != nil {
// 		svc.logFatal(js, fmt.Sprintf("Unable to load unit %d: %s", unitID, err.Error()))
// 		c.String(http.StatusBadRequest, err.Error())
// 		return
// 	}

// 	srcDir := path.Join(svc.ProcessingDir, "finalization", fmt.Sprintf("%09d", unitID))
// 	if pathExists(srcDir) == false {
// 		svc.setUnitFatal(js, &tgtUnit, fmt.Sprintf("Finalization directory %s does not exist.", srcDir))
// 		c.String(http.StatusBadRequest, err.Error())
// 		return
// 	}

// 	svc.logInfo(js, fmt.Sprintf("HACK: reimport all images for unit %d", unitID))
// 	err = svc.importImages(js, &tgtUnit, srcDir)
// 	if err != nil {
// 		svc.setUnitFatal(js, &tgtUnit, err.Error())
// 		c.String(http.StatusBadRequest, err.Error())
// 		return
// 	}

// 	c.String(http.StatusOK, "happy")
// }

// curl -X POST  https://dpg-jobs.lib.virginia.edu/hathitrust/package/submit -H "Content-Type: application/json" --data '{"computeID": "lf6f", "order": 11662, "barcodes": ["X031014505"]}'
