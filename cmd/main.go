package main

import (
	"fmt"
	"log"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

// Version of the service
const version = "1.3.0"

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

	router.GET("/archivesspace/lookup", svc.archivesSpaceMiddleware, svc.lookupArchivesSpaceURL)
	router.GET("/archivesspace/validate", svc.archivesSpaceMiddleware, svc.validateArchivesSpaceURL)
	router.POST("/archivesspace/convert", svc.archivesSpaceMiddleware, svc.convertToArchivesSpace)
	router.POST("/archivesspace/publish", svc.archivesSpaceMiddleware, svc.publishToArchivesSpace)

	router.GET("/jstor/lookup", svc.lookupJstorMetadata)

	router.POST("/iiif/publish", svc.publishMasterFileToIIIF)

	router.POST("/metadata/:id/baggit", svc.createBag)
	router.POST("/metadata/:id/publish", svc.publishToVirgo)

	router.GET("/ocr/languages", svc.getOCRLanguages)
	router.POST("/ocr", svc.handleOCRRequest)

	router.POST("/orders/:id/check", svc.checkOrderReady)
	router.POST("/orders/:id/pdf", svc.createOrderPDF)
	router.GET("/orders/:id/pdf", svc.viewOrderPDF)
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

	router.POST("/units/:id/masterfiles/add", svc.addMasterFiles)
	router.POST("/units/:id/masterfiles/delete", svc.deleteMasterFiles)
	router.POST("/units/:id/masterfiles/renumber", svc.renumberMasterFiles)
	router.POST("/units/:id/masterfiles/replace", svc.replaceMasterFiles)
	router.POST("/units/:id/masterfiles/clone", svc.cloneMasterFiles)

	router.POST("/callbacks/:jid/ocr", svc.ocrDoneCallback)

	portStr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("INFO: start service v%s on port %s", version, portStr)
	log.Fatal(router.Run(portStr))
}
