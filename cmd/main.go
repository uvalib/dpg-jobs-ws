package main

import (
	"fmt"
	"log"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

// Version of the service
const version = "0.1.0"

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

	router.POST("/orders/:id/check", svc.checkOrderReady)
	router.POST("/orders/:id/pdf", svc.createOrderPDF)
	router.GET("/orders/:id/pdf", svc.viewOrderPDF)
	router.POST("/orders/:id/email", svc.createOrderEmail)
	router.POST("/orders/:id/email/send", svc.sendOrderEmail)
	router.POST("/orders/:id/fees", svc.sendFeesEmail)

	portStr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("INFO: start service v%s on port %s", version, portStr)
	log.Fatal(router.Run(portStr))
}
