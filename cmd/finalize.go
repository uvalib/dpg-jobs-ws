package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (svc *ServiceContext) finalizeUnit(c *gin.Context) {
	c.String(http.StatusNotImplemented, "not implemenetd")
}
