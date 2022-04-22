package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/SebastiaanKlippert/go-wkhtmltopdf"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func (svc *ServiceContext) viewOrderPDF(c *gin.Context) {
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)

	var o order
	err := svc.GDB.Preload("Customer").Preload("Customer.AcademicStatus").
		Preload("Invoices").Preload("Units").Preload("Units.IntendedUse").
		Preload("Units.Metadata").Preload("Units.MasterFiles").
		Preload("Units.MasterFiles.Component").Preload("Units.MasterFiles.Component.ComponentType").
		First(&o, orderID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("ERROR: order %d not found", orderID)
			c.String(http.StatusNotFound, fmt.Sprintf("order %d not found", orderID))
		} else {
			log.Printf("ERROR: unable to load order %d: %s", orderID, err.Error())
			c.String(http.StatusInternalServerError, err.Error())
		}
		return
	}

	pdfGen, err := svc.generateOrderPDF(&o)
	if err != nil {
		log.Printf("ERROR: generation of order pdf failed: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.Data(http.StatusOK, "application/pdf", pdfGen.Bytes())
}

func (svc *ServiceContext) createOrderPDF(c *gin.Context) {
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	js, err := svc.createJobStatus("CreateOrderPDF", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var o order
	err = svc.GDB.Preload("Customer").Preload("Customer.AcademicStatus").
		Preload("Invoices").Preload("Units").Preload("Units.IntendedUse").
		Preload("Units.Metadata").Preload("Units.MasterFiles").
		Preload("Units.MasterFiles.Component").Preload("Units.MasterFiles.Component.ComponentType").
		First(&o, orderID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			svc.logFatal(js, fmt.Sprintf("order %d not found", orderID))
			c.String(http.StatusNotFound, fmt.Sprintf("order %d not found", orderID))
		} else {
			svc.logFatal(js, fmt.Sprintf("unable to load order %d: %s", orderID, err.Error()))
			c.String(http.StatusInternalServerError, err.Error())
		}
		return
	}

	svc.logInfo(js, "Create order PDF...")
	pdfGen, err := svc.generateOrderPDF(&o)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to generate order PDF: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	err = svc.saveOrderPDF(js, &o, pdfGen)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to save order PDF: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) saveOrderPDF(js *jobStatus, o *order, pdfGen *wkhtmltopdf.PDFGenerator) error {

	dir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", o.ID))
	err := ensureDirExists(dir, 0777)
	if err != nil {
		return err
	}

	pdfFile := path.Join(dir, fmt.Sprintf("%d.pdf", o.ID))
	err = pdfGen.WriteFile(pdfFile)
	if err != nil {
		return err
	}

	svc.logInfo(js, fmt.Sprintf("PDF created at %s", pdfFile))
	return nil
}

func (svc *ServiceContext) generateOrderPDF(order *order) (*wkhtmltopdf.PDFGenerator, error) {
	pdfGen, err := wkhtmltopdf.NewPDFGenerator()
	if err != nil {
		return nil, err
	}

	pdfGen.PageSize.Set(wkhtmltopdf.PageSizeA4)
	pdfGen.MarginBottom.Set(10)
	pdfGen.MarginTop.Set(10)
	pdfGen.MarginLeft.Set(10)
	pdfGen.MarginRight.Set(10)

	type mfData struct {
		Filename    string
		Title       string
		Description string
		Even        bool
	}
	type containerData struct {
		ID    int64
		Type  string
		Name  string
		Date  string
		Files []mfData
	}
	type itemData struct {
		Number     int
		Title      string
		Author     string
		CallNumber string
		Citation   string
		Containers []containerData // item wil have Containers or Files, but not both
		Files      []mfData
	}
	type pdfData struct {
		FirstName   string
		LastName    string
		OrderID     int64
		DateOrdered string
		ItemCount   int
		Items       []itemData
	}
	data := pdfData{
		FirstName:   order.Customer.FirstName,
		LastName:    order.Customer.LastName,
		OrderID:     order.ID,
		ItemCount:   len(order.Units),
		DateOrdered: order.DateRequestSubmitted.Format("January 2, 2006"),
		Items:       make([]itemData, 0),
	}

	for idx, unit := range order.Units {
		item := itemData{Number: idx + 1,
			Title:      unit.Metadata.Title,
			Author:     unit.Metadata.CreatorName,
			Files:      make([]mfData, 0),
			Containers: make([]containerData, 0),
		}
		if unit.Metadata.Type == "SirsiMetadata" {
			item.CallNumber = unit.Metadata.CallNumber
			item.Citation = svc.getCitation(unit.Metadata)
		}
		var currContainer *containerData
		var component *component
		if unit.MasterFiles[0].ComponentID != nil {
			component = unit.MasterFiles[0].Component
		}
		for mfIdx, mf := range unit.MasterFiles {
			m := mfData{Title: mf.Title, Description: mf.Description, Filename: mf.Filename}
			if mfIdx%2 != 0 {
				m.Even = true
			}
			if component == nil {
				item.Files = append(item.Files, m)
			} else {
				if mf.ComponentID != nil && (currContainer == nil || currContainer.ID != *mf.ComponentID) {
					if currContainer != nil {
						item.Containers = append(item.Containers, *currContainer)
					}
					newContainer := containerData{ID: *mf.ComponentID, Type: component.Type(), Name: component.Name(),
						Date: mf.Component.Date, Files: make([]mfData, 0)}
					currContainer = &newContainer
				}
				currContainer.Files = append(currContainer.Files, m)
			}
		}
		if currContainer != nil && len(currContainer.Files) > 0 {
			item.Containers = append(item.Containers, *currContainer)
		}

		data.Items = append(data.Items, item)
	}

	var content bytes.Buffer
	err = svc.Templates.PDFOrderSummary.Execute(&content, data)
	if err != nil {
		return nil, err
	}
	page := wkhtmltopdf.NewPageReader(strings.NewReader(content.String()))
	page.FooterRight.Set("[page]")
	page.FooterFontSize.Set(10)
	pdfGen.AddPage(page)

	err = pdfGen.Create()
	if err != nil {
		return nil, err
	}

	return pdfGen, nil
}

type subField struct {
	XMLName xml.Name `xml:"subfield"`
	Code    string   `xml:"code,attr"`
	Value   string   `xml:",chardata"`
}
