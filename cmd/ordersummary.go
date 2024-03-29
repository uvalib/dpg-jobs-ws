package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func (svc *ServiceContext) viewOrderSummary(c *gin.Context) {
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

	pdfBytes, err := svc.generateOrderSummary(&o)
	if err != nil {
		log.Printf("ERROR: generation of order pdf failed: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.Data(http.StatusOK, "text/html", pdfBytes)
}

func (svc *ServiceContext) createOrderSummary(c *gin.Context) {
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
	pdfGen, err := svc.generateOrderSummary(&o)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to generate order PDF: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	err = svc.saveOrderSummary(js, &o, pdfGen)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to save order PDF: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) saveOrderSummary(js *jobStatus, o *order, orderData []byte) error {

	dir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", o.ID))
	err := ensureDirExists(dir, 0777)
	if err != nil {
		return err
	}

	summaryFileName := path.Join(dir, "summary.html")
	err = os.WriteFile(summaryFileName, orderData, 0755)
	if err != nil {
		return err
	}

	svc.logInfo(js, fmt.Sprintf("HTML order summary created at %s", summaryFileName))
	return nil
}

func (svc *ServiceContext) generateOrderSummary(order *order) ([]byte, error) {
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
		if len(unit.MasterFiles) == 0 {
			continue
		}
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
	err := svc.Templates.OrderSummary.Execute(&content, data)
	if err != nil {
		return nil, err
	}

	return content.Bytes(), nil
}

type subField struct {
	XMLName xml.Name `xml:"subfield"`
	Code    string   `xml:"code,attr"`
	Value   string   `xml:",chardata"`
}
