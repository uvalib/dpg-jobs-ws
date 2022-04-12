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
	"time"

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
		Preload("Units.Metadata").Preload("Units.MasterFiles").First(&o, orderID).Error
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
		Preload("Units.Metadata").Preload("Units.MasterFiles").First(&o, orderID).Error
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

	err = svc.createPDFDeliverable(js, &o)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to generate PDF: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) createOrderEmail(c *gin.Context) {
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	js, err := svc.createJobStatus("CreateOrderEmail", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	var o order
	err = svc.GDB.Preload("Customer").Preload("Customer.AcademicStatus").
		Preload("Invoices").Preload("Units").Preload("Units.IntendedUse").
		Preload("Units.Metadata").Preload("Units.MasterFiles").First(&o, orderID).Error
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

	err = svc.generateOrderEmail(js, &o)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to generate email: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) checkOrderReady(c *gin.Context) {
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	js, err := svc.createJobStatus("CheckOrderReadyForDelivery", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Start CheckOrderReadyForDelivery for order %d", orderID))

	var o order
	err = svc.GDB.Preload("Customer").Preload("Customer.AcademicStatus").
		Preload("Invoices").Preload("Units").Preload("Units.IntendedUse").
		Preload("Units.Metadata").Preload("Units.MasterFiles").First(&o, orderID).Error
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

	if o.DateCustomerNotified != nil {
		svc.logError(js, "The date_customer_notified field on this order is filled out.  The order appears to have been delivered already.")
		c.String(http.StatusOK, "done")
		return
	}

	incomplete := make([]int64, 0)
	svc.logInfo(js, "Checking units for completeness...")
	for _, unit := range o.Units {
		// If an order can have both patron and dl-only units (i.e. some units have an intended use of "Digital Collection Building")
		// then we have to remove from consideration those units whose intended use is "Digital Collection Building"
		// and consider all other units.
		svc.logInfo(js, fmt.Sprintf("   Check unit %d", unit.ID))
		if unit.IntendedUseID != 110 {
			if unit.UnitStatus != "canceled" {
				if unit.DatePatronDeliverablesReady == nil {
					svc.logInfo(js, fmt.Sprintf("   Unit %d incomplete", unit.ID))
					incomplete = append(incomplete, unit.ID)
				} else {
					svc.logInfo(js, fmt.Sprintf("   Unit %d COMPLETE", unit.ID))
				}
			} else {
				svc.logInfo(js, "   unit is canceled")
			}
		} else {
			svc.logInfo(js, "   unit is for digital collection building")
		}
	}

	svc.logInfo(js, fmt.Sprintf("Incomplete units count %d", len(incomplete)))

	// If any units are not comlete, the order is incomplete
	if len(incomplete) > 0 {
		svc.logInfo(js, fmt.Sprintf("Order is incomplete with units %v still unfinished", incomplete))
		svc.jobDone(js)
		c.String(http.StatusOK, "done")
		return
	}

	// The 'patron' units within the order are complete, and customer not yet notified
	// Flag deliverable complete data and begin order QA process that will result
	// in a PDF ad patron email being generated if all is good
	now := time.Now()
	svc.logInfo(js, "All units in order are complete and will now begin the delivery process.")
	svc.GDB.Model(&o).Select("date_patron_deliverables_complete").Updates(order{DatePatronDeliverablesComplete: &now})

	svc.logInfo(js, "QA order status and fees...")
	if o.OrderStatus != "approved" {
		svc.logFatal(js, "Order does not have an order status of 'approved'.  Please correct before proceeding.")
		c.String(http.StatusBadRequest, "failed")
		return
	}

	//  An order whose customer is non-UVA and whose actual fee is blank is invalid.
	if o.Customer.AcademicStatusID == 1 && o.Fee.Valid == false {
		svc.logFatal(js, "Order has a non-UVA customer and the fee is blank.")
		c.String(http.StatusBadRequest, "failed")
		return
	}

	// If there is a value for order fee then there must be a paid invoice
	if o.Fee.Valid && o.Fee.Float64 > 0 {
		if feePaid(&o) == false {
			svc.logFatal(js, "Order has an unpaid fee.")
		} else {
			svc.logInfo(js, "Order fee paid.")
		}
	} else {
		svc.logInfo(js, "Order has no fees associated with it.")
	}

	svc.logInfo(js, "Order has passed QA")
	err = svc.createPDFDeliverable(js, &o)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to generate PDF: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	err = svc.generateOrderEmail(js, &o)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to generate email: %s", err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) createPDFDeliverable(js *jobStatus, o *order) error {
	svc.logInfo(js, "Create order PDF...")
	pdfGen, err := svc.generateOrderPDF(o)
	if err != nil {
		return err
	}
	dir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", o.ID))
	err = ensureDirExists(dir, 0777)
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
	type itemData struct {
		Number     int
		Title      string
		Author     string
		CallNumber string
		Citation   string
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
			Title:  unit.Metadata.Title,
			Author: unit.Metadata.CreatorName,
			Files:  make([]mfData, 0),
		}
		if unit.Metadata.Type == "SirsiMetadata" {
			item.CallNumber = unit.Metadata.CallNumber
			item.Citation = svc.getCitation(unit.Metadata)
		}
		for mfIdx, mf := range unit.MasterFiles {
			m := mfData{Title: mf.Title, Description: mf.Description, Filename: mf.Filename}
			if mfIdx%2 != 0 {
				m.Even = true
			}
			item.Files = append(item.Files, m)
		}

		data.Items = append(data.Items, item)
	}

	var content bytes.Buffer
	err = svc.Templates.PDFOrderSummary.Execute(&content, data)
	if err != nil {
		return nil, err
	}
	page := wkhtmltopdf.NewPageReader(strings.NewReader(content.String()))
	page.DisableExternalLinks.Set(false)
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

type dataField struct {
	XMLName   xml.Name   `xml:"datafield"`
	Tag       string     `xml:"tag,attr"`
	Subfields []subField `xml:"subfield"`
}

type mods = struct {
	XMLName    xml.Name    `xml:"record"`
	Leader     string      `xml:"leader"`
	DataFields []dataField `xml:"datafield"`
}

func (svc *ServiceContext) getCitation(md *metadata) string {
	log.Printf("INFO: get citation from marc for pid [%s] barcode [%s]", md.PID, md.Barcoode)
	citation := ""
	location := ""
	out, err := svc.getRequest(fmt.Sprintf("%s/api/metadata/%s?type=marc", svc.TrackSysURL, md.PID))
	if err != nil {
		log.Printf("ERROR: unable to get marc for pid [%s] barcode [%s]: %s", md.PID, md.Barcoode, err.Message)
	} else {
		var parsed mods
		parseErr := xml.Unmarshal(out, &parsed)
		if parseErr != nil {
			log.Printf("ERROR: unable to parse marc response for %s: %s", md.PID, parseErr.Error())
		} else {
			for _, df := range parsed.DataFields {
				// log.Printf("DF %s", df.Tag)
				if df.Tag == "524" {
					for _, sf := range df.Subfields {
						if sf.Code == "a" {
							citation = sf.Value
							break
						}
					}
				}
				if df.Tag == "999" {
					// MARC 999 is repeated, once per barcdode. Barcode stored in 'i'
					// pick the data that matches the target barcode and grab location from 'l'
					barcodeMatch := false
					for _, sf := range df.Subfields {
						if sf.Code == "i" && sf.Value == md.Barcoode {
							barcodeMatch = true
						}
						if sf.Code == "l" && barcodeMatch {
							location = sf.Value
							break
						}
					}
				}
				if citation != "" {
					break
				}
			}
		}
	}

	if citation == "" {
		if md.Title != "" {
			citation = md.Title + ". "
		}
		if md.CallNumber != "" {
			citation += md.CallNumber + ". "
		}
		if location != "" {
			if val, ok := VirgoLocations[location]; ok {
				citation += val
			} else {
				citation += "Special Collections, University of Virginia, Charlottesville, VA"
			}
		} else {
			citation += "Special Collections, University of Virginia, Charlottesville, VA"
		}
	}

	return citation
}

func feePaid(order *order) bool {
	if len(order.Invoices) == 0 {
		return false
	}
	for _, inv := range order.Invoices {
		if inv.DateFeePaid != nil {
			return true
		}
	}
	return false
}
