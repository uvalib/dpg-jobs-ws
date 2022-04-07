package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/johnfercher/maroto/pkg/color"
	"github.com/johnfercher/maroto/pkg/consts"
	"github.com/johnfercher/maroto/pkg/pdf"
	"github.com/johnfercher/maroto/pkg/props"
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

	m := svc.generateOrderPDF(&o)
	pdf, pErr := m.Output()
	if pErr != nil {
		log.Printf("ERROR: unable to generate PDF data: %s", pErr.Error())
		c.String(http.StatusInternalServerError, pErr.Error())
		return
	}
	c.Data(http.StatusOK, "application/pdf", pdf.Bytes())
}

func (svc *ServiceContext) createOrderPDF(c *gin.Context) {
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	js, err := svc.createJobStatus("CreateOrderPDF", "Order", orderID)

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

	svc.logInfo(js, "Create order PDF...")
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
		log.Printf("ERROR: unable to create check order job js: %s", err.Error())
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
		log.Printf("ERROR: unable to create check order job js: %s", err.Error())
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
	m := svc.generateOrderPDF(o)
	dir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", o.ID))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Printf("INFO: create pdf output directory %s", dir)
		err := os.Mkdir(dir, 0777)
		if err != nil {
			return err
		}
	}
	pdfFile := path.Join(dir, fmt.Sprintf("%d.pdf", o.ID))
	err := m.OutputFileAndClose(pdfFile)
	if err != nil {
		return err
	}

	svc.logInfo(js, fmt.Sprintf("PDF created at %s", pdfFile))
	return nil
}

func (svc *ServiceContext) generateOrderPDF(order *order) pdf.Maroto {
	ltGrey := color.Color{
		Red:   220,
		Green: 220,
		Blue:  220,
	}
	m := pdf.NewMaroto(consts.Portrait, consts.A4)
	m.SetPageMargins(10, 10, 10)
	m.SetAliasNbPages("{nb}")
	m.SetFirstPageNb(1)
	m.RegisterFooter(func() {
		m.Row(10, func() {
			m.Col(12, func() {
				m.Text("page "+strconv.Itoa(m.GetCurrentPage())+" of {nb}", props.Text{
					Align: consts.Right,
					Size:  8,
				})
			})
		})
	})

	// define the PDF heading
	m.Row(15, func() {
		m.Col(12, func() { // 12 means full width (its a column count and PDF grid has 12 cells)
			m.FileImage("./assets/lib_letterhead.jpg", props.Rect{
				Center:  true,
				Percent: 75,
			})
		})
	})

	addPDFTextRow(m, "Digital Production Group, University of Virginia Library", 5, consts.Center)
	addPDFTextRow(m, "Post Office Box 400155, Charlottesville, Virginia 22904 U.S.A.", 5, consts.Center)
	addPDFTextRow(m, fmt.Sprintf("Order ID: %d", order.ID), 15, consts.Right)

	// define the message to the user
	addPDFTextRow(m, fmt.Sprintf("Dear %s %s, ", order.Customer.FirstName, order.Customer.LastName), 10, consts.Left)
	msg := fmt.Sprintf("On %s you placed an order with the Digital Production Group ", order.DateRequestSubmitted.Format("January 2, 2006"))
	msg += fmt.Sprintf("of the University of Virginia, Charlottesville, VA. Your request comprised %d items. ", len(order.Units))
	msg += "Below you will find a description of your digital order and how to cite the material for publication."
	addPDFTextRow(m, msg, 18, consts.Left)
	addPDFTextRow(m, "Sincerely,", 5, consts.Left)
	addPDFTextRow(m, "Digital Production Group Staff", 5, consts.Left)
	m.AddPage()

	// the tables of masterfiles
	addPDFTextRow(m, "Digital Order Summary", 5, consts.Center)

	tableHeader := []string{"Filename", "Title", "Description"}
	for idx, unit := range order.Units {
		addPDFTextRow(m, "", 5, consts.Left)
		addPDFLine(m)
		addPDFTextRow(m, fmt.Sprintf("Item #%d", idx+1), 5, consts.Right)
		addPDFLabeledTextRow(m, "Title:", unit.Metadata.Title)
		if unit.Metadata.CreatorName != "" {
			addPDFLabeledTextRow(m, "Author:", unit.Metadata.CreatorName)
		}
		if unit.Metadata.Type == "SirsiMetadata" {
			addPDFLabeledTextRow(m, "Call Number:", unit.Metadata.CallNumber)
			citation := svc.getCitation(&unit.Metadata)
			if citation != "" {
				log.Printf(citation)
			}
		}

		content := [][]string{}
		for _, mf := range unit.MasterFiles {
			content = append(content, []string{mf.Filename, mf.Title, mf.Description})
		}

		addPDFTextRow(m, "", 3, consts.Left)
		m.TableList(tableHeader, content, props.TableList{
			HeaderProp: props.TableListContent{
				Size: 10,
			},
			AlternatedBackground: &ltGrey,
			Align:                consts.Left,
			Line:                 false,
			HeaderContentSpace:   1,
		})
	}

	return m
}

func addPDFLine(m pdf.Maroto) {
	m.Row(10, func() {
		m.Col(12, func() {
			m.Line(1.0, props.Line{
				Style: consts.Solid,
			})
		})
	})
}

func addPDFTextRow(m pdf.Maroto, text string, rowHeight float64, align consts.Align) {
	m.Row(rowHeight, func() {
		m.Col(12, func() {
			m.Text(text, props.Text{
				Size:  10,
				Style: consts.Normal,
				Align: align,
			})
		})
	})
}

func addPDFLabeledTextRow(m pdf.Maroto, label string, text string) {
	m.Row(5, func() {
		m.Col(2, func() {
			m.Text(label, props.Text{
				Style: consts.Bold,
				Align: consts.Left,
			})
		})
		m.Col(10, func() {
			m.Text(text, props.Text{
				Style: consts.Normal,
				Align: consts.Left,
			})
		})
	})
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

func (svc *ServiceContext) generateOrderEmail(js *jobStatus, o *order) error {
	svc.logInfo(js, "Create email for order")
	type MailData struct {
		FirstName     string
		LastName      string
		Fee           *float64
		DatePaid      string
		DeliveryFiles []string
	}
	data := MailData{
		FirstName:     o.Customer.FirstName,
		LastName:      o.Customer.LastName,
		DeliveryFiles: make([]string, 0),
	}
	if o.Fee.Valid {
		data.Fee = &o.Fee.Float64
		for _, inv := range o.Invoices {
			if inv.DateFeePaid != nil {
				data.DatePaid = inv.DateFeePaid.Format("2006-01-02")
				break
			}
		}
	}
	deliveryDir := path.Join(svc.DeliveryDir, fmt.Sprintf("order_%d", o.ID))
	data.DeliveryFiles = append(data.DeliveryFiles, fmt.Sprintf("http://digiservdelivery.lib.virginia.edu/order_%d/%d.pdf", o.ID, o.ID))
	files, err := ioutil.ReadDir(deliveryDir)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to get deliverable zip file list: %s", err.Error()))
	} else {
		// strip off the full path; only add: order_dir/file.zip
		for _, fi := range files {
			if strings.Index(fi.Name(), ".zip") > 0 {
				data.DeliveryFiles = append(data.DeliveryFiles, fmt.Sprintf("http://digiservdelivery.lib.virginia.edu/order_%d/%s", o.ID, fi.Name()))
			}
		}
	}

	var renderedEmail bytes.Buffer
	tpl, err := template.New("order.html").ParseFiles("./templates/order.html")
	if err != nil {
		return err
	}
	err = tpl.Execute(&renderedEmail, data)
	if err != nil {
		return err
	}

	log.Printf(renderedEmail.String())

	svc.GDB.Model(o).Select("email").Updates(order{Email: renderedEmail.String()})
	svc.logInfo(js, "An email for web delivery has been created")

	return nil
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
