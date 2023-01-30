package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gopkg.in/gomail.v2"
	"gorm.io/gorm"
)

type emailRequest struct {
	Subject string
	To      []string
	ReplyTo string
	CC      string
	From    string
	Body    string
}

func (svc *ServiceContext) sendOrderEmail(c *gin.Context) {
	altEmail := c.Query("alt")
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	js, err := svc.createJobStatus("SendOrderEmail", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, "Start send order email...")
	svc.logInfo(js, fmt.Sprintf("Loading customer and order data for order %d", orderID))
	var o order
	err = svc.GDB.Preload("Customer").First(&o, orderID).Error
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

	tgtEmail := o.Customer.Email
	if altEmail != "" {
		tgtEmail = altEmail
	}
	svc.logInfo(js, fmt.Sprintf("Sending order email to  %s", tgtEmail))
	req := emailRequest{Subject: fmt.Sprintf("UVA Digital Production Group - Order # %d Complete", o.ID),
		To:      []string{tgtEmail},
		From:    svc.SMTP.Sender,
		ReplyTo: svc.SMTP.Sender,
		Body:    o.Email,
	}
	err = svc.sendEmail(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to send order email: %s", err.Error()))
		return
	}

	now := time.Now()
	o.DateCustomerNotified = &now
	if altEmail != "" {
		msg := fmt.Sprintf("Order notification sent to alternate email address: %s on %s.", altEmail, now.Format("2006-01-02"))
		if o.StaffNotes != "" {
			o.StaffNotes += " "
		}
		o.StaffNotes += msg
	}
	err = svc.GDB.Model(&o).Select("DateCustomerNotified", "StaffNotes").Updates(o).Error
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to set date customer notified: %s", err.Error()))
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) sendFeesEmail(c *gin.Context) {
	resend := (c.Query("resend") != "")
	altEmail := c.Query("alt")
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	staffIDStr := c.Query("staff")
	js, err := svc.createJobStatus("SendFeeEstimateToCustomer", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	svc.logInfo(js, fmt.Sprintf("Start send fees email from staff id %s...", staffIDStr))
	svc.logInfo(js, fmt.Sprintf("Loading customer and invoice data for order %d", orderID))
	var o order
	err = svc.GDB.Preload("Customer").Preload("Invoices").First(&o, orderID).Error
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

	type feeData struct {
		FirstName string
		LastName  string
		Fee       float64
	}
	svc.logInfo(js, "Rendering fees email")
	data := feeData{FirstName: o.Customer.FirstName, LastName: o.Customer.LastName, Fee: o.Fee.Float64}
	var renderedEmail bytes.Buffer
	err = svc.Templates.Fees.Execute(&renderedEmail, data)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("unable to render fees email for order %d: %s", orderID, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
	}

	tgtEmail := o.Customer.Email
	if altEmail != "" {
		tgtEmail = altEmail
	}
	svc.logInfo(js, fmt.Sprintf("Sending fees email to  %s", tgtEmail))
	req := emailRequest{Subject: fmt.Sprintf("UVA Digital Production Group - Request # %d Estimated Fee", o.ID),
		To:      []string{tgtEmail},
		From:    svc.SMTP.Sender,
		ReplyTo: svc.SMTP.Sender,
		Body:    renderedEmail.String(),
	}
	err = svc.sendEmail(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to send fees email: %s", err.Error()))
		return
	}
	svc.logInfo(js, "Fee estimate email sent to customer.")
	now := time.Now()

	// If an invoice does not yet exist for this order, create one
	if len(o.Invoices) == 0 && resend == false {
		inv := invoice{OrderID: o.ID, DateInvoice: time.Now()}
		err = svc.GDB.Create(&inv).Error
		if err != nil {
			svc.logError(js, fmt.Sprintf("Unable to create invoice: %s", err.Error()))
		}
		svc.logInfo(js, "A new invoice has been created")
	} else {
		svc.logInfo(js, "An invoice already exists for this order; not creating another.")
	}

	if resend == false {
		if o.OrderStatus != "await_fee" {
			svc.GDB.Model(o).Select("date_fee_estimate_sent_to_customer", "order_status").
				Updates(order{DateFeeEstimateSentToCustomer: &now, OrderStatus: "await_fee"})
		}

		svc.logInfo(js, "Order status and date fee estimate sent to customer have been updated.")
	}
	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) sendEmail(request *emailRequest) error {
	mail := gomail.NewMessage()
	mail.SetHeader("Subject", request.Subject)
	mail.SetHeader("To", request.To...)
	mail.SetHeader("From", request.From)
	if request.ReplyTo != "" {
		mail.SetHeader("Reply-To", request.ReplyTo)
	}
	if len(request.CC) > 0 {
		mail.SetHeader("Cc", request.CC)
	}
	mail.SetBody("text/html", request.Body)

	if svc.SMTP.FakeSMTP {
		log.Printf("Email is in dev mode. Logging message instead of sending")
		log.Printf("==================================================")
		mail.WriteTo(log.Writer())
		log.Printf("==================================================")
		return nil
	}

	log.Printf("Sending %s email to %s", request.Subject, strings.Join(request.To, ","))
	if svc.SMTP.Pass != "" {
		dialer := gomail.Dialer{Host: svc.SMTP.Host, Port: svc.SMTP.Port, Username: svc.SMTP.User, Password: svc.SMTP.Pass}
		dialer.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		return dialer.DialAndSend(mail)
	}

	log.Printf("Sending email with no auth")
	dialer := gomail.Dialer{Host: svc.SMTP.Host, Port: svc.SMTP.Port}
	dialer.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	return dialer.DialAndSend(mail)
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
	data.DeliveryFiles = append(data.DeliveryFiles, fmt.Sprintf("https://digiservdelivery.lib.virginia.edu/order_%d/%d.pdf", o.ID, o.ID))
	files, err := ioutil.ReadDir(deliveryDir)
	if err != nil {
		svc.logError(js, fmt.Sprintf("Unable to get deliverable zip file list: %s", err.Error()))
	} else {
		// strip off the full path; only add: order_dir/file.zip
		for _, fi := range files {
			if strings.Index(fi.Name(), ".zip") > 0 {
				data.DeliveryFiles = append(data.DeliveryFiles, fmt.Sprintf("https://digiservdelivery.lib.virginia.edu/order_%d/%s", o.ID, fi.Name()))
			}
		}
	}

	var renderedEmail bytes.Buffer
	err = svc.Templates.OrderAvailable.Execute(&renderedEmail, data)
	if err != nil {
		return err
	}

	log.Printf(renderedEmail.String())

	svc.GDB.Model(o).Select("email").Updates(order{Email: renderedEmail.String()})
	svc.logInfo(js, "An email for web delivery has been created")

	return nil
}
