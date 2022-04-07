package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"text/template"

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
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	js, err := svc.createJobStatus("SendOrderEmail", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	svc.logInfo(js, "Start send order email...")
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

	req := emailRequest{Subject: fmt.Sprintf("UVA Digital Production Group - Order # %d Complete", o.ID),
		To:      []string{o.Customer.Email},
		From:    svc.SMTP.Sender,
		ReplyTo: svc.SMTP.Sender,
		Body:    o.Email,
	}
	err = svc.sendEmail(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to send order email: %s", err.Error()))
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) sendFeesEmail(c *gin.Context) {
	orderIDStr := c.Param("id")
	orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
	js, err := svc.createJobStatus("SendFeeEstimateToCustomer", "Order", orderID)
	if err != nil {
		log.Printf("ERROR: unable to create job js: %s", err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	svc.logInfo(js, "Start send fees email...")
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

	type feeData struct {
		FirstName string
		LastName  string
		Fee       float64
	}
	data := feeData{FirstName: o.Customer.FirstName, LastName: o.Customer.LastName, Fee: o.Fee.Float64}
	var renderedEmail bytes.Buffer
	tpl, err := template.New("fees.html").ParseFiles("./templates/fees.html")
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("unable to load fees email template for order %d: %s", orderID, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
	}
	err = tpl.Execute(&renderedEmail, data)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("unable to render fees email for order %d: %s", orderID, err.Error()))
		c.String(http.StatusInternalServerError, err.Error())
	}

	req := emailRequest{Subject: fmt.Sprintf("UVA Digital Production Group - Request # %d Estimated Fee", o.ID),
		To:      []string{o.Customer.Email},
		From:    svc.SMTP.Sender,
		ReplyTo: svc.SMTP.Sender,
		Body:    o.Email,
	}
	err = svc.sendEmail(&req)
	if err != nil {
		svc.logFatal(js, fmt.Sprintf("Unable to send fees email: %s", err.Error()))
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) sendEmail(request *emailRequest) error {
	mail := gomail.NewMessage()
	mail.SetHeader("MIME-version", "1.0")
	mail.SetHeader("Content-Type", "text/html; charset=\"UTF-8\"")
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
