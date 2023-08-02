package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

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

	err = svc.checkOrderReadyForDelivery(js, orderID)
	if err != nil {
		svc.logFatal(js, err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	svc.jobDone(js)
	c.String(http.StatusOK, "done")
}

func (svc *ServiceContext) checkOrderReadyForDelivery(js *jobStatus, orderID int64) error {
	svc.logInfo(js, fmt.Sprintf("Start CheckOrderReadyForDelivery for order %d", orderID))

	var o order
	err := svc.GDB.Preload("Customer").Preload("Customer.AcademicStatus").
		Preload("Invoices").Preload("Units").Preload("Units.IntendedUse").
		Preload("Units.Metadata").Preload("Units.MasterFiles").
		Preload("Units.MasterFiles.Component").
		Preload("Units.MasterFiles.Component.ComponentType").First(&o, orderID).Error
	if err != nil {
		return fmt.Errorf("unable to load order %d: %s", orderID, err.Error())
	}

	if o.DateCustomerNotified != nil {
		svc.logError(js, "The date_customer_notified field on this order is filled out.  The order appears to have been delivered already.")
		return nil
	}

	incomplete := make([]int64, 0)
	svc.logInfo(js, "Checking units for completeness...")
	for _, unit := range o.Units {
		// If an order can have both patron and dl-only units (i.e. some units have an intended use of "Digital Collection Building")
		// then we have to remove from consideration those units whose intended use is "Digital Collection Building"
		// and consider all other units.
		svc.logInfo(js, fmt.Sprintf("   Check unit %d", unit.ID))
		if *unit.IntendedUseID != 110 {
			if unit.UnitStatus != "canceled" {
				if unit.DatePatronDeliverablesReady == nil {
					svc.logInfo(js, fmt.Sprintf("   Unit %d incomplete", unit.ID))
					incomplete = append(incomplete, unit.ID)
				} else {
					svc.logInfo(js, fmt.Sprintf("   Unit %d COMPLETE", unit.ID))
					if unit.UnitStatus != "done" {
						unit.UnitStatus = "done"
						svc.GDB.Model(&unit).Select("UnitStatus").Updates(unit)
					}
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
		return nil
	}

	// The 'patron' units within the order are complete, and customer not yet notified
	// Flag deliverable complete data and begin order QA process that will result
	// in a PDF ad patron email being generated if all is good
	now := time.Now()
	svc.logInfo(js, "All units in order are complete and will now begin the delivery process.")
	svc.GDB.Model(&o).Select("date_patron_deliverables_complete").Updates(order{DatePatronDeliverablesComplete: &now})

	svc.logInfo(js, "QA order status and fees...")
	if o.OrderStatus != "approved" {
		return fmt.Errorf("Order does not have an order status of 'approved'")
	}

	//  An order whose customer is non-UVA must validate fee info:
	if o.Customer.AcademicStatusID == 1 {
		if o.FeeWaived == false {
			if o.Fee == nil {
				return fmt.Errorf("Order has a non-UVA customer and the fee is blank")
			}

			// If there is a value for order fee then there must be a paid invoice
			if feePaid(&o) == false {
				return fmt.Errorf("order has an unpaid fee")
			}
			svc.logInfo(js, "Order fee paid.")
		} else {
			svc.logInfo(js, "The fee has been waived for this order.")
		}
	}

	svc.logInfo(js, "Order has passed QA")

	svc.logInfo(js, "Create order PDF...")
	pdfGen, err := svc.generateOrderPDF(&o)
	if err != nil {
		return fmt.Errorf("Unable to generate order PDF: %s", err.Error())
	}

	err = svc.saveOrderPDF(js, &o, pdfGen)
	if err != nil {
		return fmt.Errorf("Unable to save order PDF: %s", err.Error())
	}

	err = svc.generateOrderEmail(js, &o)
	if err != nil {
		return fmt.Errorf("Unable to generate email: %s", err.Error())
	}
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
