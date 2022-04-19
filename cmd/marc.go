package main

import (
	"encoding/xml"
	"fmt"
	"log"
)

type dataField struct {
	XMLName   xml.Name   `xml:"datafield"`
	Tag       string     `xml:"tag,attr"`
	Subfields []subField `xml:"subfield"`
}

type marcMetadata = struct {
	XMLName    xml.Name    `xml:"record"`
	Leader     string      `xml:"leader"`
	DataFields []dataField `xml:"datafield"`
}

func (svc *ServiceContext) getLocation(md *metadata) string {
	log.Printf("INFO: get location from marc for pid [%s] barcode [%s]", md.PID, md.Barcoode)
	parsed, err := svc.getMarcMetadata(md)
	if err != nil {
		log.Printf("ERROR: get marc metadata failed: %s", err.Error())
		return ""
	}
	location := ""
	for _, df := range parsed.DataFields {
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
		if location != "" {
			break
		}
	}
	return location
}

func (svc *ServiceContext) getMarcMetadata(md *metadata) (*marcMetadata, error) {
	log.Printf("INFO: get marc metadata for pid [%s] barcode [%s]", md.PID, md.Barcoode)
	out, err := svc.getRequest(fmt.Sprintf("%s/api/metadata/%s?type=marc", svc.TrackSysURL, md.PID))
	if err != nil {
		return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
	}

	var parsed marcMetadata
	parseErr := xml.Unmarshal(out, &parsed)
	if parseErr != nil {
		return nil, parseErr
	}
	return &parsed, nil
}

func (svc *ServiceContext) getCitation(md *metadata) string {
	log.Printf("INFO: get citation from marc for pid [%s] barcode [%s]", md.PID, md.Barcoode)

	parsed, err := svc.getMarcMetadata(md)
	if err != nil {
		log.Printf("ERROR: get marc metadata failed: %s", err.Error())
		return ""
	}

	citation := ""
	location := ""
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
