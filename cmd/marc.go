package main

import (
	"encoding/xml"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

type dataField struct {
	XMLName   xml.Name   `xml:"datafield"`
	Tag       string     `xml:"tag,attr"`
	Subfields []subField `xml:"subfield"`
}

type marcRecord struct {
	XMLName    xml.Name    `xml:"record"`
	Leader     string      `xml:"leader"`
	DataFields []dataField `xml:"datafield"`
}

type marcMetadata struct {
	XMLName xml.Name `xml:"collection"`
	Record  marcRecord
}

func (svc *ServiceContext) getMarcPublicationYear(md *metadata) int {
	log.Printf("INFO: get publication date from marc for pid [%s] barcode [%s]", md.PID, md.Barcode)
	parsed, err := svc.getMarcMetadata(md)
	if err != nil {
		log.Printf("ERROR: get marc metadata failed: %s", err.Error())
		return 0
	}

	year := ""
	for _, df := range parsed.Record.DataFields {
		if df.Tag == "260" {
			for _, sf := range df.Subfields {
				if sf.Code == "c" {
					raw := sf.Value
					year = extractYearFrom260c(raw)
				}
			}
		}
		if year != "" {
			break
		}
	}
	intYear, _ := strconv.Atoi(year)
	return intYear
}

func (svc *ServiceContext) getMarcLocation(md *metadata) string {
	log.Printf("INFO: get location from marc for pid [%s] barcode [%s]", md.PID, md.Barcode)
	parsed, err := svc.getMarcMetadata(md)
	if err != nil {
		log.Printf("ERROR: get marc metadata failed: %s", err.Error())
		return ""
	}
	location := ""
	for _, df := range parsed.Record.DataFields {
		if df.Tag == "999" {
			// MARC 999 is repeated, once per barcdode. Barcode stored in 'i'
			// pick the data that matches the target barcode and grab location from 'l'
			barcodeMatch := false
			for _, sf := range df.Subfields {
				if sf.Code == "i" && sf.Value == md.Barcode {
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
	log.Printf("INFO: get marc metadata for pid [%s] barcode [%s]", md.PID, md.Barcode)
	out, err := svc.getRequest(fmt.Sprintf("%s/api/metadata/%s?type=marc", svc.TrackSys.API, md.PID))
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

func (svc *ServiceContext) getModsMetadata(md *metadata) ([]byte, error) {
	log.Printf("INFO: get mods metadata for pid [%s]", md.PID)
	out, err := svc.getRequest(fmt.Sprintf("%s/api/metadata/%s?type=mods", svc.TrackSys.API, md.PID))
	if err != nil {
		return nil, fmt.Errorf("%d:%s", err.StatusCode, err.Message)
	}
	return out, nil
}

func (svc *ServiceContext) getCitation(md *metadata) string {
	log.Printf("INFO: get citation from marc for pid [%s] barcode [%s]", md.PID, md.Barcode)

	parsed, err := svc.getMarcMetadata(md)
	if err != nil {
		log.Printf("ERROR: get marc metadata failed: %s", err.Error())
		return ""
	}

	citation := ""
	location := ""
	for _, df := range parsed.Record.DataFields {
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
				if sf.Code == "i" && sf.Value == md.Barcode {
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

func extractYearFrom260c(raw string) string {
	year := raw
	re := regexp.MustCompile(`([\[\]\(\)]|\.$)`)
	srx := regexp.MustCompile(`\s+`)
	year = re.ReplaceAllString(year, "")
	year = srx.ReplaceAllString(year, " ")
	if year == "" {
		return ""
	}

	if matched, _ := regexp.MatchString(`^\d{4}.0`, year); matched {
		return strings.Split(year, ".")[0]
	}
	if matched, _ := regexp.MatchString(`^\d{2}--`, year); matched {
		// century only
		return year[0:2] + "99"
	}
	if matched, _ := regexp.MatchString(`^\d{2}-`, year); matched {
		// decade only
		return year[0:3] + "9"
	}
	if matched, _ := regexp.MatchString(`^\d{4}\s*-\s*\d{4}`, year); matched {
		// year range separated by -; take the latest year (after the -)
		return strings.Split(year, "-")[1]
	}
	if matched, _ := regexp.MatchString(`^\d{4}\s*-\s*\d{2}`, year); matched {
		// year range separated by - but only 2 digit second year; take the latest year (after the -)
		bits := strings.Split(year, "-")
		return fmt.Sprintf("%s%s", bits[0][0:2], bits[1])
	}

	// mess. just strip out non-number/non-space and see if anything looks like a year
	digitRx := regexp.MustCompile(`[^0-9 ]`)
	strippedYear := digitRx.ReplaceAllLiteralString(year, "")
	done := false
	for _, part := range strings.Split(strippedYear, " ") {
		if matched, _ := regexp.MatchString(`^\d{4}`, part); matched {
			year = part
			done = true
		}
	}
	if done {
		return year
	}

	// maybe roman numerals...
	latest := 0
	romanRx := regexp.MustCompile(`[^IVXLCDM ]`)
	strippedYear = romanRx.ReplaceAllLiteralString(year, "")
	for _, part := range strings.Split(strippedYear, "") {
		val := toArabic(part, 0)
		if val > 1500 && val > latest {
			latest = val
		}
	}
	if latest > 0 {
		return fmt.Sprintf("%d", latest)
	}

	// just return the first part of the year as read (split by space)
	return strings.Split(year, " ")[0]
}

func toArabic(romanStr string, val int) int {
	if romanStr == "" {
		return val
	}
	type rn struct {
		Roman  string
		Arabic int
	}
	romanMap := []rn{
		{Roman: "M", Arabic: 1000}, {Roman: "CM", Arabic: 900}, {Roman: "D", Arabic: 500},
		{Roman: "CD", Arabic: 400}, {Roman: "C", Arabic: 100}, {Roman: "XC", Arabic: 90},
		{Roman: "L", Arabic: 50}, {Roman: "XL", Arabic: 40}, {Roman: "X", Arabic: 10},
		{Roman: "IX", Arabic: 9}, {Roman: "V", Arabic: 5}, {Roman: "IV", Arabic: 4},
		{Roman: "I", Arabic: 1},
	}
	for _, rv := range romanMap {
		if strings.Index(romanStr, rv.Roman) == 0 {
			val += rv.Arabic
			romanStr = romanStr[len(rv.Roman):]
			return toArabic(romanStr, val)
		}
	}
	return val
}
