package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const url = "https://mfinante.gov.ro/apps/infocodfiscal.html"

type CompanyInfo struct {
	CUI            int
	AvailableYears []string
}

type FinancialData struct {
	An                    string
	ActiveImobilizate     string
	ActiveCirculante      string
	Stocuri               string
	Creante               string
	CasaSiConturiLaBanci  string
	CheltuieliInAvans     string
	Datorii               string
	VenituriInAvans       string
	Provizioane           string
	CapitaluriTotal       string
	CapitalSubscrisVarsat string
	PatrimoniulRegiei     string
	CifraDeAfaceriNeta    string
	VenituriTotale        string
	CheltuieliTotale      string
	ProfitBrut            string
	PierdereBrut          string
	ProfitNet             string
	PierdereNet           string
	NumarMediuDeSalariati string
	TipulDeActivitate     string
}

func getNewCookie() string {

	cmd := exec.Command("python3", "../csolver.py")
	output, err := cmd.Output()
	if err != nil {
		check(err, "Error running python script")
		return ""
	}
	result := strings.TrimSpace(string(output))
	return result
}

func check(e error, msg string) {
	if e != nil {
		fmt.Printf("msg: %s, %v\n", msg, e)
	}
}

func formatString(input string) string {
	re := regexp.MustCompile(`\s+`)
	result := re.ReplaceAllString(strings.TrimSpace(input), " ")
	result = strings.ReplaceAll(result, "\n", "")
	result = strings.ReplaceAll(result, "\t", "")
	return result
}

func extractFinancialData(result []byte, an string) (FinancialData, error) {
	financialData := FinancialData{An: an[len(an)-4:]}
	formattedResult := formatString(string(result))
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(formattedResult))
	if err != nil {
		return financialData, err
	}
	invalidData := false
	doc.Find("h3").Each(func(i int, s *goquery.Selection) {
		if strings.Contains(s.Text(), "Introduceti codul unic") {
			invalidData = true
		}
	})

	if invalidData {
		return financialData, fmt.Errorf("invalid-data")
	}

	doc.Find("tr").Each(func(i int, s *goquery.Selection) {
		cells := s.Find("td")
		if cells.Length() == 2 {
			indicator := strings.TrimSpace(cells.Eq(0).Text())
			value := strings.TrimSpace(cells.Eq(1).Text())
			if value == "-" {
				return
			}
			switch indicator {
			case "ACTIVE IMOBILIZATE - TOTAL":
				financialData.ActiveImobilizate = value
			case "ACTIVE CIRCULANTE - TOTAL, din care":
				financialData.ActiveCirculante = value
			case "Stocuri (materii prime, materiale, productie in curs de executie, semifabricate, produse finite, marfuri etc.)":
				financialData.Stocuri = value
			case "Creante":
				financialData.Creante = value
			case "Casa si conturi la banci":
				financialData.CasaSiConturiLaBanci = value
			case "CHELTUIELI IN AVANS":
				financialData.CheltuieliInAvans = value
			case "DATORII":
				financialData.Datorii = value
			case "VENITURI IN AVANS":
				financialData.VenituriInAvans = value
			case "PROVIZIOANE":
				financialData.Provizioane = value
			case "CAPITALURI - TOTAL, din care:":
				financialData.CapitaluriTotal = value
			case "Capital subscris varsat":
				financialData.CapitalSubscrisVarsat = value
			case "Patrimoniul regiei":
				financialData.PatrimoniulRegiei = value
			case "Cifra de afaceri neta":
				financialData.CifraDeAfaceriNeta = value
			case "VENITURI TOTALE":
				financialData.VenituriTotale = value
			case "CHELTUIELI TOTALE":
				financialData.CheltuieliTotale = value
			case "-Profit":
				if i > 0 && strings.Contains(strings.TrimSpace(doc.Find("tr").Eq(i-1).Find("td").Eq(0).Text()), "brut(a)") {
					financialData.ProfitBrut = value
				}
				if i > 0 && strings.Contains(strings.TrimSpace(doc.Find("tr").Eq(i-1).Find("td").Eq(0).Text()), "net(a) a exercitiului financiar") {
					financialData.ProfitNet = value
				}
			case "-Pierdere":
				if i > 0 && strings.Contains(strings.TrimSpace(doc.Find("tr").Eq(i-2).Find("td").Eq(0).Text()), "brut(a)") {
					financialData.PierdereBrut = value
				}
				if i > 0 && strings.Contains(strings.TrimSpace(doc.Find("tr").Eq(i-2).Find("td").Eq(0).Text()), "net(a) a exercitiului financiar") {
					financialData.PierdereNet = value
				}

			case "Numar mediu de salariati":
				financialData.NumarMediuDeSalariati = value
			case "Tipul de activitate, conform clasificarii CAEN":
				financialData.TipulDeActivitate = value
			}
		}
	})

	return financialData, nil
}

func getJsonData(filename string) []CompanyInfo {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		check(err, "Error reading json file")
		return []CompanyInfo{}
	}
	var companies []CompanyInfo
	err = json.Unmarshal(bytes, &companies)
	if err != nil {
		check(err, "Error unmarshalling json file")
		return []CompanyInfo{}
	}
	return companies
}

func financialWorker(client *http.Client, tasks <-chan CompanyInfo, results chan<- []byte, wg *sync.WaitGroup, cookie_string string) {
	defer wg.Done()
	headers := map[string]string{
		"Connection":   "keep-alive",
		"Content-Type": "application/x-www-form-urlencoded",
		"Cookie":       cookie_string,
		"User-Agent":   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	}
	for company := range tasks {
		for _, year := range company.AvailableYears {
			data := fmt.Sprintf("an=%s&cod=%d&method.bilant=VIZUALIZARE", year, company.CUI)
			body := bytes.NewBufferString(data)
			req, err := http.NewRequest("POST", url, body)
			if err != nil {
				check(err, fmt.Sprintf("Error creating request for CUI %d", company.CUI))
				continue
			}
			for key, value := range headers {
				req.Header.Set(key, value)
			}
			resp, err := client.Do(req)
			if err != nil {
				check(err, fmt.Sprintf("Error: sending request for CUI %d %s", company.CUI, err))
				newCookie := getNewCookie()
				if newCookie != "" {
					fmt.Println("Got new cookie:", newCookie)
					cookie_string = newCookie
					headers["Cookie"] = cookie_string
					continue
				} else {
					fmt.Println("Error getting new cookie")
					break
				}
			}
			responseBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				check(err, fmt.Sprintf("Error: reading responde body %d", company.CUI))
				continue
			}
			if resp.StatusCode != http.StatusOK {
				fmt.Printf("Error: Unexpected status code %d", resp.StatusCode)
				continue
			}
			results <- append([]byte(fmt.Sprintf("%d\n%s\n", company.CUI, year)), responseBody...)
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func writer(results <-chan []byte, done chan<- bool, csv_writer *csv.Writer, initialTime time.Time) {
	timeTicker := time.Now()
	for result := range results {
		lines := bytes.SplitN(result, []byte("\n"), 3)
		if len(lines) != 3 {
			continue
		}
		cui := string(lines[0])
		an := string(lines[1])
		resultHTML := lines[2]

		data, err := extractFinancialData(resultHTML, an)
		if err != nil {
			check(err, fmt.Sprintf("Error extracting data for CUI %s", cui))
			continue
		}
		record := []string{
			cui,
			data.An,
			data.ActiveImobilizate,
			data.ActiveCirculante,
			data.Stocuri,
			data.Creante,
			data.CasaSiConturiLaBanci,
			data.CheltuieliInAvans,
			data.Datorii,
			data.VenituriInAvans,
			data.Provizioane,
			data.CapitaluriTotal,
			data.CapitalSubscrisVarsat,
			data.PatrimoniulRegiei,
			data.CifraDeAfaceriNeta,
			data.VenituriTotale,
			data.CheltuieliTotale,
			data.ProfitBrut,
			data.PierdereBrut,
			data.ProfitNet,
			data.PierdereNet,
			data.NumarMediuDeSalariati,
			data.TipulDeActivitate,
		}
		err = csv_writer.Write(record)
		if err != nil {
			check(err, fmt.Sprintf("Error writing record to csv %s", cui))
		}
		if time.Since(timeTicker) > 1*time.Minute {
			fmt.Println("Writing data", cui, time.Since(initialTime))
			timeTicker = time.Now()
		}

	}
	done <- true
}

func main() {
	start := time.Now()
	initialTime := start
	client := &http.Client{}
	done := make(chan bool)
	var wg sync.WaitGroup

	args := os.Args
	if len(args) < 3 {
		fmt.Println("Usage: go run financial_data_extractor.go <filename> <output_file>")
		return
	}
	vars_file := args[1]
	output_file := args[2]

	fmt.Println("Wait to get cookie...")
	cookie_string := getNewCookie()
	if cookie_string == "" {
		fmt.Println("Error getting cookie")
		return
	}
	fmt.Println("Got cookie", cookie_string, time.Since(start))
	start = time.Now()

	companies := getJsonData(vars_file)
	fmt.Println("Got companies", len(companies), time.Since(start))
	start = time.Now()

	csv_data_file, err := os.OpenFile(fmt.Sprintf("%s.csv", output_file), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		check(err, "Error opening csv file")
		return
	}
	defer csv_data_file.Close()
	fmt.Println("Opened csv file", time.Since(start))
	start = time.Now()

	csv_writer := csv.NewWriter(csv_data_file)
	csv_writer.Comma = '^'
	defer csv_writer.Flush()

	tasks := make(chan CompanyInfo, len(companies))
	results := make(chan []byte, len(companies))

	wg.Add(1)
	go financialWorker(client, tasks, results, &wg, cookie_string)

	go writer(results, done, csv_writer, initialTime)

	for _, company := range companies {
		tasks <- company
	}
	close(tasks)
	fmt.Println("Sent tasks", time.Since(start))

	wg.Wait()
	close(results)
	<-done

	fmt.Println("Finished writing", time.Since(initialTime))
}
