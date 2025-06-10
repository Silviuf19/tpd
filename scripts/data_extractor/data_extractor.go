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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const url = "https://mfinante.gov.ro/apps/infocodfiscal.html"
const outputFolder = "../../output"

type CompanyInfo struct {
	CUI            int
	AvailableYears []string
}

type Data struct {
	CUI                   int
	ActAutorizare         string
	CodPostal             string
	Telefon               string
	Fax                   string
	StareSocietate        string
	DataUltimeiDeclaratii string
	DataUltimeiPrelucrari string
	ImpozitProfit         string
	ImpozitMicroint       string
	Accize                string
	Tva                   string
	ContributiiAsigSoc    string
	ContributiaAsigMunca  string
	ContributiiAsigSan    string
	TaxaJocuriNoroc       string
	ImpozitSalarii        string
	ImpozitConstructii    string
	ImpozitTiteiGaz       string
	RedeventeMiniere      string
	RedeventePetroliere   string
	AvailableYears        []string
}

// Function to extract data from the HTML table
func extractData(result []byte, cui int) (Data, error) {
	var info Data
	info.CUI = cui
	isDataValid := true

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(result)))
	if err != nil {
		return info, err
	}

	doc.Find("b").First().Each(func(i int, s *goquery.Selection) {
		if !strings.HasPrefix(s.Text(), "AGENTUL ECONOMIC CU CODUL UNIC DE IDENTIFICARE") {
			isDataValid = false
		}
	})

	if !isDataValid {
		return info, fmt.Errorf("invalid data")
	}

	doc.Find(".container .row").Each(func(i int, s *goquery.Selection) {
		key := strings.TrimSpace(s.Find(".col-sm-6").First().Text())
		value := replaceMultipleSpaces(strings.TrimSpace(s.Find(".col-sm-6").Last().Text()))
		if value == "-" {
			value = ""
		}
		switch key {
		case "Act autorizare:":
			info.ActAutorizare = value
		case "Codul postal:":
			info.CodPostal = value
		case "Telefon:":
			info.Telefon = value
		case "Fax:":
			info.Fax = value
		case "Stare societate:":
			info.StareSocietate = value
		case "Data inregistrarii ultimei declaratii: (*)":
			info.DataUltimeiDeclaratii = value
		case "Data ultimei prelucrari: (**)":
			info.DataUltimeiPrelucrari = value
		case "Impozit pe profit (data luarii in evidenta):":
			info.ImpozitProfit = value
		case "Impozit pe veniturile microintreprinderilor (data luarii in evidenta):":
			info.ImpozitMicroint = value
		case "Accize (data luarii in evidenta):":
			info.Accize = value
		case "Taxa pe valoarea adaugata (data luarii in evidenta):":
			info.Tva = value
		case "Contributiile de asigurari sociale (data luarii in evidenta):":
			info.ContributiiAsigSoc = value
		case "Contributia asiguratorie pentru munca (data luarii in evidenta):":
			info.ContributiaAsigMunca = value
		case "Contributia de asigurari sociale de sanatate(data luarii in evidenta):":
			info.ContributiiAsigSan = value
		case "Taxa jocuri de noroc (data luarii in evidenta):":
			info.TaxaJocuriNoroc = value
		case "Impozit pe veniturile din salarii si asimilate salariilor (data luarii in evidenta):":
			info.ImpozitSalarii = value
		case "Impozit pe constructii(data luarii in evidenta):":
			info.ImpozitConstructii = value
		case "Impozit la titeiul si la gazele naturale din productia interna (data luarii in evidenta):":
			info.ImpozitTiteiGaz = value
		case "Redevente miniere/Venituri din concesiuni si inchirieri (data luarii in evidenta):":
			info.RedeventeMiniere = value
		case "Redevente petroliere (data luarii in evidenta):":
			info.RedeventePetroliere = value
		}
	})

	doc.Find("select[name='an'] option").Each(func(i int, s *goquery.Selection) {
		value := s.AttrOr("value", "")
		if strings.HasPrefix(value, "WEB_") {
			info.AvailableYears = append(info.AvailableYears, value)
		}
	})

	return info, nil
}

func replaceMultipleSpaces(input string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(strings.TrimSpace(input), " ")
}

func check(e error, msg string) {
	if e != nil {
		fmt.Printf("msg: %s, %v\n", msg, e)
	}
}

func getCuisFromFile(filename string) []int {
	cuisData, err := os.ReadFile(filename)
	check(err, "Error reading cuis file")

	cuisLines := bytes.Split(cuisData, []byte("\n"))
	cuis := make([]int, 0, len(cuisLines))
	for _, line := range cuisLines {
		cui, err := strconv.Atoi(string(line))
		if err != nil {
			continue
		}
		cuis = append(cuis, cui)
	}
	return cuis
}

func saveJsonData(vars []CompanyInfo) {
	if len(vars) == 0 {
		return
	}
	jsonData, err := json.Marshal(vars)
	check(err, "Error marshalling companies data")
	err = os.WriteFile(fmt.Sprintf("%s/vars/%d-%d.json", outputFolder, vars[0].CUI, vars[len(vars)-1].CUI), jsonData, 0644)
	check(err, "Error writing companies data to file")
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

func worker(client *http.Client, tasks <-chan int, results chan<- []byte, wg *sync.WaitGroup, cookie_string string) {
	defer wg.Done()
	headers := map[string]string{
		"Connection":   "keep-alive",
		"Content-Type": "application/x-www-form-urlencoded",
		"Cookie":       cookie_string,
		"User-Agent":   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	}

	for cui := range tasks {
		data := fmt.Sprintf("pagina=domenii&cod=%d&B1=VIZUALIZARE", cui)
		body := bytes.NewBufferString(data)
		req, err := http.NewRequest("POST", url, body)
		if err != nil {
			check(err, fmt.Sprintf("Error creating request for CUI %d", cui))
			continue
		}

		for key, value := range headers {
			req.Header.Set(key, value)
		}
		resp, err := client.Do(req)
		if err != nil {
			check(err, fmt.Sprintf("Error: sending request for CUI %d %s", cui, err))
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
			check(err, fmt.Sprintf("Error: reading responde body %d", cui))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Error: Unexpected status code %d\n", resp.StatusCode)
			continue
		}
		results <- append([]byte(fmt.Sprintf("%d\n", cui)), responseBody...)
		time.Sleep(20 * time.Millisecond)
	}
}

func writer(results <-chan []byte, done chan<- bool, companies_vars *[]CompanyInfo, csv_writer *csv.Writer, initial_time time.Time) {
	startTime := time.Now()
	for result := range results {
		lines := bytes.SplitN(result, []byte("\n"), 2)
		if len(lines) != 2 {
			continue
		}
		cui := string(lines[0])
		resultHTML := lines[1]

		cuiInt, err := strconv.Atoi(cui)
		check(err, fmt.Sprintf("Error converting CUI %s to int", cui))

		data, err := extractData(resultHTML, cuiInt)
		if err != nil {
			check(err, fmt.Sprintf("Error extracting data for CUI %s", cui))
			continue
		}
		if len(data.AvailableYears) > 0 {
			*companies_vars = append(*companies_vars, CompanyInfo{CUI: cuiInt, AvailableYears: data.AvailableYears})
		}
		record := []string{
			cui,
			data.ActAutorizare,
			data.CodPostal,
			data.Telefon,
			data.Fax,
			data.StareSocietate,
			data.DataUltimeiDeclaratii,
			data.DataUltimeiPrelucrari,
			data.ImpozitProfit,
			data.ImpozitMicroint,
			data.Accize,
			data.Tva,
			data.ContributiiAsigSoc,
			data.ContributiaAsigMunca,
			data.ContributiiAsigSan,
			data.TaxaJocuriNoroc,
			data.ImpozitSalarii,
			data.ImpozitConstructii,
			data.ImpozitTiteiGaz,
			data.RedeventeMiniere,
			data.RedeventePetroliere,
		}
		err = csv_writer.Write(record)
		if err != nil {
			check(err, fmt.Sprintf("Error writing record to csv %d", cuiInt))
		}
		if time.Since(startTime) > 1*time.Minute {
			saveJsonData(*companies_vars)
			companies_vars = &[]CompanyInfo{}
			fmt.Println("Saved data at:", time.Since(initial_time))
			startTime = time.Now()
		}
	}
	done <- true
}

func main() {
	start := time.Now()
	initial_time := start
	client := &http.Client{}
	done := make(chan bool)
	var wg sync.WaitGroup
	companies_vars := []CompanyInfo{}

	args := os.Args
	if len(args) < 2 {
		fmt.Println("Usage: go run data_extractor.go <filename>")
		return
	}
	cuis_file := args[1]
	fmt.Println("wait to get cookie")
	cookie_string := getNewCookie()
	fmt.Println("got cookie", cookie_string, time.Since(start))
	
	if cookie_string == "" {
		fmt.Println("Error getting cookie")
		return
	}
	fmt.Println("cookie ", cookie_string, time.Since(start))
	start = time.Now()

	// reading CUIs from file
	cuis := getCuisFromFile(cuis_file)
	cuis_range_string := fmt.Sprintf("%d-%d", cuis[0], cuis[len(cuis)-1])
	fmt.Println("Number of CUIs to process:", len(cuis), time.Since(start))
	start = time.Now()

	csv_data_file, err := os.OpenFile(fmt.Sprintf("%s/data/%s.csv", outputFolder, cuis_range_string), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		check(err, "Error opening csv file")
		return
	}
	defer csv_data_file.Close()
	fmt.Println("Opened csv file", time.Since(start))
	start = time.Now()

	csv_writer := csv.NewWriter(csv_data_file)
	defer csv_writer.Flush()

	tasks := make(chan int, len(cuis))
	results := make(chan []byte, len(cuis))

	// start tasks
	wg.Add(1)
	go worker(client, tasks, results, &wg, cookie_string)

	// start writerinsert time in the print
	go writer(results, done, &companies_vars, csv_writer, initial_time)

	// send tasks
	for _, cui := range cuis {
		tasks <- cui
	}
	close(tasks)
	fmt.Println("Sent tasks", time.Since(start))
	start = time.Now()

	// wait for workers to finish
	wg.Wait()
	close(results)
	<-done
	fmt.Println("Workers finished", time.Since(start))
	start = time.Now()

	jsonData, err := json.Marshal(companies_vars)
	check(err, "Error marshalling companies data")
	err = os.WriteFile(fmt.Sprintf("%s/vars/%d-%d.json", outputFolder, cuis[0], cuis[len(cuis)-1]), jsonData, 0644)
	check(err, "Error writing companies data to file")
	fmt.Println("Saved all the files", time.Since(start))
	fmt.Printf("Completed in %v\n", time.Since(initial_time))
}