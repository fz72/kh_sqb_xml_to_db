package main

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

type Config struct {
	DBName     string `json:"dbname"`
	DBUser     string `json:"user"`
	DBPassword string `json:"password"`
	DBHost     string `json:"host"`
	DBPort     int    `json:"port"`
	XmlPath    string `json:"xml_path`
}

type PlzData struct {
	Landkreis  string `json:"landkreis"`
	Bundesland string `json:"bundesland"`
}

type XMLParser struct {
	ConfigFile  string
	Config      Config
	Columns     []string
	Rows        []map[string]string
	PlzDatabase map[string]PlzData
}

func (parser *XMLParser) LoadConfig() error {
	file, err := os.Open(parser.ConfigFile)
	if err != nil {
		return fmt.Errorf("error opening config file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&parser.Config); err != nil {
		return fmt.Errorf("error decoding config file: %v", err)
	}
	return nil
}

func (parser *XMLParser) SaveToDB() error {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=disable",
		parser.Config.DBUser, parser.Config.DBPassword, parser.Config.DBName, parser.Config.DBHost, parser.Config.DBPort)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("error connecting to the database: %v", err)
	}
	defer db.Close()

	// Create the table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS kh_daten (
		jahr INT,
		name TEXT,
		ik TEXT,
		ik_weitere TEXT,
		standortnummer TEXT,
		standortnummer_datei TEXT,
		standortnummer_alt TEXT,
		strasse TEXT,
		hausnummer TEXT,
		postleitzahl TEXT,
		ort TEXT,
		bundesland TEXT,
		bundesland_plz TEXT,
		landkreis TEXT,
		krankenhaustraeger_name TEXT,
		art TEXT,
		art_nummer TEXT,
		sonstiges TEXT,
		lehrstatus TEXT,
		akademisches_lehrkrankenhaus TEXT,
		universitaetsklinikum TEXT,
		anzahl_betten INT,
		vollstationaere_fallzahl INT,
		teilstationaere_fallzahl INT,
		ambulante_fallzahl INT,
		staeb_fallzahl INT,
		sp04_anzahl_vk TEXT,
		sp04_ambulant_anzahl_vk TEXT,
		sp04_stationaere_anzahl_vk TEXT,
		filename TEXT
	)`)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Insert data into the database
	for _, row := range parser.Rows {
		_, err := db.Exec(`INSERT INTO kh_daten (jahr, name, ik, ik_weitere, standortnummer, standortnummer_datei, standortnummer_alt, 
			strasse, hausnummer, postleitzahl, ort, bundesland, bundesland_plz, landkreis, krankenhaustraeger_name, art, art_nummer,
			sonstiges, lehrstatus, akademisches_lehrkrankenhaus, universitaetsklinikum, anzahl_betten, vollstationaere_fallzahl,
			teilstationaere_fallzahl, ambulante_fallzahl, staeb_fallzahl, sp04_anzahl_vk, sp04_ambulant_anzahl_vk, sp04_stationaere_anzahl_vk, filename) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)`,
			row["jahr"], row["name"], row["ik"], row["ik_weitere"], row["standortnummer"], row["standortnummer_datei"], row["standortnummer_alt"],
			row["strasse"], row["hausnummer"], row["postleitzahl"], row["ort"], row["bundesland"], row["bundesland_plz"], row["landkreis"],
			row["krankenhaustraeger_name"], row["art"], row["art_nummer"], row["sonstiges"], row["lehrstatus"], row["akademisches_lehrkrankenhaus"],
			row["universitaetsklinikum"], row["anzahl_betten"], row["vollstationaere_fallzahl"], row["teilstationaere_fallzahl"], row["ambulante_fallzahl"],
			row["staeb_fallzahl"], row["sp04_anzahl_vk"], row["sp04_ambulant_anzahl_vk"], row["sp04_stationaere_anzahl_vk"], row["filename"])
		if err != nil {
			return fmt.Errorf("error inserting row: %v", err)
		}
	}
	return nil
}

func (parser *XMLParser) ParseXML() error {
	path = parser.Config.XmlPath
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return fmt.Errorf("error reading directory: %v", err)
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".xml") {
			filePath := path + "/" + file.Name()
			fmt.Println("Parsing:", filePath)
			xmlFile, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("error opening XML file: %v", err)
			}
			defer xmlFile.Close()

			var xmlData map[string]interface{}
			decoder := xml.NewDecoder(xmlFile)
			if err := decoder.Decode(&xmlData); err != nil {
				return fmt.Errorf("error decoding XML: %v", err)
			}

			row := make(map[string]string)
			row["jahr"] = path
			row["filename"] = file.Name()

			// Example of how to populate fields from the XML
			if name, ok := xmlData["Name"]; ok {
				row["name"] = name.(string)
			}

			// Add your logic for extracting other fields here...

			// Append row to the rows slice
			parser.Rows = append(parser.Rows, row)
		}
	}
	return nil
}

func main() {
	parser := XMLParser{
		ConfigFile: "config.json",
	}

	// Load config
	if err := parser.LoadConfig(); err != nil {
		fmt.Println("Error loading config:", err)
		return
	}

	// Parse XML files
	if err := parser.ParseXML(); err != nil {
		fmt.Println("Error parsing XML:", err)
		return
	}

	// Save to DB
	if err := parser.SaveToDB(); err != nil {
		fmt.Println("Error saving to DB:", err)
	}
}
