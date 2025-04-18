package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

// PlzData repräsentiert die Daten einer Postleitzahl
type PlzData struct {
	Landkreis  string `json:"landkreis"`
	Bundesland string `json:"bundesland"`
}

// PlzDatabase stellt die Datenbank der PLZ-Daten dar
type PlzDatabase struct {
	CacheFile string
	PlzDb     map[string][]map[string]interface{}
}

// NewPlzDatabase erstellt eine neue Instanz von PlzDatabase
func NewPlzDatabase(cacheFile string) *PlzDatabase {
	db := &PlzDatabase{
		CacheFile: cacheFile,
		PlzDb:     make(map[string][]map[string]interface{}),
	}
	db.readPlz()
	return db
}

// GetPlzData holt die PLZ-Daten für eine bestimmte Postleitzahl und Ort
func (db *PlzDatabase) GetPlzData(ivPlz, ivOrt string) (*PlzData, error) {
	plzData := &PlzData{}

	// Überprüfen, ob die Daten im Cache vorhanden sind
	data, exists := db.PlzDb[ivPlz]
	if !exists || len(data) == 0 {
		// Falls nicht, API-Abfrage durchführen
		url := fmt.Sprintf("https://openplzapi.org/de/Localities?postalCode=%s", ivPlz)
		response, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()

		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}

		// JSON-Daten parsen
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, err
		}

		// Speichern der abgerufenen Daten im Cache
		db.PlzDb[ivPlz] = data
	}

	// Suchen nach den richtigen Daten für den Ort
	for _, elem := range data {
		ortTmp, ok := elem["name"].(string)
		if ok && ortTmp == ivOrt {
			if district, ok := elem["district"].(map[string]interface{}); ok {
				plzData.Landkreis = district["name"].(string)
			}
			plzData.Bundesland = elem["federalState"].(map[string]interface{})["name"].(string)
			break
		}
	}

	// Falls keine passende Übereinstimmung gefunden wurde, nehmen wir die erste gefundene Region
	if plzData.Landkreis == "" && len(data) > 0 {
		if district, ok := data[0]["district"].(map[string]interface{}); ok {
			plzData.Landkreis = district["name"].(string)
		}
		plzData.Bundesland = data[0]["federalState"].(map[string]interface{})["name"].(string)
	}

	return plzData, nil
}

// SavePlz speichert die PLZ-Daten im Cache in einer JSON-Datei
func (db *PlzDatabase) SavePlz() error {
	file, err := json.MarshalIndent(db.PlzDb, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(db.CacheFile, file, 0644)
	if err != nil {
		return err
	}
	fmt.Println("PLZ-Daten wurden gespeichert.")
	return nil
}

// ReadPlz liest die PLZ-Daten aus einer JSON-Datei
func (db *PlzDatabase) readPlz() {
	// Prüfen, ob die Datei existiert
	_, err := os.Stat(db.CacheFile)
	if os.IsNotExist(err) {
		// Falls die Datei nicht existiert, geben wir ein leeres Dictionary zurück
		return
	}

	// Datei einlesen
	file, err := ioutil.ReadFile(db.CacheFile)
	if err != nil {
		fmt.Println("Fehler beim Lesen der Datei:", err)
		return
	}

	// JSON-Daten parsen
	err = json.Unmarshal(file, &db.PlzDb)
	if err != nil {
		fmt.Println("Fehler beim Parsen der JSON-Datei:", err)
	}
}

func main() {
	// Beispiel der Verwendung
	plzDb := NewPlzDatabase("plz_data.json")

	// Beispiel: PLZ-Daten für eine bestimmte PLZ und Ort abfragen
	ivPlz := "72290"
	ivOrt := "Alpirsbach"
	plzData, err := plzDb.GetPlzData(ivPlz, ivOrt)
	if err != nil {
		fmt.Println("Fehler:", err)
		return
	}

	fmt.Printf("Landkreis: %s\n", plzData.Landkreis)
	fmt.Printf("Bundesland: %s\n", plzData.Bundesland)

	// Speichern der PLZ-Daten in der Datei
	if err := plzDb.SavePlz(); err != nil {
		fmt.Println("Fehler beim Speichern der Datei:", err)
	}
}
