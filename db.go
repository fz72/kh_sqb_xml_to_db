package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type PostgreSQLConnection struct {
	dbname   string
	user     string
	password string
	host     string
	port     int
	conn     *sql.DB
}

func NewPostgreSQLConnection(dbname, user, password, host string, port int) *PostgreSQLConnection {
	if port == 0 {
		port = 5432 // Standardport für PostgreSQL
	}
	return &PostgreSQLConnection{
		dbname:   dbname,
		user:     user,
		password: password,
		host:     host,
		port:     port,
	}
}

func (pg *PostgreSQLConnection) Connect() {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", pg.user, pg.password, pg.host, pg.port, pg.dbname)
	var err error
	pg.conn, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Fehler bei der Verbindung zur Datenbank: ", err)
	}
	fmt.Println("Verbindung zur Datenbank hergestellt.")
}

func (pg *PostgreSQLConnection) CreateTable() {
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS kh_daten (
		jahr integer NOT NULL,
		name varchar(255),
		ik bigint NOT NULL,
		ik_weitere bigint,
		standortnummer bigint,
		standortnummer_datei varchar(255),
		standortnummer_alt bigint,
		strasse varchar(255),
		hausnummer varchar(255),
		postleitzahl varchar(10),
		ort varchar(255),
		bundesland varchar(100),
		bundesland_plz varchar(100),
		landkreis varchar(255),
		krankenhaustraeger_name text,
		art varchar(100),
		art_nummer varchar(50),
		sonstiges varchar(255),
		lehrstatus varchar(50),
		akademisches_lehrkrankenhaus varchar(50),
		universitaetsklinikum varchar(50),
		anzahl_betten integer,
		vollstationaere_fallzahl integer,
		teilstationaere_fallzahl integer,
		ambulante_fallzahl integer,
		staeb_fallzahl integer,
		sp04_anzahl_vk decimal(10,2),
		sp04_ambulant_anzahl_vk decimal(10,2),
		sp04_Stationaere_anzahl_vk decimal(10,2),
		filename varchar(255),`

	// Dynamisch die bf Spalten hinzufügen
	for i := 1; i <= 42; i++ {
		createTableQuery += fmt.Sprintf("bf%02d integer,\n", i)
		createTableQuery += fmt.Sprintf("bf%02d_kommentar varchar(500),\n", i)
	}

	createTableQuery += `PRIMARY KEY (jahr, ik, standortnummer, standortnummer_alt)
	);`

	_, err := pg.conn.Exec(createTableQuery)
	if err != nil {
		log.Fatal("Fehler beim Erstellen der Tabelle: ", err)
	}
	fmt.Println("Tabelle 'kh_daten' wurde erfolgreich erstellt.")
}

func (pg *PostgreSQLConnection) InsertRow(table string, data map[string]interface{}) {
	columns := ""
	values := ""
	args := []interface{}{}
	i := 1
	for column, value := range data {
		columns += column
		values += fmt.Sprintf("$%d", i)
		args = append(args, value)

		if i < len(data) {
			columns += ", "
			values += ", "
		}
		i++
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columns, values)
	_, err := pg.conn.Exec(insertQuery, args...)
	if err != nil {
		log.Fatal("Fehler beim Einfügen der Zeile: ", err)
	}
	fmt.Println("Daten wurden erfolgreich eingefügt.")
}

func (pg *PostgreSQLConnection) FetchAll(query string, params ...interface{}) ([]map[string]interface{}, error) {
	rows, err := pg.conn.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	results := []map[string]interface{}{}
	for rows.Next() {
		columnPointers := make([]interface{}, len(columns))
		columnValues := make([]interface{}, len(columns))
		for i := range columns {
			columnPointers[i] = &columnValues[i]
		}
		err = rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}
		result := make(map[string]interface{})
		for i, col := range columns {
			result[col] = columnValues[i]
		}
		results = append(results, result)
	}
	return results, nil
}

func (pg *PostgreSQLConnection) FetchOne(query string, params ...interface{}) (map[string]interface{}, error) {
	row := pg.conn.QueryRow(query, params...)
	columns, _ := row.Columns()
	columnPointers := make([]interface{}, len(columns))
	columnValues := make([]interface{}, len(columns))
	for i := range columns {
		columnPointers[i] = &columnValues[i]
	}

	err := row.Scan(columnPointers...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i, col := range columns {
		result[col] = columnValues[i]
	}
	return result, nil
}

func (pg *PostgreSQLConnection) Close() {
	if pg.conn != nil {
		pg.conn.Close()
	}
	fmt.Println("Datenbankverbindung geschlossen.")
}

func main() {
	// Verbindungsdaten
	dbname := "deine_datenbank"
	user := "dein_benutzername"
	password := "dein_passwort"
	host := "dein_host" // z.B. 'localhost' oder eine IP-Adresse

	// Erstelle eine Instanz der PostgreSQLConnection-Klasse
	db := NewPostgreSQLConnection(dbname, user, password, host, 5432)

	// Stelle eine Verbindung zur Datenbank her
	db.Connect()
	defer db.Close()

	// Erstelle die Tabelle
	db.CreateTable()

	// Beispiel-Daten zum Einfügen
	data := map[string]interface{}{
		"jahr":       2023,
		"name":       "Beispielname",
		"ik":         12345,
		"ik_weitere": 67890,
	}

	// Füge eine Zeile in die Tabelle ein
	db.InsertRow("kh_daten", data)

	// Beispiel für das Abrufen von Daten
	query := "SELECT * FROM kh_daten LIMIT 1"
	result, err := db.FetchOne(query)
	if err != nil {
		log.Fatal("Fehler beim Abrufen der Daten: ", err)
	}
	fmt.Println("Ergebnisse:", result)
}
