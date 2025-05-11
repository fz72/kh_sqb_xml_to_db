import psycopg2
from psycopg2 import sql
import psycopg2.extras
from psycopg2.extras import execute_values
import io
import csv

class PostgreSQLConnection:
    def __init__(self, dbname, user, password, host, port=5432):
        """
        Konstruktor der Klasse.
        
        :param dbname: Name der PostgreSQL-Datenbank
        :param user: Benutzername für die PostgreSQL-Datenbank
        :param password: Passwort für die PostgreSQL-Datenbank
        :param host: Hostname oder IP-Adresse des PostgreSQL-Servers
        :param port: Port (default: 5432)
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None

    def connect(self):
        """Stellt eine Verbindung zur PostgreSQL-Datenbank her."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            print("Verbindung zur Datenbank hergestellt.")
        except Exception as e:
            print(f"Fehler bei der Verbindung zur Datenbank: {e}")

    def create_table(self):

        if self.conn == None:
            return

        """Erstellt die Tabelle 'kh_daten' in der Datenbank."""
        create_table_query = """
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
            art_nummer int,
            sonstiges varchar(255),
            lehrstatus BOOLEAN,
            akademisches_lehrkrankenhaus varchar(512),
            universitaetsklinikum varchar(512),
            anzahl_betten integer,
            vollstationaere_fallzahl integer,
            teilstationaere_fallzahl integer,
            ambulante_fallzahl integer,
            staeb_fallzahl integer,
            sp04_anzahl_vk decimal(10,2),
            sp04_ambulant_anzahl_vk decimal(10,2),
            sp04_Stationaere_anzahl_vk decimal(10,2),
            filename varchar(255),\n"""

        #for i in range(1, 43):
        #    create_table_query += "bf" + '{:02}'.format(i) + " integer,\n"
        #    create_table_query += "bf" + '{:02}'.format(i) + "_kommentar varchar(500),\n"

        create_table_query += """
            PRIMARY KEY (jahr, ik, standortnummer, standortnummer_alt)
        );
        """
        try:

            # Tabelle löschen (falls sie existiert)
            # self.cur.execute("DROP TABLE IF EXISTS kh_daten CASCADE;")

            self.cur.execute(create_table_query)
            self.conn.commit()
            print("Tabelle 'kh_daten' wurde erfolgreich erstellt.")
        except Exception as e:
            print(f"Fehler beim Erstellen der Tabelle: {e}")
            self.conn.rollback()

    def create_bf_table(self):

        if self.conn == None:
            return

        """Erstellt die Tabelle 'kh_bf' in der Datenbank."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS kh_bf (
            jahr INTEGER NOT NULL,
            ik BIGINT NOT NULL,
            standortnummer BIGINT,
            standortnummer_alt BIGINT,
            bf VARCHAR(4),
            bf_status BOOLEAN,
            bf_kommentar VARCHAR(512),
            PRIMARY KEY (jahr, ik, standortnummer, standortnummer_alt, bf)
        );
        """

        try:

            # Tabelle löschen (falls sie existiert)
            #self.cur.execute("DROP TABLE IF EXISTS kh_bf CASCADE;")

            self.cur.execute(create_table_query)
            self.conn.commit()
            print("Tabelle 'kh_bf' wurde erfolgreich erstellt.")
        except Exception as e:
            print(f"Fehler beim Erstellen der Tabelle: {e}")
            self.conn.rollback()

    def create_art_table(self):
        if self.conn == None:
            return

        """Erstellt die Tabelle 'kh_traeger_art' in der Datenbank."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS kh_traeger_art (
            art_nummer INT NOT NULL,
            art_text VARCHAR(100),
            PRIMARY KEY (art_nummer)
        );
        """

        insert_data_query = """
        INSERT INTO kh_traeger_art (art_nummer, art_text) VALUES
            (1, 'öffentlich'),
            (2, 'freigemeinnützig'),
            (3, 'privat')
        ON CONFLICT (art_nummer) DO NOTHING;
        """

        try:

            # Tabelle löschen (falls sie existiert)
            #self.cur.execute("DROP TABLE IF EXISTS kh_bf CASCADE;")

            self.cur.execute(create_table_query)
            self.cur.execute(insert_data_query)
            self.conn.commit()
            print("Tabelle 'kh_bf' wurde erfolgreich erstellt.")
        except Exception as e:
            print(f"Fehler beim Erstellen der Tabelle: {e}")
            self.conn.rollback()

    def create_location_table(self):
        if self.conn is None:
            return

        """Erstellt die Tabelle 'kh_location' zur Speicherung von Geokoordinaten, verknüpft mit 'kh_daten'."""
        create_location_query = """
        CREATE TABLE IF NOT EXISTS kh_location (
            strasse VARCHAR(255),
            hausnummer VARCHAR(50),
            ort VARCHAR(255),
            postleitzahl VARCHAR(10),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            PRIMARY KEY (strasse, hausnummer, ort, postleitzahl)
        );
        """

        try:
            self.cur.execute(create_location_query)
            self.conn.commit()
            print("Tabelle 'kh_location' wurde erfolgreich erstellt.")
        except Exception as e:
            print(f"Fehler beim Erstellen der Tabelle 'kh_location': {e}")
            self.conn.rollback()

    def create_bf_kategorie_table(self):
        if self.conn == None:
            return

        """Erstellt die Tabelle 'bf_kategorie' in der Datenbank."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS bf_kategorie (
            bf_kategorie_id VARCHAR(10),
            kategorie_text VARCHAR(255),
            PRIMARY KEY (bf_kategorie_id)
        );
        """

        try:

            # Tabelle löschen (falls sie existiert)
            self.cur.execute("DROP TABLE IF EXISTS bf_kategorie CASCADE;")

            self.cur.execute(create_table_query)
            self.conn.commit()
            print("Tabelle 'bf_kategorie' wurde erfolgreich erstellt.")
        except Exception as e:
            print(f"Fehler beim Erstellen der Tabelle: {e}")
            self.conn.rollback()

    def create_bf_aspekte(self):
        if self.conn == None:
            return

        """Erstellt die Tabelle 'bf_aspekte' in der Datenbank."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS bf_aspekte (
            bf VARCHAR(4),
            bf_text VARCHAR(255),
            bf_kategorie_id VARCHAR(10),
            PRIMARY KEY (bf)
        );
        """

        try:

            # Tabelle löschen (falls sie existiert)
            self.cur.execute("DROP TABLE IF EXISTS bf_aspekte CASCADE;")

            self.cur.execute(create_table_query)
            self.conn.commit()
            print("Tabelle 'bf_aspekte' wurde erfolgreich erstellt.")
        except Exception as e:
            print(f"Fehler beim Erstellen der Tabelle: {e}")
            self.conn.rollback()


    def close(self):

        if self.conn == None:
            return

        """Schließt die Verbindung und den Cursor."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("Datenbankverbindung geschlossen.")

    def insert_row(self, table, data):

        if self.conn == None:
            return

        """
        Fügt eine Zeile in die angegebene Tabelle ein.
        
        :param table: Der Tabellenname, in den die Daten eingefügt werden sollen
        :param data: Ein Dictionary mit den Spaltennamen als Schlüssel und den Werten als Werte
        """
        # Stelle sicher, dass Daten und Spalten korrekt sind
        if not isinstance(data, dict):
            raise ValueError("Die 'data' muss ein Dictionary sein.")
        
        # Spaltennamen und Werte extrahieren
        columns = list(data.keys())
        values = [data[column] for column in columns]
        
        # Baue das SQL-Insert-Statement
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() for _ in columns)
        )
        
        try:
            # Führe das Insert-Statement aus
            self.cur.execute(insert_query, values)
            self.conn.commit()  # Commit der Transaktion
            print("Daten wurden erfolgreich eingefügt.")
        except Exception as e:
            self.conn.rollback()  # Rollback bei Fehler
            print(f"Fehler beim Einfügen der Zeile: {e}")

    def insert_rows(self, table, data_list):
        if self.conn is None or not data_list:
            return

        if not isinstance(data_list, list) or not isinstance(data_list[0], dict):
            raise ValueError("Die Eingabedaten müssen eine Liste von Dictionaries sein.")

        columns = list(data_list[0].keys())
        values = [[row[col] for col in columns] for row in data_list]

        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        try:
            execute_values(self.cur, insert_query.as_string(self.conn), values)
            self.conn.commit()
            print(f"{len(data_list)} Zeilen erfolgreich in '{table}' eingefügt.")
        except Exception as e:
            self.conn.rollback()
            print(f"Fehler beim Batch-Insert: {e}")

    def insert_rows_copy(self, table, data_list):
        if self.conn is None or not data_list:
            return

        if not isinstance(data_list, list) or not isinstance(data_list[0], dict):
            raise ValueError("Die Eingabedaten müssen eine Liste von Dictionaries sein.")

        columns = list(data_list[0].keys())
        column_names = ', '.join(columns)

        try:
            with self.cur.copy(f"COPY {table} ({column_names}) FROM STDIN") as copy:
                for row in data_list:
                    # PostgreSQL interpretiert Python None automatisch als NULL
                    copy.write_row([row[col] for col in columns])
            self.conn.commit()
            print(f"{len(data_list)} Zeilen erfolgreich per COPY in '{table}' eingefügt.")
        except Exception as e:
            self.conn.rollback()
            print(f"Fehler beim COPY-Insert: {e}")
    
    def delete_row(self, table, conditions):
        """
        Löscht eine oder mehrere Zeilen aus der angegebenen Tabelle basierend auf Bedingungen.

        :param table: Der Tabellenname, aus dem gelöscht werden soll.
        :param conditions: Ein Dictionary mit Spaltennamen als Schlüssel und Vergleichswerten als Werte.
        """
        if self.conn is None:
            return

        if not isinstance(conditions, dict):
            raise ValueError("Die 'conditions' müssen ein Dictionary sein.")

        try:
            # WHERE-Klausel aufbauen
            where_clause = sql.SQL(' AND ').join(
                sql.Composed([sql.Identifier(k), sql.SQL(' = '), sql.Placeholder()]) for k in conditions
            )

            # DELETE-Query zusammensetzen
            delete_query = sql.SQL("DELETE FROM {} WHERE {}").format(
                sql.Identifier(table),
                where_clause
            )

            # Ausführen
            self.cur.execute(delete_query, list(conditions.values()))
            self.conn.commit()
            print("Eintrag erfolgreich gelöscht.")
        except Exception as e:
            self.conn.rollback()
            print(f"Fehler beim Löschen des Eintrags: {e}")
    
    def fetch_all(self, query, params=None):

        if self.conn == None:
            return

        """
        Führt eine SELECT-Anfrage aus und gibt alle Ergebnisse zurück.
        
        :param query: Die SQL-Abfrage (SELECT)
        :param params: Parameter für die SQL-Abfrage (optional)
        :return: Eine Liste der Ergebnisse
        """
        self.cur.execute(query, params or ())
        return self.cur.fetchall()
    
    def fetch_one(self, query, params=None):

        if self.conn == None:
            return

        """
        Führt eine SELECT-Anfrage aus und gibt das erste Ergebnis zurück.
        
        :param query: Die SQL-Abfrage (SELECT)
        :param params: Parameter für die SQL-Abfrage (optional)
        :return: Ein einzelnes Ergebnis
        """
        self.cur.execute(query, params or ())
        return self.cur.fetchone()

    def fetch_missing_location(self):

        if self.conn == None:
            return

        query = """
        SELECT DISTINCT strasse, hausnummer, ort, postleitzahl
        FROM kh_daten
        WHERE (strasse, hausnummer, ort, postleitzahl) NOT IN (
            SELECT strasse, hausnummer, ort, postleitzahl FROM kh_location
        )
        """

        try:
            # Alle Adressen aus kh_daten laden, die noch nicht in kh_location sind
            
            self.cur.execute(query)
            rows = self.cur.fetchall()
            return rows

        except Exception as e:
            print(f"Failed to read missing locations: {e}")

        

# Beispiel der Verwendung:
if __name__ == "__main__":
    # Ersetze diese Werte durch deine tatsächlichen Verbindungsdaten:
    dbname = 'deine_datenbank'
    user = 'dein_benutzername'
    password = 'dein_passwort'
    host = 'dein_host'  # z.B. 'localhost' oder eine IP-Adresse
    
    # Erstelle eine Instanz der PostgreSQLConnection-Klasse
    db = PostgreSQLConnection(dbname, user, password, host)
    
    # Stelle eine Verbindung zur Datenbank her
    db.connect()
    
    # Erstelle die Tabelle
    db.create_table()

    data = {
        'Jahr': 2023,
        'Name': 'Beispielname',
        'IK': 12345,
        'IK_Weitere': 67890
    }

    db.insert_row("kh_daten", data)
    
    # Schließe die Verbindung zur Datenbank
    db.close()
