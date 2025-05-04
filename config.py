import json

class Config:
    def __init__(self):
        self.config_file = "config.json"
        self.load_config()

    def load_config(self):
        """Lädt die Konfigurationsparameter aus der JSON-Datei."""
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                self.xml_jahr = config.get('xml_jahr')
                self.xml_path = config.get('xml_path')
                self.dbname = config.get('dbname')
                self.dbuser = config.get('user')
                self.dbpassword = config.get('password')
                self.dbhost = config.get('host')
                self.dbport = config.get('port', 5432)  # Standardport 5432
                self.osm_user_agent = config.get('osm_user_agent')
                print("Konfiguration erfolgreich geladen.")
        except Exception as e:
            print(f"Fehler beim Laden der Konfigurationsdatei: {e}")