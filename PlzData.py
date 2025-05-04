import requests
import json

class PlzData:
    def __init__(self):
        self.landkreis = ""
        self.bundesland = ""

class PlzDatabase:
    def __init__(self, cache_file='plz_data.json'):
        """
        Konstruktor der Klasse. Initialisiert den Cache und lädt Daten aus einer Datei, falls vorhanden.
        
        :param cache_file: Der Pfad zur Datei, in der die PLZ-Daten gespeichert sind (Standard: 'plz_data.json')
        """
        self.cache_file = cache_file
        self.plz_db = self.read_plz()  # Lädt die PLZ-Daten, falls vorhanden

    def get_plz_data(self, iv_plz, iv_ort):
        """
        Holt die PLZ-Daten für eine bestimmte Postleitzahl und Ort.
        Falls die Daten nicht im Cache sind, wird eine API-Abfrage durchgeführt.
        
        :param iv_plz: Die Postleitzahl
        :param iv_ort: Der Ort
        :return: Ein `PlzData`-Objekt mit Landkreis und Bundesland
        """
        plz_data = PlzData()

        # Überprüfen, ob die Daten im Cache vorhanden sind
        data = self.plz_db.get(iv_plz)

        if data is None or len(data) == 0:
            # Falls nicht, API-Abfrage durchführen
            url = f"https://openplzapi.org/de/Localities?postalCode={iv_plz}"
            response = requests.get(url)
            data = response.json()

            # Speichern der abgerufenen Daten im Cache
            self.plz_db[iv_plz] = data

        # Suchen nach den richtigen Daten für den Ort
        for elem in data:
            ort_tmp = elem.get("name", "")

            if ort_tmp == iv_ort:
                plz_data.landkreis = elem.get("district", {}).get("name", "error")
                plz_data.bundesland = elem.get("federalState", {}).get("name", "error")
                break

        # Falls keine passende Übereinstimmung gefunden wurde, nehmen wir die erste gefundene Region
        if plz_data.landkreis == "" and len(data) > 0:
            plz_data.landkreis = data[0].get("district", {}).get("name", "error")
            plz_data.bundesland = data[0].get("federalState", {}).get("name", "error")

        return plz_data

    def save_plz(self):
        """Speichert die PLZ-Daten im Cache in einer JSON-Datei."""
        with open(self.cache_file, "w", encoding='utf8') as outfile:
            json.dump(self.plz_db, outfile)
        print("PLZ-Daten wurden gespeichert.")

    def read_plz(self):
        """Liest die PLZ-Daten aus einer JSON-Datei."""
        try:
            with open(self.cache_file, "r", encoding='utf8') as data_file:
                return json.load(data_file)
        except FileNotFoundError:
            # Falls die Datei nicht existiert, geben wir ein leeres Dictionary zurück
            return {}

# Beispiel der Verwendung:
if __name__ == "__main__":
    plz_db = PlzDatabase()

    # Beispiel: PLZ-Daten für eine bestimmte PLZ und Ort abfragen
    iv_plz = "80331"
    iv_ort = "München"
    plz_data = plz_db.get_plz_data(iv_plz, iv_ort)

    print(f"Landkreis: {plz_data.landkreis}")
    print(f"Bundesland: {plz_data.bundesland}")

    # Speichern der PLZ-Daten in der Datei
    plz_db.save_plz()
