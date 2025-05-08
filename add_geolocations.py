from geopy.geocoders import Nominatim

from dbConnection import PostgreSQLConnection
from config import Config

class GeoLocator:
    def __init__(self):

        self.cfg = Config()

        # Initialisieren Sie den Geocoder
        self.geolocator = Nominatim(user_agent=self.cfg.osm_user_agent)

    def geocode_with_retry(self, address, max_retries=3, delay=1):
        attempts = 0
        while attempts < max_retries:
            try:
                return self.geolocator.geocode(address, timeout=10)
            except (GeocoderTimedOut, GeocoderUnavailable) as e:
                attempts += 1
                print(f"[{attempts}/{max_retries}] Fehler bei Geocoding: {e}. Neuer Versuch in {delay} Sekunde(n)...")
                time.sleep(delay)
        print(f"Adresse konnte nach {max_retries} Versuchen nicht geocodiert werden: {address}")
        return None

    def add_geolocation(self):

        db = PostgreSQLConnection(self.cfg.dbname, self.cfg.dbuser, self.cfg.dbpassword, self.cfg.dbhost, self.cfg.dbport)
    
        # Stelle eine Verbindung zur Datenbank her
        db.connect()

        # create table if not exists
        db.create_location_table()
        
        # Erstelle die Tabelle
        addresses = db.fetch_missing_location()

        print(f"Es fehlen aktuell noch { len( addresses )} Adressen")

        for address in addresses:
            row = {}

            row["postleitzahl"] = address["postleitzahl"]
            row["ort"] = address["ort"]
            row["strasse"] = address["strasse"]
            row["hausnummer"] = address["hausnummer"] or ''

            # Adresse zusammenstellen
            address_string = f"{row['strasse']} {row['hausnummer']}, {row['postleitzahl']} {row['ort']}".strip()

            print(f"Koordinaten für die Adresse suchen: {address_string}")

            # Adresse in geografische Koordinaten umwandeln
            location = self.geocode_with_retry(address_string)

            if location:
                row["latitude"] = location.latitude
                row["longitude"] = location.longitude
                print(f"Koordinaten für die Adresse: {row['latitude']}, {row['longitude']}")


                db.insert_row("kh_location", row)



location = GeoLocator()

location.add_geolocation()