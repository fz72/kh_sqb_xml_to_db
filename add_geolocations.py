from geopy.geocoders import Nominatim

from dbConnection import PostgreSQLConnection
from config import Config

class GeoLocator:
    def __init__(self):

        self.cfg = Config()

        # Initialisieren Sie den Geocoder
        self.geolocator = Nominatim(user_agent=self.cfg.osm_user_agent)

    def add_geolocation(self):

        db = PostgreSQLConnection(self.cfg.dbname, self.cfg.dbuser, self.cfg.dbpassword, self.cfg.dbhost, self.cfg.dbport)
    
        # Stelle eine Verbindung zur Datenbank her
        db.connect()

        # create table if not exists
        db.create_location_table()
        
        # Erstelle die Tabelle
        addresses = db.fetch_missing_location()

        for address in addresses:
            row = {}

            row["postleitzahl"] = address["postleitzahl"]
            row["ort"] = address["ort"]
            row["strasse"] = address["strasse"]
            row["hausnummer"] = address["hausnummer"]

            # Adresse zusammenstellen
            address_string = f"{row['strasse']} {row['hausnummer']}, {row['postleitzahl']} {row['ort']}"

            print(f"Koordinaten für die Adresse suchen: {address_string}")

            # Adresse in geografische Koordinaten umwandeln
            location = self.geolocator.geocode(address_string)

            if location:
                row["latitude"] = location.latitude
                row["longitude"] = location.longitude
                print(f"Koordinaten für die Adresse: {row['latitude']}, {row['longitude']}")


                db.insert_row("kh_location", row)



location = GeoLocator()

location.add_geolocation()