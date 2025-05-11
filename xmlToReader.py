import xml.etree.ElementTree as Xet
import pandas as pd
import requests
import os
import io
import json
from datetime import datetime
import time

from docutils.nodes import TextElement
#from salt.states.dellchassis import switch

from dbConnection import PostgreSQLConnection
from PlzData import PlzDatabase
from config import Config

class xmlToReader:
    def __init__(self):

        self.cfg = Config()

        self.bf_kategorie = [
            {
                "bf_kategorie_id": "CAT01",
                "kategorie_text": "Bauliche und organisatorische Maßnahmen zur Berücksichtigung des besonderen Bedarfs von Menschen mit Sehbehinderungen oder Blindheit"
            },
            {
                "bf_kategorie_id": "CAT02",
                "kategorie_text": "Bauliche und organisatorische Maßnahmen zur Berücksichtigung des besonderen Bedarfs von Menschen mit Mobilitätseinschränkungen"
            },
            {
                "bf_kategorie_id": "CAT03",
                "kategorie_text": "Organisatorische Maßnahmen zur Berücksichtigung des besonderen Bedarfs von Menschen mit Hörbehinderung oder Gehörlosigkeit"
            },
            {
                "bf_kategorie_id": "CAT04",
                "kategorie_text": "Bauliche und organisatorische Maßnahmen zur Berücksichtigung des besonderen Bedarfs von Menschen mit Demenz oder geistiger Behinderung"
            },
            {
                "bf_kategorie_id": "CAT05",
                "kategorie_text": "Bauliche und organisatorische Maßnahmen zur Berücksichtigung des besonderen Bedarfs von Patientinnen und Patienten mit besonderem Übergewicht oder besonderer Körpergröße oder massiver körperlicher Beeinträchtigung"
            },
            {
                "bf_kategorie_id": "CAT06",
                "kategorie_text": "Bauliche und organisatorische Maßnahmen zur Berücksichtigung des besonderen Bedarfs von Patientinnen oder Patienten mit schweren Allergien"
            },
            {
                "bf_kategorie_id": "CAT07",
                "kategorie_text": "Berücksichtigung von Fremdsprachlichkeit und Religionsausübung"
            },
            {
                "bf_kategorie_id": "CAT08",
                "kategorie_text": "Organisatorische Rahmenbedingungen zur Barrierefreiheit"
            }
        ]


        self.bf_aspekte = [
            # CAT01: Sehbehinderung / Blindheit
            {"bf": "BF01", "bf_text": "Kontrastreiche Beschriftungen in erhabener Profilschrift und/oder Blindenschrift/Brailleschrift", "bf_kategorie_id": "CAT01"},
            {"bf": "BF02", "bf_text": "Aufzug mit Sprachansage und/oder Beschriftung in erhabener Profilschrift und/oder Blindenschrift/Brailleschrift", "bf_kategorie_id": "CAT01"},
            {"bf": "BF03", "bf_text": "Tastbarer Gebäudeplan", "bf_kategorie_id": "CAT01"},
            {"bf": "BF04", "bf_text": "Schriftliche Hinweise in gut lesbarer, großer und kontrastreicher Beschriftung", "bf_kategorie_id": "CAT01"},
            {"bf": "BF05", "bf_text": "Leitsysteme und/oder personelle Unterstützung für sehbehinderte oder blinde Menschen", "bf_kategorie_id": "CAT01"},

            # CAT02: Mobilitätseinschränkungen
            {"bf": "BF33", "bf_text": "Barrierefreie Erreichbarkeit für Menschen mit Mobilitätseinschränkungen", "bf_kategorie_id": "CAT02"},
            {"bf": "BF34", "bf_text": "Barrierefreie Erschließung des Zugangs- und Eingangsbereichs für Menschen mit Mobilitätseinschränkungen", "bf_kategorie_id": "CAT02"},
            {"bf": "BF06", "bf_text": "Zimmerausstattung mit rollstuhlgerechten Sanitäranlagen", "bf_kategorie_id": "CAT02"},
            {"bf": "BF08", "bf_text": "Rollstuhlgerechter Zugang zu Serviceeinrichtungen", "bf_kategorie_id": "CAT02"},
            {"bf": "BF09", "bf_text": "Rollstuhlgerecht bedienbarer Aufzug (innen/außen)", "bf_kategorie_id": "CAT02"},
            {"bf": "BF10", "bf_text": "Rollstuhlgerechte Toiletten für Besucherinnen und Besucher", "bf_kategorie_id": "CAT02"},
            {"bf": "BF11", "bf_text": "Besondere personelle Unterstützung", "bf_kategorie_id": "CAT02"},

            # CAT03: Hörbehinderung / Gehörlosigkeit
            {"bf": "BF35", "bf_text": "Ausstattung von Zimmern mit Signalanlagen und/oder visuellen Anzeigen", "bf_kategorie_id": "CAT03"},
            {"bf": "BF36", "bf_text": "Ausstattung der Wartebereiche vor Behandlungsräumen mit einer visuellen Anzeige einer oder eines zur Behandlung aufgerufenen Patientin oder Patienten", "bf_kategorie_id": "CAT03"},
            {"bf": "BF37", "bf_text": "Aufzug mit visueller Anzeige", "bf_kategorie_id": "CAT03"},
            {"bf": "BF38", "bf_text": "Kommunikationshilfen", "bf_kategorie_id": "CAT03"},
            {"bf": "BF13", "bf_text": "Übertragung von Informationen in leicht verständlicher, klarer Sprache", "bf_kategorie_id": "CAT03"},

            # CAT04: Demenz / geistige Behinderung
            {"bf": "BF14", "bf_text": "Arbeit mit Piktogrammen", "bf_kategorie_id": "CAT04"},
            {"bf": "BF15", "bf_text": "Bauliche Maßnahmen für Menschen mit Demenz oder geistiger Behinderung", "bf_kategorie_id": "CAT04"},
            {"bf": "BF16", "bf_text": "Besondere personelle Unterstützung von Menschen mit Demenz oder geistiger Behinderung", "bf_kategorie_id": "CAT04"},

            # CAT05: Übergewicht / Körpergröße / massive körperliche Beeinträchtigung
            {"bf": "BF17", "bf_text": "Geeignete Betten für Patientinnen und Patienten mit besonderem Übergewicht oder besonderer Körpergröße", "bf_kategorie_id": "CAT05"},
            {"bf": "BF18", "bf_text": "OP-Einrichtungen für Patientinnen und Patienten mit besonderem Übergewicht oder besonderer Körpergröße", "bf_kategorie_id": "CAT05"},
            {"bf": "BF19", "bf_text": "Röntgeneinrichtungen für Patientinnen und Patienten mit besonderem Übergewicht oder besonderer Körpergröße", "bf_kategorie_id": "CAT05"},
            {"bf": "BF20", "bf_text": "Untersuchungseinrichtungen/-geräte für Patientinnen und Patienten mit besonderem Übergewicht oder besonderer Körpergröße", "bf_kategorie_id": "CAT05"},
            {"bf": "BF21", "bf_text": "Hilfsgeräte zur Unterstützung bei der Pflege für Patientinnen und Patienten mit besonderem Übergewicht oder besonderer Körpergröße", "bf_kategorie_id": "CAT05"},
            {"bf": "BF22", "bf_text": "Hilfsmittel für Patientinnen und Patienten mit besonderem Übergewicht oder besonderer Körpergröße", "bf_kategorie_id": "CAT05"},

            # CAT06: Allergien
            {"bf": "BF23", "bf_text": "Allergenarme Zimmer", "bf_kategorie_id": "CAT06"},
            {"bf": "BF24", "bf_text": "Diätische Angebote", "bf_kategorie_id": "CAT06"},

            # CAT07: Fremdsprachlichkeit / Religion
            {"bf": "BF25", "bf_text": "Dolmetscherdienste", "bf_kategorie_id": "CAT07"},
            {"bf": "BF26", "bf_text": "Behandlungsmöglichkeiten durch fremdsprachiges Personal", "bf_kategorie_id": "CAT07"},
            {"bf": "BF29", "bf_text": "Mehrsprachiges Informationsmaterial über das Krankenhaus", "bf_kategorie_id": "CAT07"},
            {"bf": "BF30", "bf_text": "Mehrsprachige Internetseite", "bf_kategorie_id": "CAT07"},
            {"bf": "BF31", "bf_text": "Mehrsprachiges Orientierungssystem (Ausschilderung)", "bf_kategorie_id": "CAT07"},
            {"bf": "BF32", "bf_text": "Räumlichkeiten zur religiösen und spirituellen Besinnung", "bf_kategorie_id": "CAT07"},

            # CAT08: Barrierefreiheit allgemein
            {"bf": "BF39", "bf_text": "Informationen zur Barrierefreiheit auf der Internetseite des Krankenhauses", "bf_kategorie_id": "CAT08"},
            {"bf": "BF40", "bf_text": "Barrierefreie Eigenpräsentation/Informationsdarbietung auf der Krankenhaushomepage", "bf_kategorie_id": "CAT08"},
            {"bf": "BF41", "bf_text": "Barrierefreie Zugriffsmöglichkeiten auf Notrufsysteme", "bf_kategorie_id": "CAT08"}
        ]


        self.cols = ["jahr", "name", "ik", "ik_weitere", "standortnummer", "standortnummer_datei", "standortnummer_alt",
                "strasse", "hausnummer", "postleitzahl", "ort", "bundesland", "bundesland_plz", "landkreis",
                "krankenhaustraeger_name", "art", "art_nummer", "sonstiges",
                "lehrstatus", "akademisches_lehrkrankenhaus", "universitaetsklinikum",
                "anzahl_betten",
                "vollstationaere_fallzahl", "teilstationaere_fallzahl", "ambulante_fallzahl", "staeb_fallzahl",
                "sp04_anzahl_vk", "sp04_ambulant_anzahl_vk", "sp04_stationaere_anzahl_vk", "filename",
                "bf_gesamt_absolut", "bf_gesamt_cat01", "bf_gesamt_cat02", "bf_gesamt_cat03", "bf_gesamt_cat04", 
                "bf_gesamt_cat05", "bf_gesamt_cat06", "bf_gesamt_cat07", "bf_gesamt_cat08"]
        
        for aspekt in self.bf_aspekte:
            col_name = aspekt["bf"]
            self.cols.append(col_name)
            self.cols.append(col_name + "_kommentar")
        self.rows = []

        self.plz_db = PlzDatabase()

    def save_to_db(self):
        db = PostgreSQLConnection(self.cfg.dbname, self.cfg.dbuser, self.cfg.dbpassword, self.cfg.dbhost, self.cfg.dbport)
    
        # Stelle eine Verbindung zur Datenbank her
        db.connect()
        
        # Erstelle die Tabellen
        db.create_bf_kategorie_table()
        db.insert_rows("bf_kategorie", self.bf_kategorie)

        db.create_bf_aspekte()
        db.insert_rows("bf_aspekte", self.bf_aspekte)

        db.create_table()

        db.create_bf_table()

        db.create_art_table()

        db.delete_row("kh_daten", {
            "jahr": self.cfg.xml_jahr
        })
        db.delete_row("kh_bf", {
            "jahr": self.cfg.xml_jahr
        })

        kh_daten_rows = []


        for index, row in enumerate(self.rows, start=1):
            print(f"Zeile {index} von {len(self.rows)}")
            # Zeile einfügen

            bf_rows = []

            # Lehrstatus als BOOL
            row["lehrstatus"] = bool(row["lehrstatus"])
            row["universitaetsklinikum"] = bool(row["universitaetsklinikum"])

            for aspekt in self.bf_aspekte:
                key = aspekt["bf"]

                bf_row = {}
                bf_row["jahr"] = row["jahr"]
                bf_row["ik"] = row["ik"]
                bf_row["standortnummer"] = row["standortnummer"]
                bf_row["standortnummer_alt"] = row["standortnummer_alt"]
                bf_row["bf"] = key

                key_kommentar = key + "_kommentar"

                bf_row["bf_status"] = bool(row[key])
                if key_kommentar in row:
                    bf_row["bf_kommentar"] = row[key_kommentar]
                else:
                    bf_row["bf_kommentar"] = None

                if bf_row["bf_status"] == True or "bf_kommentar" != None:
                    #db.insert_row("kh_bf", bf_row)
                    bf_rows.append(bf_row)

                # Nicht vorhandene Spalten löschen                
                if key in row:
                    del row[key]

                if key_kommentar in row:
                    del row[key_kommentar]

            start = time.time()
            db.insert_row("kh_daten", row)
            print(f"Einfügen von kh_daten dauerte {time.time() - start:.2f} Sekunden")
            #kh_daten_rows.append(row)

        
            # Jetzt Batch-Inserts durchführen
            start = time.time()
            print(f"{len(bf_rows)} Zeilen in 'kh_bf' einfügen.")
            db.insert_rows("kh_bf", bf_rows)
            print(f"Einfügen von kh_bf dauerte {time.time() - start:.2f} Sekunden")

        #start = time.time()
        #print(f"{len(kh_daten_rows)} Zeilen in 'kh_daten' einfügen.")
        #db.insert_rows_copy("kh_daten", kh_daten_rows)
        #print(f"Einfügen von kh_daten dauerte {time.time() - start:.2f} Sekunden")

        # Schließe die Verbindung zur Datenbank
        db.close()

    def save_to_file(self):

        df = pd.DataFrame(self.rows, columns=self.cols)

        # Aktuelles Datum und Uhrzeit als String
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Dateinamen mit Timestamp
        csv_filename = f"output/{ self.cfg.xml_jahr }_{timestamp}.csv"
        excel_filename = f"output/{ self.cfg.xml_jahr }_{timestamp}.xlsx"

        df.to_csv(csv_filename, sep=";")
        df.to_excel(excel_filename)

    def main(self):

        # Get the list of all files and directories
        dir_list = os.listdir(self.cfg.xml_path)

        for filename in dir_list:

            filepath = self.cfg.xml_path + "/" + filename

            row = {}

            if filepath.endswith(".xml"):
                print(filepath)

                # Parsing the XML file
                xmlparse = Xet.parse(filepath)

                row["jahr"] = self.cfg.xml_jahr
                row["filename"] = filename
                row["standortnummer_alt"] = 0 # Key
                row["strasse"] = ""
                row["hausnummer"] = ""
                row["postleitzahl"] = ""
                row["ort"] = ""

                #korrekten Standort in alten Dateien lesen
                standortNr = ""
                if filename[9:10] == "-" and filename[12:13] == "-":
                    standortNr = filename[10:12]
                    row["standortnummer_datei"] = standortNr

                # für 2013 ergänzt
                standort = xmlparse.find("Standort_dieses_Berichts")
                if standort is not None:
                    kontaktdaten = standort.find("Kontaktdaten")
                elif standortNr != "":
                    standort = xmlparse.find("Standorte_des_Krankenhauses")
                    if standort is not None:
                        for node in standort.iter("Kontaktdaten"):
                            elem = node.find("Standortnummer")
                            if elem is not None and elem.text == standortNr:
                                kontaktdaten = node

                if standort is None or kontaktdaten is None:
                    # 2022
                    for node in xmlparse.iter("Krankenhaus"):
                        standort = node.find("Mehrere_Standorte")
                        if standort is None:
                            standort = node.find("Ein_Standort")
                            if standort is not None:
                                kontaktdaten = standort.find("Krankenhauskontaktdaten")
                        else:
                            kontaktdaten = standort.find("Standortkontaktdaten")
                        if standort is None:
                            standort = xmlparse.find("Einziger_Standort")
                            kontaktdaten = node.find("Kontaktdaten")

                if standort is None or kontaktdaten is None:
                    print("Standortfehler")
                else:
                    elem = kontaktdaten.find("Name")
                    if elem is not None:
                        row["name"] = elem.text
                    elem = kontaktdaten.find("IK")
                    if elem is not None:
                        row["ik"] = elem.text
                        match (elem.text[2:4]):
                            case "01":
                                row["bundesland"] = "Schleswig-Holstein"
                            case "02":
                                row["bundesland"] = "Hamburg"
                            case "03":
                                row["bundesland"] = "Niedersachsen"
                            case "04":
                                row["bundesland"] = "Bremen"
                            case "05":
                                row["bundesland"] = "Nordrhein-Westfalen"
                            case "06":
                                row["bundesland"] = "Hessen"
                            case "07":
                                row["bundesland"] = "Rheinland-Pfalz"
                            case "08":
                                row["bundesland"] = "Baden-Württemberg"
                            case "09":
                                row["bundesland"] = "Bayern"
                            case "10":
                                row["bundesland"] = "Saarland"
                            case "11":
                                row["bundesland"] = "Berlin"
                            case "12":
                                row["bundesland"] = "Brandenburg"
                            case "13":
                                row["bundesland"] = "Mecklenburg-Vorpommern"
                            case "14":
                                row["bundesland"] = "Sachsen"
                            case "15":
                                row["bundesland"] = "Sachsen-Anhalt"
                            case "16":
                                row["bundesland"] = "Thüringen"
                            case "00":
                                row["bundesland"] = "Ausland"
                            case _:
                                row["bundesland"] = "Fehler:" + elem.text[2:4]

                    elem = kontaktdaten.find("IK_Weitere")
                    if elem is not None:
                        row["ik_weitere"] = elem.text
                    elem = kontaktdaten.find("Standortnummer")
                    if elem is not None:
                        row["standortnummer"] = elem.text
                    elem = kontaktdaten.find("Standortnummer_alt")
                    if elem is not None:
                        if elem.text.isdigit():
                            row["standortnummer_alt"] = elem.text
                        else:
                            print("Standortnummer_Alt ist nicht numerisch: " + elem.text)

                    kontakt_zugang = kontaktdaten.find("Kontakt_Zugang")
                    if kontakt_zugang is None:
                        kontakt_zugang = kontaktdaten.find("Hausanschrift") # für 2013 ergänzt
                    elem = kontakt_zugang.find("Strasse")
                    if elem is not None:
                        row["strasse"] = elem.text
                    elem = kontakt_zugang.find("Hausnummer")
                    if elem is not None:
                        row["hausnummer"] = elem.text
                    plz = ""
                    elem = kontakt_zugang.find("Postleitzahl")
                    if elem is not None:
                        plz = elem.text
                        row["postleitzahl"] = elem.text

                    ort = ""
                    elem = kontakt_zugang.find("Ort")
                    if elem is not None:
                        ort = elem.text
                        row["ort"] = elem.text

                    if plz != "":
                        # Zur Postleitzahl den Landkreis ermitteln
                        plz_data = self.plz_db.get_plz_data(plz, ort)
                        row["landkreis"] = plz_data.landkreis
                        row["bundesland_plz"] = plz_data.bundesland

                # IK 51 ist nicht relevant
                if str(row["ik"]).startswith("51"):
                    print(f"IK { row["ik"]} wird übersprungen, da 51* nicht relevant")
                    continue

                row["art_nummer"] = 0
                for node in xmlparse.iter("Krankenhaustraeger"):
                    elem = node.find("Name")
                    if elem is not None:
                        row["krankenhaustraeger_name"] = elem.text

                    traeger_art = node.find("Krankenhaustraeger_Art")
                    elem = traeger_art.find("Art")
                    if elem is not None:
                        row["art"] = elem.text
                        match (elem.text):
                            case "freigemeinnützig":
                                row["art_nummer"] = 1
                            case "öffentlich":
                                row["art_nummer"] = 2
                            case "privat":
                                row["art_nummer"] = 3

                    elem = traeger_art.find("Sonstiges")
                    if elem is not None:
                        row["sonstiges"] = elem.text
                        if row["art_nummer"] == "":
                            match(elem.text):
                                case "öffentlich-rechtliche-trägerschaft":
                                    row["art_nummer"] = 2


                row["lehrstatus"] = 0
                row["universitaetsklinikum"] = 0
                kh_art = xmlparse.find("Krankenhaus_Art")
                if kh_art is None:
                    row["lehrstatus"] = 0
                else:
                    row["lehrstatus"] = 1
                    lehrkrankenhaus = kh_art.find("Akademisches_Lehrkrankenhaus")
                    if lehrkrankenhaus is not None:
                        row["akademisches_lehrkrankenhaus"] = ""
                        for node in lehrkrankenhaus:
                            if node.tag == "Name_Universitaet":
                                if row["akademisches_lehrkrankenhaus"] == "":
                                    row["akademisches_lehrkrankenhaus"] = node.text
                                else:
                                    row["akademisches_lehrkrankenhaus"] += ";" + node.text

                    uni = kh_art.find("Universitaetsklinikum")
                    if uni is not None:
                        row["universitaetsklinikum"] = 1

                elem = xmlparse.find("Anzahl_Betten")
                if elem is not None:
                    row["anzahl_betten"] = elem.text

                fallzahlen = xmlparse.find("Fallzahlen")
                elem = fallzahlen.find("Vollstationaere_Fallzahl")
                if elem is not None:
                    row["vollstationaere_fallzahl"] = elem.text

                elem = fallzahlen.find("Teilstationaere_Fallzahl")
                if elem is not None:
                    row["teilstationaere_fallzahl"] = elem.text

                elem = fallzahlen.find("Ambulante_Fallzahl")
                if elem is not None:
                    row["ambulante_fallzahl"] = elem.text

                elem = fallzahlen.find("StaeB_Fallzahl")
                if elem is not None:
                    row["staeb_fallzahl"] = elem.text

                for aspekt in self.bf_aspekte:
                    row[aspekt["bf"]] = 0

                for node in xmlparse.iter("Barrierefreiheit"):
                    for aspekt in node.iter("Barrierefreiheit_Aspekt"):
                        bfkey = aspekt.find("BF_Schluessel").text
                        row[bfkey] = 1

                        if aspekt.find("Erlaeuterungen") is not None:
                            row[bfkey + "_kommentar"] = aspekt.find("Erlaeuterungen").text
                            if (aspekt.find("Erlaeuterungen").text == "nicht vorhanden" or
                                    aspekt.find("Erlaeuterungen").text == "Nicht vorhanden" or
                                    aspekt.find("Erlaeuterungen").text == "nicht bekannt" or
                                    aspekt.find("Erlaeuterungen").text == "nein" or
                                    aspekt.find("Erlaeuterungen").text == "Nein"):
                                row[bfkey] = 0


                row["bf_gesamt_absolut"] = 0
                row["bf_gesamt_cat01"] = 0
                row["bf_gesamt_cat02"] = 0
                row["bf_gesamt_cat03"] = 0
                row["bf_gesamt_cat04"] = 0
                row["bf_gesamt_cat05"] = 0
                row["bf_gesamt_cat06"] = 0
                row["bf_gesamt_cat07"] = 0
                row["bf_gesamt_cat08"] = 0
                for aspekt in self.bf_aspekte:
                    if row[aspekt["bf"]] == 1:
                        row["bf_gesamt_absolut"] += row[aspekt["bf"]] 

                        if aspekt["bf_kategorie_id"] == "CAT01":
                            row["bf_gesamt_cat01"] += row[aspekt["bf"]] 
                        if aspekt["bf_kategorie_id"] == "CAT02":
                            row["bf_gesamt_cat02"] += row[aspekt["bf"]] 
                        if aspekt["bf_kategorie_id"] == "CAT03":
                            row["bf_gesamt_cat03"] += row[aspekt["bf"]] 
                        if aspekt["bf_kategorie_id"] == "CAT04":
                            row["bf_gesamt_cat04"] += row[aspekt["bf"]] 
                        if aspekt["bf_kategorie_id"] == "CAT05":
                            row["bf_gesamt_cat05"] += row[aspekt["bf"]] 
                        if aspekt["bf_kategorie_id"] == "CAT06":
                            row["bf_gesamt_cat06"] += row[aspekt["bf"]] 
                        if aspekt["bf_kategorie_id"] == "CAT07":
                            row["bf_gesamt_cat07"] += row[aspekt["bf"]] 
                        if aspekt["bf_kategorie_id"] == "CAT08":
                            row["bf_gesamt_cat08"] += row[aspekt["bf"]] 


                personal = xmlparse.find("Personal_des_Krankenhauses")
                if personal is not None:
                    spez_tera = personal.find("Spezielles_Therapeutisches_Personal")
                    if spez_tera is not None:
                        for ter_sp in spez_tera.iter("Therapeutisches_Personal"):
                            sp_key = ter_sp.find("SP_Schluessel")
                            if sp_key is not None and sp_key.text == "SP04":
                                personalerfassung = ter_sp.find("Personalerfassung")
                                if personalerfassung is not None:
                                    elem = personalerfassung.find("Anzahl_VK")
                                    if elem is not None:
                                        row["sp04_anzahl_vk"] = elem.text
                                    form = personalerfassung.find("Versorgungsform")
                                    if form is not None:
                                        form_1 = form.find("Ambulante_Versorgung")
                                        if form_1 is not None:
                                            elem = personalerfassung.find("Anzahl_VK")
                                            if elem is not None:
                                                row["sp04_ambulant_anzahl_vk"] = elem.text
                                        form_1 = form.find("Stationaere_Versorgung")
                                        if form_1 is not None:
                                            elem = personalerfassung.find("Anzahl_VK")
                                            if elem is not None:
                                                row["sp04_stationaere_anzahl_vk"] = elem.text

                if "sp04_anzahl_vk" in row:
                    row["sp04_anzahl_vk"] = row["sp04_anzahl_vk"].replace(",",".")

                if "sp04_ambulant_anzahl_vk" in row:
                    row["sp04_ambulant_anzahl_vk"] = row["sp04_ambulant_anzahl_vk"].replace(",",".")

                if "sp04_stationaere_anzahl_vk" in row:
                    row["sp04_stationaere_anzahl_vk"] = row["sp04_stationaere_anzahl_vk"].replace(",",".")

                for elemName in row:
                    if type(row[elemName]) == str:
                        row[elemName] = row[elemName].replace('\n', ' ').replace('\r', '')

                self.rows.append(row)