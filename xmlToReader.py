import xml.etree.ElementTree as Xet
import requests
import os
import io
import json
import pandas as pd
from datetime import datetime

from docutils.nodes import TextElement
#from salt.states.dellchassis import switch

from dbConnection import PostgreSQLConnection
from PlzData import PlzDatabase
from config import Config

class xmlToReader:
    def __init__(self):

        self.cfg = Config()

        self.cols = ["jahr", "name", "ik", "ik_weitere", "standortnummer", "standortnummer_datei", "standortnummer_alt",
                "strasse", "hausnummer", "postleitzahl", "ort", "bundesland", "bundesland_plz", "landkreis",
                "krankenhaustraeger_name", "art", "art_nummer", "sonstiges",
                "lehrstatus", "akademisches_lehrkrankenhaus", "universitaetsklinikum",
                "anzahl_betten",
                "vollstationaere_fallzahl", "teilstationaere_fallzahl", "ambulante_fallzahl", "staeb_fallzahl",
                "sp04_anzahl_vk", "sp04_ambulant_anzahl_vk", "sp04_stationaere_anzahl_vk", "filename"]
        for i in range(1, 43):
            col_name = "bf" + '{:02}'.format(i)
            self.cols.append(col_name)
            self.cols.append(col_name + "_kommentar")
        self.rows = []

        self.plz_db = PlzDatabase()

    def save_to_db(self):
        db = PostgreSQLConnection(self.cfg.dbname, self.cfg.dbuser, self.cfg.dbpassword, self.cfg.dbhost, self.cfg.dbport)
    
        # Stelle eine Verbindung zur Datenbank her
        db.connect()
        
        # Erstelle die Tabelle
        db.create_table()

        db.delete_row("kh_daten", {
            "jahr": self.cfg.xml_jahr
        })

        for row in self.rows:
            # Zeile einfügen
            db.insert_row("kh_daten", row)
        
        # Schließe die Verbindung zur Datenbank
        db.close()

    def save_to_file(self):

        df = pd.DataFrame(rows, columns=self.cols)

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

                row["art_nummer"] = ""
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
                                row["art_nummer"] = "1"
                            case "öffentlich":
                                row["art_nummer"] = "2"
                            case "privat":
                                row["art_nummer"] = "3"

                    elem = traeger_art.find("Sonstiges")
                    if elem is not None:
                        row["sonstiges"] = elem.text
                        if row["art_nummer"] == "":
                            match(elem.text):
                                case "öffentlich-rechtliche-trägerschaft":
                                    row["art_nummer"] = "2"


                kh_art = xmlparse.find("krankenhaus_art")
                if kh_art is None:
                    row["lehrstatus"] = "0"
                else:
                    row["lehrstatus"] = "1"
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
                        row["universitaetsklinikum"] = "1"

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

                for i in range(1, 43):
                    row["bf" + '{:02}'.format(i)] = 0

                for node in xmlparse.iter("Barrierefreiheit"):
                    for aspekt in node.iter("Barrierefreiheit_Aspekt"):
                        bfkey = aspekt.find("BF_Schluessel").text.lower()
                        row[bfkey] = "1"

                        if aspekt.find("Erlaeuterungen") is not None:
                            row[bfkey + "_kommentar"] = aspekt.find("Erlaeuterungen").text
                            if (aspekt.find("Erlaeuterungen").text == "nicht vorhanden" or
                                    aspekt.find("Erlaeuterungen").text == "Nicht vorhanden" or
                                    aspekt.find("Erlaeuterungen").text == "nicht bekannt" or
                                    aspekt.find("Erlaeuterungen").text == "nein" or
                                    aspekt.find("Erlaeuterungen").text == "Nein"):
                                row[bfkey] = "0"

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