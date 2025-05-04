# Importing the required libraries
import xml.etree.ElementTree as Xet
import pandas as pd
import requests
import os
import io
import json

from docutils.nodes import TextElement
from salt.states.dellchassis import switch

plz_db = {}

class PlzData(object):
    def __init__(self):
        self.landkreis = ""
        self.bundesland = ""

def get_plz_data(iv_plz, iv_ort):
    plz_data = PlzData()

    data = plz_db.get(iv_plz)

    if data is None:
        url = "https://openplzapi.org/de/Localities?postalCode="+iv_plz

        response = requests.get(url)

        data = response.json()

        plz_db[iv_plz] = data


    for elem in data:
        try:
            ort_tmp = elem["name"]
        except:
            ort_tmp = ""

        if ort_tmp == iv_ort:
            try:
                plz_data.landkreis = elem["district"]["name"]
            except:
                plz_data.landkreis = "error"

            try:
                plz_data.bundesland = data[0]["federalState"]["name"]
            except:
                plz_data.bundesland = "error"

    if plz_data.landkreis == "":
        try:
            plz_data.landkreis = data[0]["district"]["name"]
        except:
            plz_data.landkreis = "error"

        try:
            plz_data.bundesland = data[0]["federalState"]["name"]
        except:
            plz_data.bundesland = "error"

    return plz_data

def save_plz():
    # Write JSON file
    with open("plz_data.json", "w", encoding='utf8') as outfile:
        json.dump(plz_db, outfile)

def read_plz():
    try:
        with open("plz_data.json", "r") as data_file:
            return json.load(data_file)
    except:
        return {}



cols = ["Name", "IK", "IK_Weitere", "Standortnummer", "Standortnummer_datei", "Standortnummer_alt",
        "Strasse", "Hausnummer", "Postleitzahl", "Ort", "Bundesland", "Bundesland_PLZ", "Landkreis",
        "Krankenhaustraeger_Name", "Art", "Art_Nummer", "Sonstiges",
        "Lehrstatus", "Akademisches_Lehrkrankenhaus", "Universitaetsklinikum",
        "Anzahl_Betten",
        "Vollstationaere_Fallzahl", "Teilstationaere_Fallzahl", "Ambulante_Fallzahl", "StaeB_Fallzahl",
        "SP04_Anzahl_VK", "SP04_Ambulant_Anzahl_VK", "SP04_Stationaere_Anzahl_VK"]
for i in range(1, 43):
    col_name = "BF" + '{:02}'.format(i)
    cols.append(col_name)
    cols.append(col_name + "_kommentar")
rows = []
row = {}


plz_db = read_plz()

# Get the list of all files and directories
path = "2022"
dir_list = os.listdir(path)

for filename in dir_list:

    filepath = path + "/" + filename

    row = {}

    if filepath.endswith(".xml"):
        print(filepath)

        # Parsing the XML file
        xmlparse = Xet.parse(filepath)

        #korrekten Standort in alten Dateien lesen
        standortNr = ""
        if filename[9:10] == "-" and filename[12:13] == "-":
            standortNr = filename[10:12]
            row["Standortnummer_datei"] = standortNr

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
                row[elem.tag] = elem.text
            elem = kontaktdaten.find("IK")
            if elem is not None:
                row[elem.tag] = elem.text
                match (elem.text[2:4]):
                    case "01":
                        row["Bundesland"] = "Schleswig-Holstein"
                    case "02":
                        row["Bundesland"] = "Hamburg"
                    case "03":
                        row["Bundesland"] = "Niedersachsen"
                    case "04":
                        row["Bundesland"] = "Bremen"
                    case "05":
                        row["Bundesland"] = "Nordrhein-Westfalen"
                    case "06":
                        row["Bundesland"] = "Hessen"
                    case "07":
                        row["Bundesland"] = "Rheinland-Pfalz"
                    case "08":
                        row["Bundesland"] = "Baden-Württemberg"
                    case "09":
                        row["Bundesland"] = "Bayern"
                    case "10":
                        row["Bundesland"] = "Saarland"
                    case "11":
                        row["Bundesland"] = "Berlin"
                    case "12":
                        row["Bundesland"] = "Brandenburg"
                    case "13":
                        row["Bundesland"] = "Mecklenburg-Vorpommern"
                    case "14":
                        row["Bundesland"] = "Sachsen"
                    case "15":
                        row["Bundesland"] = "Sachsen-Anhalt"
                    case "16":
                        row["Bundesland"] = "Thüringen"
                    case "00":
                        row["Bundesland"] = "Ausland"
                    case _:
                        row["Bundesland"] = "Fehler:" + elem.text[2:4]

            elem = kontaktdaten.find("IK_Weitere")
            if elem is not None:
                row[elem.tag] = elem.text
            elem = kontaktdaten.find("Standortnummer")
            if elem is not None:
                row[elem.tag] = elem.text
            elem = kontaktdaten.find("Standortnummer_alt")
            if elem is not None:
                row[elem.tag] = elem.text

            kontakt_zugang = kontaktdaten.find("Kontakt_Zugang")
            if kontakt_zugang is None:
                kontakt_zugang = kontaktdaten.find("Hausanschrift") # für 2013 ergänzt
            elem = kontakt_zugang.find("Strasse")
            if elem is not None:
                row[elem.tag] = elem.text
            elem = kontakt_zugang.find("Hausnummer")
            if elem is not None:
                row[elem.tag] = elem.text
            plz = ""
            elem = kontakt_zugang.find("Postleitzahl")
            if elem is not None:
                plz = elem.text
                row[elem.tag] = elem.text

            ort = ""
            elem = kontakt_zugang.find("Ort")
            if elem is not None:
                ort = elem.text
                row[elem.tag] = elem.text

            if plz != "":
                # Zur Postleitzahl den Landkreis ermitteln
                plz_data = get_plz_data(plz, ort)
                row["Landkreis"] = plz_data.landkreis
                row["Bundesland_PLZ"] = plz_data.bundesland

        row["Art_Nummer"] = ""
        for node in xmlparse.iter("Krankenhaustraeger"):
            elem = node.find("Name")
            if elem is not None:
                row["Krankenhaustraeger_Name"] = elem.text

            traeger_art = node.find("Krankenhaustraeger_Art")
            elem = traeger_art.find("Art")
            if elem is not None:
                row[elem.tag] = elem.text
                match (elem.text):
                    case "freigemeinnützig":
                        row["Art_Nummer"] = "1"
                    case "öffentlich":
                        row["Art_Nummer"] = "2"
                    case "privat":
                        row["Art_Nummer"] = "3"

            elem = traeger_art.find("Sonstiges")
            if elem is not None:
                row[elem.tag] = elem.text
                if row["Art_Nummer"] == "":
                    match(elem.text):
                        case "öffentlich-rechtliche-trägerschaft":
                            row["Art_Nummer"] = "2"


        kh_art = xmlparse.find("Krankenhaus_Art")
        if kh_art is None:
            row["Lehrstatus"] = "0"
        else:
            row["Lehrstatus"] = "1"
            lehrkrankenhaus = kh_art.find("Akademisches_Lehrkrankenhaus")
            if lehrkrankenhaus is not None:
                row["Akademisches_Lehrkrankenhaus"] = ""
                for node in lehrkrankenhaus:
                    if node.tag == "Name_Universitaet":
                        if row["Akademisches_Lehrkrankenhaus"] == "":
                            row["Akademisches_Lehrkrankenhaus"] = node.text
                        else:
                            row["Akademisches_Lehrkrankenhaus"] += ";" + node.text

            uni = kh_art.find("Universitaetsklinikum")
            if uni is not None:
                row["Universitaetsklinikum"] = "1"

        elem = xmlparse.find("Anzahl_Betten")
        if elem is not None:
            row[elem.tag] = elem.text

        fallzahlen = xmlparse.find("Fallzahlen")
        elem = fallzahlen.find("Vollstationaere_Fallzahl")
        if elem is not None:
            row[elem.tag] = elem.text

        elem = fallzahlen.find("Teilstationaere_Fallzahl")
        if elem is not None:
            row[elem.tag] = elem.text

        elem = fallzahlen.find("Ambulante_Fallzahl")
        if elem is not None:
            row[elem.tag] = elem.text

        elem = fallzahlen.find("StaeB_Fallzahl")
        if elem is not None:
            row[elem.tag] = elem.text

        for i in range(1, 43):
            row["BF" + '{:02}'.format(i)] = 0

        for node in xmlparse.iter("Barrierefreiheit"):
            for aspekt in node.iter("Barrierefreiheit_Aspekt"):
                bfkey = aspekt.find("BF_Schluessel").text
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
                                row["SP04_Anzahl_VK"] = elem.text
                            form = personalerfassung.find("Versorgungsform")
                            if form is not None:
                                form_1 = form.find("Ambulante_Versorgung")
                                if form_1 is not None:
                                    elem = personalerfassung.find("Anzahl_VK")
                                    if elem is not None:
                                        row["SP04_Ambulant_Anzahl_VK"] = elem.text
                                form_1 = form.find("Stationaere_Versorgung")
                                if form_1 is not None:
                                    elem = personalerfassung.find("Anzahl_VK")
                                    if elem is not None:
                                        row["SP04_Stationaere_Anzahl_VK"] = elem.text

        for elemName in row:
            if type(row[elemName]) == str:
                row[elemName] = row[elemName].replace('\n', ' ').replace('\r', '')

        rows.append(row)



df = pd.DataFrame(rows, columns=cols)

# Writing dataframe to csv
save_plz()
df.to_csv('output.csv', sep=";")
df.to_excel('output.xlsx')

