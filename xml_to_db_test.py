from xmlToReader import xmlToReader

reader = xmlToReader()


data = {
    'jahr': 2023,
    'name': 'Beispielname',
    'ik': 12345,
    'ik_weitere': 67890,
    'standortnummer': 0,
    'standortnummer_alt': 0
}

for i, aspekt in enumerate(reader.bf_aspekte):
    # 1 und 0 abwechselnd
    data[aspekt["bf"]] = i % 2

reader.rows.append(data)

# Damit nicht die falschen Daten aus der Datenbank gelöscht werden
reader.cfg.xml_jahr = data["jahr"]

reader.save_to_db()
#reader.save_to_file()
            

        



