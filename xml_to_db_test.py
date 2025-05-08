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

for i in range(1, 43):
    # 1 und 0 abwechselnd
    data[xmlToReader.C_COLNAME_BF + '{:02}'.format(i)] = i % 2

reader.rows.append(data)

# Damit nicht die falschen Daten aus der Datenbank gelöscht werden
reader.cfg.xml_jahr = data["jahr"]

reader.save_to_db()
#reader.save_to_file()
            

        



