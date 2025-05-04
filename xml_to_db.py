from xmlToReader import xmlToReader



reader = xmlToReader()

reader.main()

# data = {
#     'jahr': 2023,
#     'name': 'Beispielname',
#     'ik': 12345,
#     'ik_weitere': 67890,
#     'standortnummer_alt': 0
# }

#reader.rows.append(data)

reader.save_to_db()
            

        



