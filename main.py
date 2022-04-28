from bs4 import BeautifulSoup
import pandas as pd
import requests
import psycopg2


conn = psycopg2.connect(database="postgres", user="postgres", password="Alex0601*", host="127.0.0.1", port="5432")
print("Database Connected....")

cursor = conn.cursor()

cursor.execute("DROP TABLE bridges")
conn.commit()

cursor.execute("""  
CREATE TABLE IF NOT EXISTS bridges(
     id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
     name TEXT,
     type TEXT,
     date TEXT,
     city TEXT,
     localisation TEXT,
     region TEXT
)
""" )
conn.commit()

page = requests.get("https://fr.wikipedia.org/wiki/Liste_de_ponts_d%27Italie")

soup = BeautifulSoup(page.text, 'html.parser')


for table in soup.find_all("table"):
    table_body = table.find("tbody")
    for row in table_body.find_all("tr"):
        if row.find_all('td'):
            if len(row.find_all('td')) > 8:
                name_bridge = row.findAll('td')[2].text
                type_bridge = row.findAll('td')[5].text
                date_bridge = row.findAll('td')[7].text
                loc_column = row.findAll('td')[8]
                city_bridge = loc_column.find("a").text
                loc_bridge = "[" + str(loc_column.find_all("a")[1].get("data-lat")) + "," + str(loc_column.find_all("a")[1].get("data-lon")) + "]"
                region_bridge = row.findAll('td')[9].text
                if city_bridge == "Gênes":
                    cursor.execute("INSERT INTO bridges(name, type, date, city, localisation, region) VALUES (%s,%s,%s,%s,%s,%s)",[name_bridge,type_bridge,date_bridge,city_bridge,loc_bridge,region_bridge])
                    conn.commit()


cursor.execute("""SELECT * FROM bridges""")
bridge1 = cursor.fetchall()
print(bridge1)

conn.close()



