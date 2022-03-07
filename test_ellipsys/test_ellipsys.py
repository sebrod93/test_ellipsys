''' Python 3.8.12 was used'''

### Import sqlite and Pandas

import pandas as pd
import sqlite3

#Connection and query for the original table retrieval
conn = sqlite3.connect("raw_data/ellipsys_test_db.sqlite")
query = "SELECT * FROM oa_trf_src"

#Storage of the original table as a pandas dataframe
df = pd.read_sql(query, conn)

#New table assignment as a variable and storage of the impact column as an int
oa_trf_src_red = df.astype({"impact": int})

#Dictionary to store the correspondance tables
correspondances = {}

#Reassingment of each of the columns as an int and storage of the correspondance
for column in oa_trf_src_red.columns.drop(['impact']):
    codes, uniques = pd.factorize(oa_trf_src_red[column])
    oa_trf_src_red[column] = codes
    correspondances[f'oa_trf_src_{column}_lkp'] = pd.DataFrame(uniques, columns=["champ"])

#Updating of the database to include the new tables
oa_trf_src_red.to_sql(name='oa_trf_src_red', con=conn, index=False)

for key, table in correspondances.items():
    table.to_sql(name=key, con=conn, index_label="id")
