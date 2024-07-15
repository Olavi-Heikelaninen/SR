import sqlite3
import os

import pandas as pd

import whoosh as wh
from whoosh import fields
from whoosh import index
from whoosh import qparser

THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))

con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/bgg_2000.db"))
df_g = pd.read_sql_query("SELECT * FROM games", con)
con.close()

col_list = ['id', 'name', 'yearpublished', 'rank', 'bayesaverage', 'average',
            'usersrated', 'abstracts_rank', 'cgs_rank', 'childrensgames_rank',
            'familygames_rank', 'partygames_rank', 'strategygames_rank',
            'thematic_rank', 'wargames_rank']

df_g[col_list] = df_g[col_list].fillna(" ")

# TODO: ver field_boost en wh.fields
schema = wh.fields.Schema(
    name=wh.fields.ID(stored=True)
)

ix = wh.index.create_in("indexdir", schema)

writer = ix.writer()
for index, row in df_g.iterrows():
    writer.add_document(id_game=row["id"]
    )
writer.commit()


terminos = [wh.query.Term("name")]
query = wh.query.Or(terminos)

with ix.searcher() as searcher:
    results = searcher.search(query, terms=True)
    for r in results:
        print(r)
