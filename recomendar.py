import sqlite3
import pandas as pd
import os

import lightfm as lfm
from lightfm import data
from lightfm import cross_validation
from lightfm import evaluation
import surprise as sp

import whoosh as wh
from whoosh import fields
from whoosh import index
from whoosh import qparser

THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))
DATABASE = os.path.join(THIS_FOLDER, "bgg_2000.db")

def sql_execute(query, params=None):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()    
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)
    
    con.commit()
    con.close()
    return

def sql_select(query, params=None):
    con = sqlite3.connect(DATABASE)
    con.row_factory = sqlite3.Row
    cur = con.cursor()    
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)
    
    ret = res.fetchall()
    con.close()
    return ret

def crear_usuario(username):
    query = "INSERT INTO users(username) VALUES (?) ON CONFLICT DO NOTHING;"
    sql_execute(query, (username,))
    return

"""
def insertar_interacciones(id_game, username, rating, interacciones="ratings"):
    query = f"INSERT INTO {interacciones}(id, username, rating) VALUES (?, ?, ?) ON CONFLICT(id, username) DO UPDATE SET rating=?;"
    sql_execute(query, (id_game, username, rating, rating))
    return
"""
def insertar_interacciones(id_game, username, rating, interacciones="ratings"):
    if id_game is not None and username is not None and rating is not None:
        query = f"""
            INSERT INTO {interacciones} (id, username, rating) 
            VALUES (?, ?, ?) 
            ON CONFLICT(id, username) DO UPDATE SET rating=excluded.rating;
        """
        sql_execute(query, (id_game, username, rating))
    return

def reset_usuario(username, interacciones="ratings"):
    query = f"DELETE FROM {interacciones} WHERE username = ?;"
    sql_execute(query, (username,))
    return

def obtener_juego(id_game):
    query = "SELECT * FROM games WHERE id = ?;"
    game = sql_select(query, (id_game,))[0]
    return game

def valorados(username, interacciones="ratings"):
    query = f"SELECT * FROM {interacciones} WHERE username = ? AND rating > 1"
    valorados = sql_select(query, (username,))
    return valorados

def ignorados(username, interacciones="ratings"):
    query = f"SELECT * FROM {interacciones} WHERE username = ? AND rating = 1"
    ignorados = sql_select(query, (username,))
    return ignorados

def datos_juegos(id_games):
    query = f"SELECT DISTINCT * FROM games WHERE id IN ({','.join(['?']*len(id_games))})"
    juegos = sql_select(query, id_games)
    return juegos

def recomendar_top_9(username, interacciones="ratings"):
    query = f"""
        SELECT id, AVG(rating) as rating, count(*) AS cant
          FROM {interacciones}
         WHERE id NOT IN (SELECT id FROM {interacciones} WHERE username = ?)
           AND rating > 0
         GROUP BY 1
         ORDER BY 3 DESC, 2 DESC
         LIMIT 9
    """
    id_juegos = [r["id"] for r in sql_select(query, (username,))]
    return id_juegos

def recomendar_perfil(username, interacciones="ratings"):
    con = sqlite3.connect(DATABASE)
    df_int = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_items = pd.read_sql_query("SELECT * FROM games", con)
    con.close()

    perf_items = pd.get_dummies(df_items[["id", "genre"]], columns=["genre"]).set_index("id")

    perf_usuario = df_int[(df_int["username"] == username) & (df_int["rating"] > 0)].merge(perf_items, on="id")

    for c in perf_usuario.columns:
        if c.startswith("genre_"):
            perf_usuario[c] = perf_usuario[c] * perf_usuario["rating"]

    perf_usuario = perf_usuario.drop(columns=["id", "rating"]).groupby("username").mean()
    perf_usuario = perf_usuario / perf_usuario.sum(axis=1)[0]
    for g in perf_items.columns:
        perf_items[g] = perf_items[g] * perf_usuario[g][0]

    juegos_leidos_o_vistos = df_int.loc[df_int["username"] == username, "id"].tolist()
    recomendaciones = [l for l in perf_items.sum(axis=1).sort_values(ascending=False).index if l not in juegos_leidos_o_vistos][:9]
    return recomendaciones

def recomendar_lightfm(username, interacciones="ratings"):
    con = sqlite3.connect(DATABASE)
    df_int = pd.read_sql_query(f"SELECT * FROM {interacciones} WHERE rating > 1", con)
    df_items = pd.read_sql_query("SELECT * FROM games", con)
    con.close()

    ds = lfm.data.Dataset()
    ds.fit(users=df_int["username"].unique(), items=df_items["id"].unique())
    
    user_id_map, user_feature_map, item_id_map, item_feature_map = ds.mapping()
    (interactions, weights) = ds.build_interactions(df_int[["username", "id", "rating"]].itertuples(index=False))

    model = lfm.LightFM(no_components=20, k=5, n=10, learning_schedule='adagrad', loss='logistic', learning_rate=0.05, rho=0.95, epsilon=1e-06, item_alpha=0.0, user_alpha=0.0, max_sampled=10, random_state=42)
    model.fit(interactions, sample_weight=weights, epochs=10)

    juegos_leidos = df_int.loc[df_int["username"] == username, "id"].tolist()
    todos_los_juegos = df_items["id"].tolist()
    juegos_no_leidos = set(todos_los_juegos).difference(juegos_leidos)
    predicciones = model.predict(user_id_map[username], [item_id_map[l] for l in juegos_no_leidos])

    recomendaciones = sorted([(p, l) for (p, l) in zip(predicciones, juegos_no_leidos)], reverse=True)[:9]
    recomendaciones = [juego[1] for juego in recomendaciones]
    return recomendaciones

def recomendar_surprise(username, interacciones="ratings"):
    con = sqlite3.connect(DATABASE)
    df_int = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_items = pd.read_sql_query("SELECT * FROM games", con)
    con.close()
    
    reader = sp.reader.Reader(rating_scale=(1, 10))

    data = sp.dataset.Dataset.load_from_df(df_int.loc[df_int["rating"] > 0, ['username', 'id', 'rating']], reader)
    trainset = data.build_full_trainset()
    model = sp.prediction_algorithms.matrix_factorization.SVD(n_factors=500, n_epochs=20, random_state=42)
    model.fit(trainset)

    juegos_leidos_o_vistos = df_int.loc[df_int["username"] == username, "id"].tolist()
    todos_los_juegos = df_items["id"].tolist()
    juegos_no_leidos_ni_vistos = set(todos_los_juegos).difference(juegos_leidos_o_vistos)
    
    predicciones = [model.predict(username, l).est for l in juegos_no_leidos_ni_vistos]
    recomendaciones = sorted([(p, l) for (p, l) in zip(predicciones, juegos_no_leidos_ni_vistos)], reverse=True)[:9]
    recomendaciones = [juego[1] for juego in recomendaciones]
    return recomendaciones

def recomendar(username, metodo="surprise"):
    cant_juegos_valorados = len(valorados(username))

    if cant_juegos_valorados > 5 and cant_juegos_valorados <= 20:
        id_recomendados = recomendar_perfil(username)
        metodo_utilizado = "Recomendación basada en perfil de usuario"
    elif cant_juegos_valorados > 20:
        id_recomendados = recomendar_lightfm(username)
        metodo_utilizado = "Recomendación usando LightFM"
    else:
        id_recomendados = recomendar_top_9(username)
        metodo_utilizado = "Recomendación top-9"
    #elif cant_juegos_valorados <= 5:
    #    id_recomendados = recomendar_top_9(username)
    #    metodo_utilizado = "Recomendación top-9"
    #else:
    #    id_recomendados = recomendar_surprise(username)
    #    metodo_utilizado = "Recomendación usando Surprise"

    juegos_recomendados = datos_juegos(id_recomendados)
    return juegos_recomendados, metodo_utilizado