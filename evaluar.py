import sqlite3
import pandas as pd
import os
import math
import recomendar

def ndcg(groud_truth, recommendation):
    dcg = 0
    idcg = 0
    for i, r in enumerate(recommendation):
        rel = int(r in groud_truth)
        dcg += rel / math.log2(i+1+1)
        idcg += 1 / math.log2(i+1+1)
    return dcg / idcg

def precision_at(ground_truth, recommendation, n=9):
    return len(set(ground_truth[:n-1]).intersection(recommendation[:len(ground_truth[:n-1])])) / len(ground_truth[:n-1])

users = recomendar.sql_select("SELECT DISTINCT username FROM users")

for row in users:
    games_rated = [row["name"] for row in recomendar.sql_select("SELECT DISTINCT name FROM ratings WHERE username = ?", (row["username"],))]
    recomendacion = recomendar.recomendar(row["id_lector"], interacciones="interacciones_test")
    p = precision_at(games_rated, recomendacion)
    n = ndcg(games_rated, recomendacion)
    print(f"{row['username']}\t\tndcg: {m:.5f}\tprecision@9: {p: .5f}")

