# -*- coding: utf-8 -*-
"""FIUBA - Distribuidos 1 - Steam Analysis 94cd69

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/#fileId=https%3A//storage.googleapis.com/kaggle-colab-exported-notebooks/fiuba-distribuidos-1-steam-analysis-94cd69-1339507c-5a27-4cd7-9b60-60e62b618a0d.ipynb%3FX-Goog-Algorithm%3DGOOG4-RSA-SHA256%26X-Goog-Credential%3Dgcp-kaggle-com%2540kaggle-161607.iam.gserviceaccount.com/20241019/auto/storage/goog4_request%26X-Goog-Date%3D20241019T012059Z%26X-Goog-Expires%3D259200%26X-Goog-SignedHeaders%3Dhost%26X-Goog-Signature%3D6cc339cb2123b16f6b2fc6b0bbf9a7b3688095132ea95621010e86f50ca286699a3b7f2274920c6db82f4842e33fa982e5523ccfc1a8cce8f8a64bf8ff8e7faa9b2489ed28cbd3a7410c29698404fab3da2a4131b59835cc1d937d3734f6dde709272ff34bb9ee1d7e000bd95becdc1cc5273cd19d564905ca0e6cdfdc7a6e4d500427ec9e9e3e1b500bb8c8695a38acf1063f0e1808ea3f89d92b1b33eeece798bb915bcbf60b110dc3d721f3e9aaf07abb370fcdf3525ac2de9a1eee9cc5e5261dfa7ad8a5abf9803747c78fca54f44123256ac9d62f1c784cd7911c4ba0dbba6e9f84509b33fca14a840048a5d23ee16417958b4e50e4b4268771553513a2
"""

"""# Data load, clean and join"""

import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import os
import langid
import time

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 100)

games_df = pd.read_csv("./data/games.csv", header=None, skiprows=1)
reviews_df = pd.read_csv("./data/filtered_reviews.csv")

# Agrego columna "Unknown" ya que no viene nomenclada y genera un desfasaje en los indices de columnas
games_df_column_names = [
    "AppID",
    "Name",
    "Release date",
    "Estimated owners",
    "Peak CCU",
    "Required age",
    "Price",
    "Unknown",
    "DiscountDLC count",
    "About the game",
    "Supported languages",
    "Full audio languages",
    "Reviews",
    "Header image",
    "Website",
    "Support url",
    "Support email",
    "Windows",
    "Mac",
    "Linux",
    "Metacritic score",
    "Metacritic url",
    "User score",
    "Positive",
    "Negative",
    "Score rank",
    "Achievements",
    "Recommendations",
    "Notes",
    "Average playtime forever",
    "Average playtime two weeks",
    "Median playtime forever",
    "Median playtime two weeks",
    "Developers",
    "Publishers",
    "Categories",
    "Genres",
    "Tags",
    "Screenshots",
    "Movies",
]
games_df.columns = games_df_column_names

games_df.shape

reviews_df.shape

games_df.dtypes

reviews_df.dtypes

games_df.head()

reviews_df.head()

games_df["AppID"].nunique()

reviews_df["app_id"].nunique()

reviews_df["review_score"].value_counts()

# Dataframes cleaning

games_df_columns = [
    "AppID",
    "Name",
    "Windows",
    "Mac",
    "Linux",
    "Genres",
    "Release date",
    "Average playtime forever",
    "Positive",
    "Negative",
]
reviews_df_columns = ["app_id", "review_text", "review_score"]

games_df_cleaned = games_df.dropna(subset=games_df_columns)[games_df_columns].copy()
reviews_df_cleaned = reviews_df.dropna(subset=reviews_df_columns)[
    reviews_df_columns
].copy()

games_df_cleaned["Genres"] = games_df_cleaned["Genres"].str.lower()

reviews_df_cleaned["review_text"] = reviews_df_cleaned["review_text"].astype(str)

"""# Queries resolution

## Q1: Cantidad de juegos soportados en cada plataforma (Windows, Linux, MAC)
"""

windows_supported_games = games_df_cleaned[games_df_cleaned["Windows"] == True]
linux_supported_games = games_df_cleaned[games_df_cleaned["Linux"] == True]
mac_supported_games = games_df_cleaned[games_df_cleaned["Mac"] == True]

data = {
    "PLATFORM": ["WINDOWS", "LINUX", "MAC"],
    "COUNT": [
        windows_supported_games.shape[0],
        linux_supported_games.shape[0],
        mac_supported_games.shape[0],
    ],
}
platform_df = pd.DataFrame(data)

platform_df.to_csv(
    "./data/results/q1_expected.csv",
    sep=",",
    encoding="utf-8",
    index=False,
    header=False,
)

"""## Q2: Nombre del top 10 de juegos del género "Indie" publicados en la década del 2010 con más tiempo promedio histórico de juego"""

games_indie = games_df_cleaned[games_df_cleaned["Genres"].str.contains("indie")]

games_indie_2010_decade = games_indie[games_indie["Release date"].str.contains("201")]

games_indie.shape

games_indie_2010_decade.shape

q2_result = games_indie_2010_decade.sort_values(
    by="Average playtime forever", ascending=False
).head(10)

q2_result[["Name", "Average playtime forever"]].to_csv(
    "./data/results/q2_expected.csv",
    sep=",",
    encoding="utf-8",
    index=False,
    header=False,
)

"""## Q3: Nombre de top 5 juegos del género "Indie" con más reseñas positivas"""

games_indie_reduced = games_indie[["AppID", "Name"]]
games_indie_reduced.head()

reviews_reduced_q3 = reviews_df_cleaned[["app_id", "review_score"]]

games_indie_reviews = pd.merge(
    games_indie_reduced,
    reviews_reduced_q3,
    left_on="AppID",
    right_on="app_id",
    how="inner",
)


def positive_score(score):
    return 1 if score > 0 else 0


games_indie_reviews["positive_score"] = games_indie_reviews["review_score"].apply(
    positive_score
)

q3_result = (
    games_indie_reviews.groupby("Name")["positive_score"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
)

q3_result = q3_result.reset_index()

q3_result.to_csv(
    "./data/results/q3_expected.csv",
    sep=",",
    encoding="utf-8",
    index=False,
    header=False,
)

"""## Q4: Nombre de juegos del género "action" con más de 5.000 reseñas negativas en idioma inglés"""

# Juegos de acción
games_action = games_df_cleaned[games_df_cleaned["Genres"].str.contains("action")]
games_action_reduced = games_action[["AppID", "Name"]]
games_action_reduced.shape

reviews_q4 = reviews_df_cleaned.copy()

# Reviews con mas de 5000 comentarios negativos


def negative_score(score):
    return 1 if score < 0 else 0


reviews_q4["negative_score"] = reviews_q4["review_score"].apply(negative_score)
reviews_q4_negatives = reviews_q4[reviews_q4["negative_score"] == 1].copy()
reviews_count = reviews_q4_negatives.groupby("app_id").size().reset_index(name="count")
reviews_count_more_than_5000 = reviews_count[reviews_count["count"] > 5000]
reviews_count_more_than_5000.shape

# De las reviews con mas de 5000 comentarios negativos, nos quedamos con aquellas que sean sobre juegos de acción
games_action_with_5000_negative_reviews = pd.merge(
    games_action_reduced,
    reviews_count_more_than_5000,
    left_on="AppID",
    right_on="app_id",
    how="inner",
)
games_action_with_5000_negative_reviews = games_action_with_5000_negative_reviews[
    ["AppID", "Name"]
]
games_action_with_5000_negative_reviews.shape

# Enriquecemos con el texto de la review
reviews_count_more_than_5000_with_text = pd.merge(
    reviews_q4_negatives,
    games_action_with_5000_negative_reviews,
    left_on="app_id",
    right_on="AppID",
    how="inner",
)
reviews_count_more_than_5000_with_text = reviews_count_more_than_5000_with_text[
    ["app_id", "review_text"]
]
reviews_count_more_than_5000_with_text.shape


# CPU INTENSIVE #############################
def detect_language(texto):
    language, _ = langid.classify(texto)
    return language


#############################################

# Calculo del idioma sobre las reviews
start_time = time.time()
reviews_count_more_than_5000_with_text["review_language"] = (
    reviews_count_more_than_5000_with_text["review_text"].apply(detect_language)
)
elapsed_time = time.time() - start_time
print(
    f"Execution time on {reviews_count_more_than_5000_with_text.shape[0]} rows: {elapsed_time:.2f} seconds"
)

reviews_count_more_than_5000_with_text.shape

# Nos quedamos con aquellas reviews que estan en idioma inglés
reviews_count_more_than_5000_with_text_english = reviews_count_more_than_5000_with_text[
    reviews_count_more_than_5000_with_text["review_language"] == "en"
]
reviews_count_more_than_5000_with_text_english.shape

# Nos quedamos con aquellos juegos que tengan mas de 5000 reseñas negativas en inglés
q4_results_app_ids = (
    reviews_count_more_than_5000_with_text_english.groupby("app_id")
    .size()
    .reset_index(name="count")
)
q4_results_app_ids = q4_results_app_ids[q4_results_app_ids["count"] > 5000]

# Enriquecemos con el nombre de esos juegos
q4_results_games_names = pd.merge(
    q4_results_app_ids,
    games_action_with_5000_negative_reviews,
    left_on="app_id",
    right_on="AppID",
    how="inner",
)["Name"]
q4_results_games_names.to_csv(
    "./data/results/q4_expected.csv",
    sep=",",
    encoding="utf-8",
    index=False,
    header=False,
)

"""## Q5: Nombre de juegos del género "action" dentro del percentil 90 en cantidad de reseñas negativas"""

games_action_reduced.head()

games_action_reduced.shape

reviews_q5 = reviews_df_cleaned.copy()
reviews_q5 = reviews_q5[["app_id", "review_score"]]
reviews_q5["negative_score"] = reviews_q5["review_score"].apply(negative_score)
reviews_q5_negative_score = reviews_q5[reviews_q5["negative_score"] == 1]
reviews_q5_negative_score.shape

reviews_q5_negative_score_action = pd.merge(
    reviews_q5_negative_score,
    games_action_reduced,
    left_on="app_id",
    right_on="AppID",
    how="inner",
)
reviews_q5_negative_score_action.shape

reviews_q5_negative_score_action_by_app_id = (
    reviews_q5_negative_score_action.groupby("app_id").size().reset_index(name="count")
)
reviews_q5_negative_score_action_by_app_id.shape

percentil_90 = reviews_q5_negative_score_action_by_app_id["count"].quantile(0.90)
percentil_90

q5_result = reviews_q5_negative_score_action_by_app_id[
    reviews_q5_negative_score_action_by_app_id["count"] >= percentil_90
]
q5_result.shape

q5_result_with_game_names = pd.merge(
    q5_result, games_action_reduced, left_on="app_id", right_on="AppID", how="inner"
)
q5_result_with_game_names[["Name", "count"]].to_csv(
    "./data/results/q5_expected.csv",
    sep=",",
    encoding="utf-8",
    index=False,
    header=False,
)
