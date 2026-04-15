# الفيشي: mexora_etl/transform/clean_produits.py

import pandas as pd
import logging

def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Règles de nettoyage des produits Mexora.
    """
    initial = len(df)

    # R1 - Harmonisation de la casse des catégories (ex: "electronique" -> "Electronique")
    if 'categorie' in df.columns:
        df['categorie'] = df['categorie'].astype(str).str.strip().str.title()
        logging.info("[TRANSFORM] R1 Produits : Catégories standardisées")

    # R2 - Traitement des prix catalogue nuls (remplacés par 0.0)
    if 'prix_catalogue' in df.columns:
        nb_nuls = df['prix_catalogue'].isna().sum()
        df['prix_catalogue'] = df['prix_catalogue'].fillna(0.0)
        logging.info(f"[TRANSFORM] R2 Produits : {nb_nuls} prix nuls remplacés par 0.0")

    logging.info(f"[TRANSFORM] Produits nettoyés : {initial} lignes")
    return df