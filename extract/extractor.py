import pandas as pd
import json
import logging

def extract_commandes(filepath: str) -> pd.DataFrame:
    """
    Extrait les commandes depuis le fichier CSV source.
    Retourne le DataFrame brut sans modification.
    """
    df = pd.read_csv(filepath, encoding='utf-8', dtype=str)  
    logging.info(f"[EXTRACT] Commandes : {len(df)} lignes extraites depuis {filepath}")
    return df

def extract_produits(filepath: str) -> pd.DataFrame:
    """Extrait les produits depuis le fichier JSON."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df = pd.DataFrame(data['produits'])
    logging.info(f"[EXTRACT] Produits : {len(df)} lignes extraites")
    return df

def extract_clients(filepath: str) -> pd.DataFrame:
    """Extrait les clients depuis le fichier CSV source."""
    df = pd.read_csv(filepath, encoding='utf-8', dtype=str)
    logging.info(f"[EXTRACT] Clients : {len(df)} lignes extraites depuis {filepath}")
    return df

def extract_regions(filepath: str) -> pd.DataFrame:
    """Extrait le référentiel des régions depuis le fichier CSV source."""
    df = pd.read_csv(filepath, encoding='utf-8', dtype=str)
    logging.info(f"[EXTRACT] Régions : {len(df)} lignes extraites depuis {filepath}")
    return df