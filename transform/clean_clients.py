import pandas as pd
import logging
from datetime import date
import re

def transform_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Règles de nettoyage des clients : Déduplication, sexe, dates, email.
    """
    initial = len(df)
    
    # R1 — Déduplication
    if 'email' in df.columns:
        df['email_norm'] = df['email'].astype(str).str.lower().str.strip()
        df = df.sort_values('date_inscription').drop_duplicates(subset=['email_norm'], keep='last')

    # R2 — Standardisation du sexe
    mapping_sexe = {
        'm': 'm', 'f': 'f', '1': 'm', '0': 'f',
        'homme': 'm', 'femme': 'f', 'male': 'm', 'female': 'f', 'h': 'm'
    }
    if 'sexe' in df.columns:
        df['sexe'] = df['sexe'].astype(str).str.lower().str.strip().map(mapping_sexe).fillna('inconnu')

    # R3 — Validation des dates de naissance
    if 'date_naissance' in df.columns:
        df['date_naissance'] = pd.to_datetime(df['date_naissance'], errors='coerce')
        today = pd.Timestamp(date.today())
        df['age'] = (today - df['date_naissance']).dt.days // 365
        df.loc[(df['age'] < 16) | (df['age'] > 100), 'date_naissance'] = pd.NaT
        df['tranche_age'] = pd.cut(
            df['age'].fillna(0),
            bins=[0, 18, 25, 35, 45, 55, 65, 200],
            labels=['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
        )

    # R4 — Validation email
    pattern_email = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if 'email' in df.columns:
        df.loc[~df['email'].astype(str).str.match(pattern_email, na=False), 'email'] = None

    logging.info(f"[TRANSFORM] Clients nettoyés : {len(df)} lignes restantes sur {initial}")
    return df