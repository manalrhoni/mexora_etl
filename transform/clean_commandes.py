import pandas as pd
import logging

def charger_referentiel_villes(filepath: str) -> dict:
    """Création d'un dictionnaire de mapping basique pour les villes"""
    return {
        'tanger': 'Tanger', 'tng': 'Tanger', 'tanger ': 'Tanger', 'tnja': 'Tanger',
        'casa': 'Casablanca', 'casablanca': 'Casablanca', 
        'kech': 'Marrakech', 'marrakech': 'Marrakech'
    }

def transform_commandes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les commandes Mexora.
    """
    initial = len(df)

    # R1 — Suppression des doublons
    df = df.drop_duplicates(subset=['id_commande'], keep='last')
    logging.info(f"[TRANSFORM] R1 doublons : {initial - len(df)} lignes supprimées")

    # R2 — Standardisation des dates
    df['date_commande'] = pd.to_datetime(
        df['date_commande'], format='mixed', dayfirst=True, errors='coerce'
    )
    dates_invalides = df['date_commande'].isna().sum()
    df = df.dropna(subset=['date_commande'])
    logging.info(f"[TRANSFORM] R2 dates : {dates_invalides} dates invalides supprimées")

    # R3 — Harmonisation des villes
    mapping_villes = charger_referentiel_villes('data/regions_maroc.csv')
    df['ville_livraison'] = df['ville_livraison'].str.strip().str.lower()
    df['ville_livraison'] = df['ville_livraison'].map(mapping_villes).fillna('Non renseignée')

    # R4 — Standardisation des statuts
    mapping_statuts = {
        'livré': 'livré', 'livre': 'livré', 'LIVRE': 'livré', 'DONE': 'livré',
        'annulé': 'annulé', 'annule': 'annulé', 'KO': 'annulé',
        'en_cours': 'en_cours', 'OK': 'en_cours',
        'retourné': 'retourné', 'retourne': 'retourné'
    }
    df['statut'] = df['statut'].replace(mapping_statuts)
    invalides = ~df['statut'].isin(['livré', 'annulé', 'en_cours', 'retourné'])
    logging.warning(f"[TRANSFORM] R4 statuts : {invalides.sum()} valeurs non reconnues -> 'inconnu'")
    df.loc[invalides, 'statut'] = 'inconnu'

    # R5 — Quantités invalides
    avant = len(df)
    df = df[df['quantite'].astype(float) > 0]
    logging.info(f"[TRANSFORM] R5 quantités : {avant - len(df)} lignes supprimées (quantité <= 0)")

    # R6 — Prix nuls (commandes test)
    avant = len(df)
    df = df[df['prix_unitaire'].astype(float) > 0]
    logging.info(f"[TRANSFORM] R6 prix : {avant - len(df)} commandes test supprimées")

    # R7 — Livreurs manquants
    nb_manquants = df['id_livreur'].isna().sum()
    df['id_livreur'] = df['id_livreur'].fillna('-1')
    logging.info(f"[TRANSFORM] R7 livreurs : {nb_manquants} valeurs manquantes remplacées par -1")

    logging.info(f"[TRANSFORM] Commandes : {initial} -> {len(df)} lignes ({initial - len(df)} supprimées au total)")
    return df