# الفيشي: mexora_etl/transform/build_dimensions.py

import pandas as pd
import logging
from datetime import date, timedelta

# ==========================================
# 1. Dimension Temps
# ==========================================
def build_dim_temps(date_debut: str, date_fin: str) -> pd.DataFrame:
    dates = pd.date_range(start=date_debut, end=date_fin, freq='D')
    feries_maroc = ['2024-01-01', '2024-01-11', '2024-05-01', '2024-07-30', '2024-08-14', '2024-11-06', '2024-11-18']
    ramadan_periodes = [('2022-04-02', '2022-05-01'), ('2023-03-22', '2023-04-20'), ('2024-03-10', '2024-04-09')]

    df = pd.DataFrame({
        'id_date':       dates.strftime('%Y%m%d').astype(int),
        'jour':          dates.day,
        'mois':          dates.month,
        'trimestre':     dates.quarter,
        'annee':         dates.year,
        'semaine':       dates.isocalendar().week.astype(int),
        'libelle_jour':  dates.strftime('%A'),
        'libelle_mois':  dates.strftime('%B'),
        'est_weekend':   dates.dayofweek >= 5,
        'est_ferie_maroc': dates.strftime('%Y-%m-%d').isin(feries_maroc),
    })

    df['periode_ramadan'] = False
    for debut, fin in ramadan_periodes:
        masque = (dates >= debut) & (dates <= fin)
        df.loc[masque, 'periode_ramadan'] = True

    logging.info(f"[BUILD] DIM_TEMPS : {len(df)} jours générés")
    return df

# ==========================================
# 2. Dimension Client (avec Segmentation)
# ==========================================
def calculer_segments_clients(df_commandes: pd.DataFrame) -> pd.DataFrame:
    date_limite = pd.Timestamp(date.today() - timedelta(days=365))
    df_recents = df_commandes[(pd.to_datetime(df_commandes['date_commande'], errors='coerce') >= date_limite) & (df_commandes['statut'] == 'livré')].copy()
    df_recents['montant_ttc'] = df_recents['quantite'].astype(float) * df_recents['prix_unitaire'].astype(float)
    ca_par_client = df_recents.groupby('id_client')['montant_ttc'].sum().reset_index()
    ca_par_client.columns = ['id_client', 'ca_12m']

    def segmenter(ca):
        if ca >= 15000: return 'Gold'
        elif ca >= 5000: return 'Silver'
        else: return 'Bronze'

    ca_par_client['segment_client'] = ca_par_client['ca_12m'].apply(segmenter)
    return ca_par_client[['id_client', 'segment_client']]

def build_dim_client(df_clients: pd.DataFrame, df_commandes: pd.DataFrame) -> pd.DataFrame:
    df_seg = calculer_segments_clients(df_commandes)
    dim_client = pd.merge(df_clients, df_seg, left_on='id_client', right_on='id_client', how='left')
    dim_client['segment_client'] = dim_client['segment_client'].fillna('Bronze')
    
    dim_client.rename(columns={'id_client': 'id_client_nk', 'nom': 'nom_complet'}, inplace=True)
    dim_client['id_client_sk'] = range(1, len(dim_client) + 1) # Surrogate Key
    
    # SCD Type 2 colonnes
    dim_client['date_debut'] = date.today().strftime('%Y-%m-%d')
    dim_client['date_fin'] = '9999-12-31'
    dim_client['est_actif'] = True
    
    logging.info(f"[BUILD] DIM_CLIENT : {len(dim_client)} clients")
    return dim_client

# ==========================================
# 3. Dimension Produit
# ==========================================
def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    dim_produit = df_produits.copy()
    dim_produit.rename(columns={'id_produit': 'id_produit_nk', 'nom': 'nom_produit', 'prix_catalogue': 'prix_standard'}, inplace=True)
    dim_produit['id_produit_sk'] = range(1, len(dim_produit) + 1) # Surrogate Key
    
    # SCD Type 2 colonnes
    dim_produit['date_debut'] = pd.to_datetime(dim_produit.get('date_creation', '2024-01-01'), errors='coerce').fillna(pd.Timestamp('2024-01-01')).dt.strftime('%Y-%m-%d')
    dim_produit['date_fin'] = '9999-12-31'
    dim_produit['est_actif'] = dim_produit.get('actif', True).fillna(True).astype(bool)
    
    logging.info(f"[BUILD] DIM_PRODUIT : {len(dim_produit)} produits")
    return dim_produit

# ==========================================
# 4. Dimension Région
# ==========================================
def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    dim_region = df_regions.copy()
    dim_region.rename(columns={'nom_ville_standard': 'ville'}, inplace=True)
    dim_region['id_region'] = range(1, len(dim_region) + 1)
    dim_region['pays'] = 'Maroc'
    logging.info(f"[BUILD] DIM_REGION : {len(dim_region)} régions")
    return dim_region

# ==========================================
# 5. Dimension Livreur
# ==========================================
def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    livreurs = df_commandes['id_livreur'].dropna().unique()
    dim_livreur = pd.DataFrame({'id_livreur_nk': livreurs})
    dim_livreur['id_livreur'] = range(1, len(dim_livreur) + 1)
    dim_livreur['nom_livreur'] = 'Livreur ' + dim_livreur['id_livreur_nk'].astype(str)
    dim_livreur['type_transport'] = 'Inconnu'
    dim_livreur['zone_couverture'] = 'Maroc'
    logging.info(f"[BUILD] DIM_LIVREUR : {len(dim_livreur)} livreurs")
    return dim_livreur

# ==========================================
# 6. Table de Faits (FAIT_VENTES)
# ==========================================
def build_fait_ventes(df_commandes: pd.DataFrame, dim_temps: pd.DataFrame, dim_client: pd.DataFrame, dim_produit: pd.DataFrame, dim_region: pd.DataFrame, dim_livreur: pd.DataFrame) -> pd.DataFrame:
    faits = df_commandes.copy()
    
    # Clé étrangère Temps
    faits['date_commande'] = pd.to_datetime(faits['date_commande'], errors='coerce')
    faits['id_date'] = faits['date_commande'].dt.strftime('%Y%m%d').astype(float).fillna(0).astype(int)
    
    # Calcul des Mesures
    faits['quantite_vendue'] = faits['quantite'].astype(float).astype(int)
    faits['montant_ttc'] = faits['quantite_vendue'] * faits['prix_unitaire'].astype(float)
    faits['montant_ht'] = faits['montant_ttc'] / 1.2  # Déduction TVA 20%
    faits['cout_livraison'] = 20.00
    faits['remise_pct'] = 0.00
    
    # Délai de livraison
    faits['date_livraison'] = pd.to_datetime(faits['date_livraison'], errors='coerce')
    faits['delai_livraison_jours'] = (faits['date_livraison'] - faits['date_commande']).dt.days.fillna(-1).astype(int)
    
    # Jointures pour récupérer les clés (Surrogate Keys)
    faits = faits.merge(dim_produit[['id_produit_nk', 'id_produit_sk']], left_on='id_produit', right_on='id_produit_nk', how='inner')
    faits = faits.merge(dim_client[['id_client_nk', 'id_client_sk']], left_on='id_client', right_on='id_client_nk', how='inner')
    faits = faits.merge(dim_region[['ville', 'id_region']], left_on='ville_livraison', right_on='ville', how='inner')
    
    # تفادي المشكل ديال تشابه السميات
    faits.rename(columns={'id_livreur': 'id_livreur_source'}, inplace=True)
    faits = faits.merge(dim_livreur[['id_livreur_nk', 'id_livreur']], left_on='id_livreur_source', right_on='id_livreur_nk', how='left')
    
    # Préparation finale
    faits.rename(columns={'id_produit_sk': 'id_produit', 'id_client_sk': 'id_client', 'statut': 'statut_commande'}, inplace=True)
    
    cols_finales = ['id_date', 'id_produit', 'id_client', 'id_region', 'id_livreur', 
                    'quantite_vendue', 'montant_ht', 'montant_ttc', 'cout_livraison', 
                    'delai_livraison_jours', 'remise_pct', 'statut_commande']
    
    faits_final = faits[cols_finales].copy()
    logging.info(f"[BUILD] FAIT_VENTES : {len(faits_final)} lignes prêtes pour chargement")
    return faits_final