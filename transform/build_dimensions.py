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
        masque = (dates >= pd.Timestamp(debut)) & (dates <= pd.Timestamp(fin))
        df.loc[masque, 'periode_ramadan'] = True

    logging.info(f"[BUILD] DIM_TEMPS : {len(df)} jours générés")
    return df[['id_date', 'jour', 'mois', 'trimestre', 'annee', 'semaine', 'libelle_jour', 'libelle_mois', 'est_weekend', 'est_ferie_maroc', 'periode_ramadan']]

# ==========================================
# 2. Dimension Client
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
    
    dim_client['id_client_nk'] = dim_client['id_client']
    dim_client['nom_complet'] = dim_client['nom'].fillna('') + ' ' + dim_client['prenom'].fillna('')
    dim_client['sexe'] = dim_client['sexe'].str[:1]
    dim_client['region_admin'] = None
    dim_client['segment_client'] = dim_client['segment_client'].fillna('Bronze')
    
    dim_client['id_client_sk'] = range(1, len(dim_client) + 1)
    dim_client['date_debut'] = date.today().strftime('%Y-%m-%d')
    dim_client['date_fin'] = '9999-12-31'
    dim_client['est_actif'] = True
    
    cols = ['id_client_sk', 'id_client_nk', 'nom_complet', 'tranche_age', 'sexe', 'ville', 
            'region_admin', 'segment_client', 'canal_acquisition', 'date_debut', 'date_fin', 'est_actif']
    
    logging.info(f"[BUILD] DIM_CLIENT : {len(dim_client)} clients")
    return dim_client[cols]

# ==========================================
# 3. Dimension Produit
# ==========================================
def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    dim_produit = df_produits.copy()
    dim_produit.rename(columns={'id_produit': 'id_produit_nk', 'nom': 'nom_produit', 'prix_catalogue': 'prix_standard'}, inplace=True)
    dim_produit['id_produit_sk'] = range(1, len(dim_produit) + 1)
    
    dim_produit['date_debut'] = pd.to_datetime(dim_produit.get('date_creation', '2024-01-01'), errors='coerce').fillna(pd.Timestamp('2024-01-01')).dt.strftime('%Y-%m-%d')
    dim_produit['date_fin'] = '9999-12-31'
    dim_produit['est_actif'] = dim_produit.get('actif', True).fillna(True).astype(bool)
    
    cols = ['id_produit_sk', 'id_produit_nk', 'nom_produit', 'categorie', 'sous_categorie', 
            'marque', 'fournisseur', 'prix_standard', 'origine_pays', 'date_debut', 'date_fin', 'est_actif']
            
    logging.info(f"[BUILD] DIM_PRODUIT : {len(dim_produit)} produits")
    return dim_produit[cols]

# ==========================================
# 4. Dimension Région
# ==========================================
def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    dim_region = df_regions.copy()
    dim_region.rename(columns={'nom_ville_standard': 'ville'}, inplace=True)
    dim_region['id_region'] = range(1, len(dim_region) + 1)
    dim_region['pays'] = 'Maroc'
    
    cols = ['id_region', 'ville', 'province', 'region_admin', 'zone_geo', 'pays']
    logging.info(f"[BUILD] DIM_REGION : {len(dim_region)} régions")
    return dim_region[cols]

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
    
    cols = ['id_livreur', 'id_livreur_nk', 'nom_livreur', 'type_transport', 'zone_couverture']
    logging.info(f"[BUILD] DIM_LIVREUR : {len(dim_livreur)} livreurs")
    return dim_livreur[cols]

# ==========================================
# 6. Table de Faits (FAIT_VENTES)
# ==========================================
def build_fait_ventes(df_commandes: pd.DataFrame, dim_temps: pd.DataFrame, dim_client: pd.DataFrame, dim_produit: pd.DataFrame, dim_region: pd.DataFrame, dim_livreur: pd.DataFrame) -> pd.DataFrame:
    faits = df_commandes.copy()
    
    faits['date_commande'] = pd.to_datetime(faits['date_commande'], errors='coerce')
    faits['id_date'] = faits['date_commande'].dt.strftime('%Y%m%d').astype(float).fillna(0).astype(int)
    
    faits['quantite_vendue'] = faits['quantite'].astype(float).astype(int)
    faits['montant_ttc'] = faits['quantite_vendue'] * faits['prix_unitaire'].astype(float)
    faits['montant_ht'] = faits['montant_ttc'] / 1.2 
    faits['cout_livraison'] = 20.00
    faits['remise_pct'] = 0.00
    
    faits['date_livraison'] = pd.to_datetime(faits['date_livraison'], errors='coerce')
    faits['delai_livraison_jours'] = (faits['date_livraison'] - faits['date_commande']).dt.days.fillna(-1).astype(int)
    
    # Jointures 
    faits = faits.merge(dim_produit[['id_produit_nk', 'id_produit_sk']], left_on='id_produit', right_on='id_produit_nk', how='inner')
    faits = faits.merge(dim_client[['id_client_nk', 'id_client_sk']], left_on='id_client', right_on='id_client_nk', how='inner')
    faits = faits.merge(dim_region[['ville', 'id_region']], left_on='ville_livraison', right_on='ville', how='inner')
    
    faits.rename(columns={'id_livreur': 'id_livreur_source'}, inplace=True)
    faits = faits.merge(dim_livreur[['id_livreur_nk', 'id_livreur']], left_on='id_livreur_source', right_on='id_livreur_nk', how='left')
    
    cols_to_keep = [
        'id_date', 
        'id_produit_sk', 
        'id_client_sk', 
        'id_region', 
        'id_livreur', 
        'quantite_vendue', 
        'montant_ht', 
        'montant_ttc', 
        'cout_livraison', 
        'delai_livraison_jours', 
        'remise_pct', 
        'statut'
    ]
    
    faits_final = faits[cols_to_keep].copy()
    
    faits_final.rename(columns={
        'id_produit_sk': 'id_produit', 
        'id_client_sk': 'id_client', 
        'statut': 'statut_commande'
    }, inplace=True)
    
    logging.info(f"[BUILD] FAIT_VENTES : {len(faits_final)} lignes prêtes pour chargement")
    return faits_final