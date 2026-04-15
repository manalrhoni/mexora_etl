import logging
from datetime import datetime
import sqlalchemy
import os

if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s',
    handlers=[
        logging.FileHandler(f'logs/etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

from extract.extractor import extract_commandes, extract_produits, extract_clients, extract_regions
from transform.clean_commandes import transform_commandes
from transform.clean_clients import transform_clients
from transform.clean_produits import transform_produits
from transform.build_dimensions import build_dim_temps, build_dim_client, build_dim_produit, build_dim_region, build_dim_livreur, build_fait_ventes
from load.loader import charger_dimension, charger_faits

def run_pipeline():
    start = datetime.now()
    logging.info("=" * 60)
    logging.info("DÉMARRAGE PIPELINE ETL MEXORA")
    logging.info("=" * 60)

    try:
        # 1. EXTRACT
        logging.info("--- PHASE EXTRACT ---")
        df_commandes_raw = extract_commandes('data/commandes_mexora.csv')
        df_produits_raw  = extract_produits('data/produits_mexora.json')
        df_clients_raw   = extract_clients('data/clients_mexora.csv')
        df_regions       = extract_regions('data/regions_maroc.csv')

        # 2. TRANSFORM
        logging.info("--- PHASE TRANSFORM ---")
        df_commandes = transform_commandes(df_commandes_raw)
        df_clients   = transform_clients(df_clients_raw)
        df_produits  = transform_produits(df_produits_raw)

        dim_temps    = build_dim_temps('2020-01-01', '2025-12-31')
        dim_client   = build_dim_client(df_clients, df_commandes)
        dim_produit  = build_dim_produit(df_produits)
        dim_region   = build_dim_region(df_regions)
        dim_livreur  = build_dim_livreur(df_commandes)
        fait_ventes  = build_fait_ventes(df_commandes, dim_temps, dim_client, dim_produit, dim_region, dim_livreur)

        # 3. LOAD 
        logging.info("--- PHASE LOAD ---")
        
        engine = sqlalchemy.create_engine("postgresql://postgres:manoula@localhost:5432/mexora_dwh")
        charger_dimension(dim_temps,   'dim_temps',   engine)
        charger_dimension(dim_client,  'dim_client',  engine)
        charger_dimension(dim_produit, 'dim_produit', engine)
        charger_dimension(dim_region,  'dim_region',  engine)
        charger_dimension(dim_livreur, 'dim_livreur', engine)
        charger_faits(fait_ventes, engine)
        
        logging.info("Phase Load en attente de l'Étape 3 (Création DWH).")

        duree = (datetime.now() - start).seconds
        logging.info(f"PIPELINE TERMINÉ EN {duree} secondes")

    except Exception as e:
        logging.error(f"ERREUR PIPELINE : {e}", exc_info=True)
        raise

if __name__ == '__main__':
    run_pipeline()