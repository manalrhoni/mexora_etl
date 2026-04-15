# Mexora Analytics - Pipeline ETL

Ce projet contient le pipeline ETL pour la construction du Data Warehouse de la marketplace Mexora, développé dans le cadre du Miniprojet 1 (Data Engineering).

## Structure du projet
- `data/` : Contient les fichiers sources (CSV, JSON).
- `extract/` : Scripts d'extraction des données brutes.
- `transform/` : Scripts de nettoyage et de création des dimensions et de la table de faits (Schéma en étoile).
- `load/` : Scripts d'insertion vers PostgreSQL.
- `main.py` : Orchestrateur principal du pipeline.
- `generate_data.py` : Script additionnel pour générer les 50 000 lignes de données de test avec les anomalies requises.

## Prérequis
- Python 3.11+
- Modules : `pandas`, `sqlalchemy`, `psycopg2-binary`

## Comment lancer le pipeline ?
1. Ouvrez un terminal dans le dossier `mexora_etl`.
2. (Optionnel) Si le dossier `data/` est vide, exécutez d'abord `python generate_data.py` pour simuler les 50 000 commandes imparfaites.
3. Installez les dépendances : `pip install pandas sqlalchemy psycopg2-binary`
4. Exécutez le pipeline : `python main.py`
5. Les logs détaillés de l'exécution (lignes nettoyées, supprimées, etc.) seront générés dans le dossier `logs/`.