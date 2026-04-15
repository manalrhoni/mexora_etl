# Rapport des Transformations ETL - Mexora Analytics

Ce document détaille les règles métier appliquées lors de la phase de transformation du pipeline ETL pour les données Mexora.

## 1. Nettoyage des Commandes (`clean_commandes.py`)

* **R1 - Suppression des doublons :**
  * **Règle :** Conserver la dernière occurrence pour chaque `id_commande`.
  * **Code :** `df = df.drop_duplicates(subset=['id_commande'], keep='last')`
  * **Lignes affectées :** 1500 lignes supprimées.

* **R2 - Standardisation des dates :**
  * **Règle :** Convertir au format YYYY-MM-DD et supprimer les dates invalides.
  * **Code :** `df['date_commande'] = pd.to_datetime(df['date_commande'], format='mixed', dayfirst=True, errors='coerce')`
  * **Lignes affectées :** 0 date invalide supprimée.

* **R3 - Harmonisation des villes :**
  * **Règle :** Mapper les noms incohérents via le référentiel des régions (ex: "tng" -> "Tanger").
  * **Code :** `df['ville_livraison'] = df['ville_livraison'].map(mapping_villes).fillna('Non renseignée')`

* **R4 - Standardisation des statuts :**
  * **Règle :** Harmoniser les statuts vers ('livré', 'annulé', 'en_cours', 'retourné').
  * **Code :** `df['statut'] = df['statut'].replace(mapping_statuts)`

* **R5 & R6 - Quantités et Prix nuls :**
  * **Règle :** Supprimer les lignes où la quantité est <= 0 ou le prix_unitaire = 0 (commandes test).
  * **Code :** `df = df[df['quantite'].astype(float) > 0]`
  * **Lignes affectées :** 484 lignes supprimées (quantité <= 0) et 927 commandes test supprimées (prix nul).

* **R7 - Livreurs manquants :**
  * **Règle :** Remplacer les valeurs nulles par '-1' (livreur inconnu).
  * **Code :** `df['id_livreur'] = df['id_livreur'].fillna('-1')`
  * **Lignes affectées :** 3388 valeurs manquantes remplacées par -1.

**Bilan Commandes :** 49 999 lignes initiales -> 47 088 lignes propres prêtes pour le DWH.

## 2. Nettoyage des Clients (`clean_clients.py`)

* **R1 - Déduplication :**
  * **Règle :** Supprimer les doublons basés sur l'email normalisé (conserver l'inscription la plus récente).
  * **Code :** `df = df.sort_values('date_inscription').drop_duplicates(subset=['email_norm'], keep='last')`
  * **Lignes affectées :** 20 doublons supprimés (1000 lignes restantes sur 1020).

* **R2 - Standardisation du Sexe :**
  * **Règle :** Cible : 'm' / 'f' / 'inconnu' via un dictionnaire de mapping.
  * **Code :** `df['sexe'] = df['sexe'].map(mapping_sexe).fillna('inconnu')`

* **R3 - Validation des dates de naissance :**
  * **Règle :** Filtrer les âges aberrants (<16 ans ou >100 ans).
  * **Code :** `df.loc[(df['age'] < 16) | (df['age'] > 100), 'date_naissance'] = pd.NaT`

## 3. Nettoyage des Produits (`clean_produits.py`)

* **R1 - Harmonisation de la casse :**
  * **Règle :** Standardiser les noms de catégories avec `.title()`.
  * **Code :** `df['categorie'] = df['categorie'].astype(str).str.strip().str.title()`

* **R2 - Prix catalogue nuls :**
  * **Règle :** Remplacer les prix manquants par 0.0.
  * **Code :** `df['prix_catalogue'] = df['prix_catalogue'].fillna(0.0)`
  * **Lignes affectées :** 5 prix nuls remplacés par 0.0.