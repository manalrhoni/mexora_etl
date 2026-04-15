-- 1. CRÉATION DES SCHÉMAS
CREATE SCHEMA IF NOT EXISTS dwh_mexora;
CREATE SCHEMA IF NOT EXISTS reporting_mexora;

-- 2. TABLES DE DIMENSIONS
CREATE TABLE dwh_mexora.dim_temps (
    id_date INTEGER PRIMARY KEY,
    jour SMALLINT, mois SMALLINT, trimestre SMALLINT, annee SMALLINT,
    semaine SMALLINT, libelle_jour VARCHAR(20), libelle_mois VARCHAR(20),
    est_weekend BOOLEAN, est_ferie_maroc BOOLEAN, periode_ramadan BOOLEAN
);

CREATE TABLE dwh_mexora.dim_produit (
    id_produit_sk SERIAL PRIMARY KEY,
    id_produit_nk VARCHAR(20), nom_produit VARCHAR(200),
    categorie VARCHAR(100), sous_categorie VARCHAR(100),
    marque VARCHAR(100), fournisseur VARCHAR(100),
    prix_standard DECIMAL(10,2), origine_pays VARCHAR(50),
    date_debut DATE, date_fin DATE, est_actif BOOLEAN
);

CREATE TABLE dwh_mexora.dim_client (
    id_client_sk SERIAL PRIMARY KEY,
    id_client_nk VARCHAR(20), nom_complet VARCHAR(200),
    tranche_age VARCHAR(10), sexe CHAR(1), ville VARCHAR(100),
    region_admin VARCHAR(100), segment_client VARCHAR(20),
    canal_acquisition VARCHAR(50), date_debut DATE, date_fin DATE, est_actif BOOLEAN
);

CREATE TABLE dwh_mexora.dim_region (
    id_region SERIAL PRIMARY KEY,
    ville VARCHAR(100), province VARCHAR(100),
    region_admin VARCHAR(100), zone_geo VARCHAR(50), pays VARCHAR(50)
);

CREATE TABLE dwh_mexora.dim_livreur (
    id_livreur SERIAL PRIMARY KEY,
    id_livreur_nk VARCHAR(20), nom_livreur VARCHAR(100),
    type_transport VARCHAR(50), zone_couverture VARCHAR(100)
);

-- 3. TABLE DE FAITS
CREATE TABLE dwh_mexora.fait_ventes (
    id_vente BIGSERIAL PRIMARY KEY,
    id_date INTEGER REFERENCES dwh_mexora.dim_temps(id_date),
    id_produit INTEGER REFERENCES dwh_mexora.dim_produit(id_produit_sk),
    id_client INTEGER REFERENCES dwh_mexora.dim_client(id_client_sk),
    id_region INTEGER REFERENCES dwh_mexora.dim_region(id_region),
    id_livreur INTEGER REFERENCES dwh_mexora.dim_livreur(id_livreur),
    quantite_vendue INTEGER, montant_ht DECIMAL(12,2), montant_ttc DECIMAL(12,2),
    cout_livraison DECIMAL(8,2), delai_livraison_jours SMALLINT,
    remise_pct DECIMAL(5,2), date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    statut_commande VARCHAR(20)
);

-- 4. VUES MATÉRIALISÉES
CREATE MATERIALIZED VIEW reporting_mexora.mv_ca_mensuel AS
SELECT t.annee, t.mois, t.libelle_mois, r.region_admin, p.categorie,
SUM(f.montant_ttc) AS ca_ttc, COUNT(DISTINCT f.id_vente) AS nb_commandes
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps t ON f.id_date = t.id_date
JOIN dwh_mexora.dim_region r ON f.id_region = r.id_region
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
GROUP BY 1,2,3,4,5 WITH DATA;