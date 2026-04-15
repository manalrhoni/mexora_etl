import os
import json
import random
import csv
from datetime import datetime, timedelta

# Créer le dossier data s'il n'existe pas
os.makedirs('data', exist_ok=True)

print("⏳ Création des 4 fichiers de données Mexora en cours...")

# ==========================================
# 1. Génération de regions_maroc.csv
# ==========================================
regions_data = [
    ["code_ville", "nom_ville_standard", "province", "region_admin", "zone_geo", "population", "code_postal"],
    ["TNG", "Tanger", "Tanger-Assilah", "Tanger-Tetouan-Al Hoceima", "Nord", "1000000", "90000"],
    ["CAS", "Casablanca", "Casablanca", "Casablanca-Settat", "Centre", "4000000", "20000"],
    ["RBA", "Rabat", "Rabat", "Rabat-Sale-Kenitra", "Centre", "1500000", "10000"],
    ["MAR", "Marrakech", "Marrakech", "Marrakech-Safi", "Sud", "1000000", "40000"],
    ["AGA", "Agadir", "Agadir-Ida Ou Tanane", "Souss-Massa", "Sud", "500000", "80000"]
]
with open('data/regions_maroc.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(regions_data)

# ==========================================
# 2. Génération de produits_mexora.json
# ==========================================
categories_casse = ["Electronique", "electronique", "ELECTRONIQUE", "Mode", "mode", "Alimentation", "ALIMENTATION"]
produits = []
for i in range(1, 101):
    cat = random.choice(categories_casse)
    prix = round(random.uniform(50, 15000), 2)
    # Problème : quelques prix à null
    if random.random() < 0.05: prix = None
    
    # Problème : certains inactifs
    actif = False if random.random() < 0.1 else True

    produits.append({
        "id_produit": f"P{i:03d}",
        "nom": f"Produit Test {i}",
        "categorie": cat,
        "sous_categorie": "Sous-cat",
        "marque": "MarqueX",
        "fournisseur": "FournisseurY",
        "prix_catalogue": prix,
        "origine_pays": "Maroc",
        "date_creation": "2023-01-01",
        "actif": actif
    })

with open('data/produits_mexora.json', 'w', encoding='utf-8') as f:
    json.dump({"produits": produits}, f, indent=2, ensure_ascii=False)

# ==========================================
# 3. Génération de clients_mexora.csv
# ==========================================
villes_messy = ["tanger", "TNG", "TANGER", "Tnja", "casa", "CASABLANCA", "kech", "marrakech", "RBA", "Agadir"]
sexes_messy = ["m", "f", "1", "0", "Homme", "Femme", "H", "F"]

clients = [["id_client", "nom", "prenom", "email", "date_naissance", "sexe", "ville", "telephone", "date_inscription", "canal_acquisition"]]
emails_used = []

for i in range(1, 1001):
    id_client = f"CL{i:04d}"
    email = f"client{i}@email.com"
    
    # Problème : emails mal formatés
    if random.random() < 0.05: email = email.replace("@", "")
    
    # Problème : dates de naissance aberrantes (<16 ans ou >120 ans)
    annee_naiss = random.randint(1950, 2005)
    if random.random() < 0.02: annee_naiss = 2025 # bébé / futur
    if random.random() < 0.02: annee_naiss = 1850 # trop vieux
    
    clients.append([
        id_client, f"Nom{i}", f"Prenom{i}", email, f"{annee_naiss}-05-15", 
        random.choice(sexes_messy), random.choice(villes_messy), "0600000000", "2023-05-10", "Web"
    ])
    emails_used.append(email)

# Problème : Doublons clients (même email, id différent)
for i in range(20):
    clients.append([
        f"CL999{i}", "DoublonNom", "DoublonPrenom", random.choice(emails_used), 
        "1990-01-01", "m", "tanger", "0600000000", "2024-01-01", "App"
    ])

with open('data/clients_mexora.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(clients)

# ==========================================
# 4. Génération de commandes_mexora.csv (50 000 lignes)
# ==========================================
commandes = [["id_commande", "id_client", "id_produit", "date_commande", "quantite", "prix_unitaire", "statut", "ville_livraison", "mode_paiement", "id_livreur", "date_livraison"]]

statuts_messy = ["livré", "livré", "livré", "livre", "LIVRE", "DONE", "annulé", "KO", "en_cours", "OK", "retourné"]
date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%b %d %Y']

base_date = datetime(2023, 1, 1)
commandes_data = []

for i in range(1, 48500): # 48500 originaux + 1500 doublons = 50000
    id_commande = f"CMD{i:05d}"
    client = f"CL{random.randint(1, 1000):04d}"
    produit = f"P{random.randint(1, 100):03d}"
    
    # Problème : dates en formats mixtes
    dt_cmd = base_date + timedelta(days=random.randint(0, 500))
    date_str = dt_cmd.strftime(random.choice(date_formats))
    
    # Problème : quantité négative
    qte = random.randint(1, 5)
    if random.random() < 0.01: qte = -1
    
    # Problème : prix à 0
    prix = round(random.uniform(50, 5000), 2)
    if random.random() < 0.02: prix = 0
    
    statut = random.choice(statuts_messy)
    ville = random.choice(villes_messy)
    
    # Problème : livreur manquant
    livreur = f"LIV{random.randint(1, 10)}"
    if random.random() < 0.07: livreur = ""
    
    dt_liv = dt_cmd + timedelta(days=random.randint(1, 5))
    date_liv_str = dt_liv.strftime('%Y-%m-%d') if statut in ["livré", "LIVRE", "DONE", "retourné"] else ""

    row = [id_commande, client, produit, date_str, qte, prix, statut, ville, "Carte", livreur, date_liv_str]
    commandes_data.append(row)

# Problème : Doublons sur id_commande (~3% = 1500 lignes)
doublons = random.choices(commandes_data, k=1500)
commandes_data.extend(doublons)

random.shuffle(commandes_data) # Mélanger le tout
commandes.extend(commandes_data)

with open('data/commandes_mexora.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(commandes)

print("✅ SUCCÈS : 50 000 commandes générées avec tous les pièges demandés par le prof !")