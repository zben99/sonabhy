import pandas as pd
import re

# Lire le fichier texte original
with open("dadfi1.csv", "r", encoding="utf-8") as file:
    lignes = file.readlines()

# Variables temporaires pour stocker les données d'un enregistrement
records = []
record = {}

for ligne in lignes:
    ligne = ligne.strip()
    if re.match(r'^\d{5}$', ligne):  # Numéro d'identification
        if record:  
            records.append(record)
            record = {}
        record['ID'] = ligne
    elif re.match(r'^\d+/.+$', ligne):  # Référence de dossier
        record['Référence'] = ligne
    elif ligne.startswith("TITRE/DOSSIER :"):
        record['Titre/Dossier'] = ligne.split(":", 1)[1].strip()
    elif ligne.startswith("N° PIECES;:"):
        pieces = ligne.split(":", 1)[1].strip()
        record['N° Pièces'] = pieces
    elif ligne.startswith("DATE;:"):
        date = ligne.split(":", 1)[1].strip()
        record['Date'] = date
    elif ligne.startswith("OBSERVATIONS"):
        record['Observations'] = ligne.split(":", 1)[1].strip()
    elif ligne.startswith("~"):  # ligne de séparation
        continue
    else:
        # Gestion observations multi-lignes
        if 'Observations' in record:
            record['Observations'] += ' ' + ligne

# Ajouter le dernier enregistrement s'il existe
if record:
    records.append(record)

# Conversion en DataFrame
df_propre = pd.DataFrame(records)

# Sauvegarde dans un fichier CSV nettoyé
df_propre.to_csv("dadfi1_propre.csv", index=False, encoding="utf-8")

print("Fichier nettoyé et sauvegardé : dadfi1_propre.csv")
