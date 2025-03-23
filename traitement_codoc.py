import csv

# Lire le fichier brut
with open("codoc.csv", "r", encoding="utf-8") as f:
    lines = [line.replace("\f", "").strip().strip('"') for line in f if line.strip() and "----" not in line]

# Traiter les données
records = []
current_record = []

for line in lines:
    if line.isdigit():  # Nouvelle entrée détectée
        if current_record:
            records.append(current_record)  # Ajouter l'ancien enregistrement
        current_record = [line]  # Démarrer un nouvel enregistrement
    else:
        current_record.append(line)

if current_record:
    records.append(current_record)  # Ajouter le dernier enregistrement

# Structurer les données en colonnes
structured_data = []
for record in records:
    id = record[0]
    cote = record[1]
    titre = []
    contenu = []
    analyse = ""
    annee = ""

    section = "titre"

    for line in record[2:]:
        if line.startswith("TITRE"):
            section = "titre"
            titre.append(line.split(": ", 1)[-1])
        elif line.startswith("CONTENU"):
            section = "contenu"
            contenu.append(line.split(": ", 1)[-1])
        elif "ANA. DIPLOM" in line:
            section = "analyse"
            analyse = line.split(": ", 1)[-1]
        elif "DATE" in line:
            section = "annee"
            annee = line.split(": ", 1)[-1]
        else:
            if section == "titre":
                titre.append(line)
            elif section == "contenu":
                contenu.append(line)

    structured_data.append([id, cote, " ".join(titre),  " ".join(contenu), analyse, annee])

# Écrire le fichier CSV nettoyé
with open("codoc_epuree.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "cote",  "Titre","Contenu", "Analyse diplomatique", "Annee"])
    writer.writerows(structured_data)

print("Fichier nettoyé et sauvegardé sous 'base_donnee_epuree.csv'.")
