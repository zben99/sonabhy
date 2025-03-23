import csv

# Lire le fichier brut et nettoyer les lignes
with open("codoc2.csv", "r", encoding="utf-8") as f:
    lines = [line.replace("\f", "").strip().strip('"') for line in f if line.strip() and "~" not in line]

# Initialisation des variables
records = []
current_record = []

# Regrouper les données en enregistrements complets
for line in lines:
    if line.isdigit():  # Nouvelle entrée détectée (ID)
        if current_record:
            records.append(current_record)  # Ajouter l'ancien enregistrement
        current_record = [line]  # Commencer un nouvel enregistrement
    else:
        current_record.append(line)

if current_record:
    records.append(current_record)  # Ajouter le dernier enregistrement

# Vérifier et traiter les enregistrements
structured_data = []
for record in records:
    if len(record) < 2:  # Vérification pour éviter les erreurs d'index
        print(f"⚠️ Enregistrement ignoré : {record}")
        continue

    id_record = record[0]  # ID
    cote_rangement = record[1]  # Cote de rangement

    # Initialisation des champs
    titre = []
    contenu = []
    analyse = ""
    annee = ""
    solution_ged = ""

    section = "titre"

    for line in record[2:]:
        line = " ".join(line.split())  # Supprime les espaces excessifs

        if line.startswith("TITRE/DOSSIER"):
            section = "titre"
            titre.append(line.split(": ", 1)[-1])
        elif line.startswith("CONTENU"):
            section = "contenu"
            contenu.append(line.split(": ", 1)[-1])
        elif "ANA. DIPLOM" in line:
            section = "analyse"
            analyse = line.split(": ", 1)[-1]
        elif "DATE EXTREME" in line or "ANNEE" in line:
            section = "annee"
            annee = line.split(": ", 1)[-1].replace(",", "")
        elif "PASSAGE GED" in line:
            section = "ged"
            solution_ged = line.split(": ", 1)[-1]
        else:
            if section == "titre":
                titre.append(line)
            elif section == "contenu":
                contenu.append(line)

    structured_data.append([
        id_record,
        cote_rangement,
        " ".join(titre),
        " ".join(contenu),
        analyse,
        solution_ged,
        annee
    ])

# Écrire les données nettoyées dans un fichier CSV
with open("codoc2_epuree.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "ID",
        "Cote de rangement",
        "Titre du dossier",
        "Contenu du dossier",
        "Analyse diplomatique",
        "Solution GED",
        "Annee"
    ])
    writer.writerows(structured_data)

print("✅ Fichier nettoyé et sauvegardé sous 'base_donnee_epuree.csv'.")
