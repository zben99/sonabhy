import pandas as pd
import re

# Lire le fichier (à adapter si en local)
with open("digen3.csv", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Séparer les blocs par les lignes de tildes
entries = []
entry = []
for line in lines:
    if line.strip().startswith("~"):
        if entry:
            entries.append(entry)
            entry = []
    else:
        entry.append(line.strip())

if entry:
    entries.append(entry)

# Extraire les données de chaque bloc
records = []
for block in entries:
    record = {}
    current_field = None

    for line in block:
        if re.match(r"^\d{5}$", line):
            record["ref"] = line
        elif re.match(r"^\d{4}/", line):
            record["cote"] = line
        elif line.startswith("TITRE/DOSSIER"):
            current_field = "titre"
            record[current_field] = line.split(":", 1)[-1].strip()
        elif line.startswith("CONTENU;:"):
            current_field = "contenu"
            record[current_field] = line.replace("CONTENU;:", "").strip()
        elif line.startswith("ANA. DIPLOM."):
            current_field = "analyse_diplomatique"
            record[current_field] = line.split(":", 1)[-1].strip()
        elif re.match(r"^\d{4}$", line):  # Date isolée en fin
            record["date"] = line.strip()
        elif current_field:
            record[current_field] += " " + line.strip()

    if record:
        records.append(record)

# DataFrame + export
df_digen3 = pd.DataFrame(records)
df_digen3.to_csv("digen3_nettoye.csv", index=False, encoding="utf-8")

print("✅ Fichier structuré sauvegardé sous : digen3_nettoye.csv")
