import pandas as pd
import re

# Lire le fichier (remplacer le chemin si besoin)
with open("digen2.csv", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Séparer les blocs à chaque ligne de tirets
entries = []
entry = []
for line in lines:
    if re.match(r"-{5,}", line):
        if entry:
            entries.append(entry)
            entry = []
    else:
        entry.append(line.strip())

if entry:
    entries.append(entry)

# Extraction des données
records = []
for block in entries:
    record = {}
    current_field = None

    for line in block:
        if re.match(r"^\d{4}$", line):
            record["ref"] = line
        elif re.match(r"^\d{4}/", line):
            record["cote"] = line
        elif line.startswith("TITRE;:"):
            current_field = "titre"
            record[current_field] = line.replace("TITRE;:", "").strip()
        elif line.startswith("CONTENU;:"):
            current_field = "contenu"
            record[current_field] = line.replace("CONTENU;:", "").strip()
        elif line.startswith("ANA. DIPLOM"):
            current_field = "analyse_diplomatique"
            record[current_field] = line.split(":", 1)[-1].strip()
        elif line.startswith("DATE;:"):
            current_field = "date"
            record[current_field] = line.replace("DATE;:", "").strip()
        elif current_field:
            record[current_field] += " " + line.strip()

    if record:
        records.append(record)

# Créer le DataFrame et exporter en CSV
df_digen2 = pd.DataFrame(records)
df_digen2.to_csv("digen2_nettoye.csv", index=False, encoding="utf-8")

print("✅ Fichier structuré sauvegardé sous : digen2_nettoye.csv")
