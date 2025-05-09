import pandas as pd
import re

# Lecture manuelle (remplace par open() si tu le fais localement)
with open("digen1.csv", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Séparer les blocs par les lignes de tirets
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

# Extraire les données de chaque bloc
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
        elif line.startswith("DATE;:"):
            current_field = "date"
            record[current_field] = line.replace("DATE;:", "").strip()
        elif line.startswith("OBSERVATIONS"):
            current_field = "observations"
            record[current_field] = line.split(":", 1)[-1].strip()
        elif current_field:
            record[current_field] += " " + line.strip()

    if record:
        records.append(record)

# Créer le DataFrame et exporter
df_structured = pd.DataFrame(records)
df_structured.to_csv("digen1_csv_structure.csv", index=False, encoding="utf-8")

print("✅ Fichier bien structuré sauvegardé sous : digen1_csv_structure.csv")
