import pandas as pd
import re

file_path = "ditec1.csv"  # à adapter selon ton contexte

with open(file_path, "r", encoding="utf-8") as f:
    lines = [line.strip() for line in f if line.strip()]

records = []
record = {}
current_field = None

for line in lines:
    if re.match(r"^\d{4}$", line):  # début d’un enregistrement
        if record:
            records.append(record)
            record = {}
        record["ref"] = line
        current_field = None
    elif re.match(r"^\d{4}/", line):  # cote
        record["cote"] = line
    elif line.startswith("TITRE;:"):
        current_field = "titre"
        record[current_field] = line.replace("TITRE;:", "").strip()
    elif line.startswith("CONTENU;:"):
        current_field = "contenu"
        record[current_field] = line.replace("CONTENU;:", "").strip()
    elif line.startswith("ANAL. DIPLOM"):
        current_field = "analyse_diplomatique"
        record[current_field] = line.split(":", 1)[-1].strip()
    elif line.startswith("DATE;:"):
        current_field = "date"
        record[current_field] = line.replace("DATE;:", "").strip()
    elif current_field:
        record[current_field] += " " + line.strip()

# Ajouter le dernier bloc
if record:
    records.append(record)

# Export CSV
df = pd.DataFrame(records)
df.to_csv("ditec1_nettoye.csv", index=False, encoding="utf-8")

print("✅ Fichier bien structuré avec 'contenu' inclus : ditec1_nettoye.csv")
