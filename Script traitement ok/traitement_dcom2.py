import pandas as pd

file_path = "dcom2.csv"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

records = []
record = {}
current_field = None

for line in lines:
    line = line.strip()
    if not line or line.startswith("-"):
        if record:
            records.append(record)
            record = {}
        current_field = None
        continue

    if line.startswith("TITRE;:"):
        current_field = "titre"
        record[current_field] = line.replace("TITRE;:", "").strip()
    elif line.startswith("CONTENU;:"):
        current_field = "contenu"
        record[current_field] = line.replace("CONTENU;:", "").strip()
    elif line.startswith("ANA. DIPLOM. :"):
        current_field = "analyse_diplomatique"
        record[current_field] = line.replace("ANA. DIPLOM. :", "").strip()
    elif line.startswith("DATE;:"):
        current_field = "date"
        record[current_field] = line.replace("DATE;:", "").strip()
    elif current_field:
        record[current_field] += " " + line
    elif "cote" not in record:
        record["ref"] = line
    else:
        record["cote"] = line

# Dernier enregistrement
if record:
    records.append(record)

# DataFrame et export
df_dcom2 = pd.DataFrame(records)
df_dcom2.to_csv("dcom2_nettoye.csv", index=False, encoding="utf-8")

print("Fichier nettoyé sauvegardé sous : dcom2_nettoye.csv")
