import pandas as pd

file_path = "dcom3.csv"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

records = []
record = {}
current_field = None

for line in lines:
    line = line.strip()
    if not line or line.startswith("~"):
        if record:
            records.append(record)
            record = {}
        current_field = None
        continue

    if line.startswith("TITRE/DOSSIER :"):
        current_field = "titre"
        record[current_field] = line.replace("TITRE/DOSSIER :", "").strip()
    elif line.startswith("CONTENU;:"):
        current_field = "contenu"
        record[current_field] = line.replace("CONTENU;:", "").strip()
    elif line.startswith("ANA. DIPLOM."):
        current_field = "analyse_diplomatique"
        record[current_field] = line.split(":", 1)[-1].strip()
    elif line.startswith("ANNEE;:"):
        current_field = "annee"
        record[current_field] = line.replace("ANNEE;:", "").strip()
    elif current_field:
        record[current_field] += " " + line
    elif "cote" not in record:
        record["ref"] = line
    else:
        record["cote"] = line

# Ajouter le dernier enregistrement
if record:
    records.append(record)

# DataFrame + export
df_dcom3 = pd.DataFrame(records)
df_dcom3.to_csv("dcom3_nettoye.csv", index=False, encoding="utf-8")

print("Fichier nettoyé sauvegardé sous : dcom3_nettoye.csv")
