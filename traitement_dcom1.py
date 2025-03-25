import pandas as pd

file_path = "dcom1.csv"

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
        record["titre"] = line.replace("TITRE;:", "").strip()
    elif line.startswith("CONTENU;:"):
        current_field = "contenu"
        record["contenu"] = line.replace("CONTENU;:", "").strip()
    elif line.startswith("DATE;:"):
        current_field = "date"
        record["date"] = line.replace("DATE;:", "").strip()
    elif current_field:
        # Ajout de ligne complémentaire à un champ existant (souvent pour "date")
        record[current_field] += " " + line
    elif "cote" not in record:
        record["ref"] = line
    else:
        record["cote"] = line

# Ajouter le dernier enregistrement
if record:
    records.append(record)

# Création du DataFrame
df_dcom = pd.DataFrame(records)
df_dcom.to_csv("dcom1_nettoye.csv", index=False, encoding="utf-8")

print("Fichier nettoyé sauvegardé sous : dcom1_nettoye.csv")
