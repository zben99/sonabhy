import pandas as pd

# === Étape 1 : Lecture du fichier brut ===
file_path = "dave.csv"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

# === Étape 2 : Extraction structurée des données ===
records = []
record = {}
current_key = None

field_map = {
    "COTE;:": "cote",
    "AUTEUR PM  :": "auteur",
    "TITRE;:": "titre",
    "ADRESSE ED :": "adresse",
    "DES. TECH. :": "description_tech",
    "DESCRIP MAT:": "descrip_matiere",
}

for line in lines:
    line = line.strip()
    if not line:
        continue

    if line.startswith("~"):  # séparateur de bloc
        if record:
            records.append(record)
            record = {}
        current_key = None
        continue

    if line in field_map:
        current_key = field_map[line]
        record[current_key] = ""
    elif current_key:
        if record[current_key]:
            record[current_key] += " " + line
        else:
            record[current_key] = line

# Dernier enregistrement
if record:
    records.append(record)

# === Étape 3 : Sauvegarde en CSV ===
df = pd.DataFrame(records)
df.to_csv("dave_nettoye.csv", index=False, encoding="utf-8")

print("Fichier nettoyé sauvegardé sous : dave_nettoye.csv")
