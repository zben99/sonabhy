import pandas as pd
import re

file_path = "ditec2.csv"  # Chemin du fichier source

with open(file_path, "r", encoding="utf-8") as f:
    lines = [line.strip() for line in f if line.strip()]

records = []
record = {}
current_field = None
inside_record = False

def is_ref(line):
    return bool(re.fullmatch(r"\d{4}", line))

def is_cote(line):
    return bool(re.match(r"^\d{4}/", line))

for line in lines:
    if is_ref(line) and not inside_record:
        record = {"ref": line}
        inside_record = True
        current_field = None
        continue

    if is_cote(line):
        record["cote"] = line
        continue

    if line.startswith("TITRE;:"):
        current_field = "titre"
        record[current_field] = line.replace("TITRE;:", "").strip()
        continue

    if line.startswith("CONTENU;:"):
        current_field = "contenu"
        record[current_field] = line.replace("CONTENU;:", "").strip()
        continue

    if line.startswith("ANAL. DIPLOM"):
        current_field = "analyse_diplomatique"
        record[current_field] = line.split(":", 1)[-1].strip()
        continue

    if line.startswith("DATE;:"):
        current_field = "date"
        record[current_field] = line.replace("DATE;:", "").strip()
        continue

    # Si ligne numérique (ex: "2000") suit un champ connu, on l'ajoute
    if current_field and inside_record:
        record[current_field] += " " + line.strip()

    # Si une nouvelle référence arrive, on clôt le bloc précédent
    if is_ref(line) and inside_record:
        records.append(record)
        record = {"ref": line}
        current_field = None

# Ajouter le dernier enregistrement
if record and "ref" in record:
    records.append(record)

# Création du DataFrame
df_cleaned = pd.DataFrame(records)

# Export CSV
df_cleaned.to_csv("ditec2_nettoye_final.csv", index=False, encoding="utf-8")
print("✅ Fichier final exporté : ditec2_nettoye_final.csv")
