import pandas as pd

file_path = "digen4.csv"  # adapte selon ton chemin

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

records = []
record = {}
current_field = None

# Dictionnaire de correspondance complet
field_map = {
    "10:": "date_versement",
    "20:": "date_enregistrement",
    "30:": "provenance_ou_processus",
    "40:": "cote_rangement",
    "50:": "titre",
    "60:": "mots_cles_matieres",  # peut être répété
    "70:": "analyse_diplomatique",
    "80:": "dates_extremes",
    "90:": "passage_ged",
    "100:": "duree_utilite_admin",
    "110:": "sort_final",
    "120:": "notes",
    "130:": "initial_processus",
    "140:": "niveau_description",
    "150:": "importance_materielle"
}

for line in lines:
    line = line.strip()

    if not line or line == "NFM:":
        if record:
            records.append(record)
            record = {}
        current_field = None
        continue

    parts = line.split(":", 1)
    if len(parts) == 2 and parts[0].isdigit():
        code = parts[0] + ":"
        content = parts[1].strip()
        current_field = field_map.get(code, None)

        if current_field:
            if current_field == "mots_cles_matieres":
                record.setdefault(current_field, []).append(content)
            else:
                record[current_field] = content
    elif current_field:
        if current_field == "mots_cles_matieres":
            record[current_field][-1] += " " + line
        else:
            record[current_field] += " " + line

# Ajouter le dernier bloc
if record:
    records.append(record)

# Finalisation
for rec in records:
    if isinstance(rec.get("mots_cles_matieres"), list):
        rec["mots_cles_matieres"] = ", ".join(rec["mots_cles_matieres"])

# Export CSV
df = pd.DataFrame(records)
df.to_csv("digen4_nettoye_complet.csv", index=False, encoding="utf-8")

print("✅ Fichier nettoyé avec tous les champs sauvegardé sous : digen4_nettoye_complet.csv")
