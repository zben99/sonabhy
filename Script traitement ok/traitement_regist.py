import pandas as pd
import re

# Lire les lignes du fichier
with open('regist.csv', 'r', encoding='utf-8', errors='ignore') as f:
    lines = [line.strip() for line in f if line.strip()]

# En-têtes à reconnaître et leur nom simplifié
headers_map = {
    'DATE DE VERSEMENT': 'date_versement',
    "DATE D'ENREGISTREMENT": 'date_enregistrement',
    'PROVENANCE': 'provenance',
    'COTE DE RANGEMENT': 'cote',
    'TITRE': 'titre',
    'CONTENU': 'contenu',
    'ANALYSE DIPLOMATIQUE': 'analyse',
    'DATES EXTREMES': 'dates_extremes',
    'ANNEE': 'annee',
    'PASSAGE A LA GED': 'ged',
    'DUREE UTILITE ADMINISTRATIVE': 'duree_utilite',
    'SORT FINAL': 'sort_final',
    'INITIAL': 'initial',
    'NIVEAU DE DESCRIPTION': 'niveau_description',
    'IMPORTANCE MATERIELLE': 'importance_materielle'
}

# Champs qui peuvent apparaître plusieurs fois
multi_fields = {'contenu', 'annee'}

def detect_header(line):
    for key in headers_map:
        if key in line:
            return headers_map[key]
    return None

# Traitement ligne par ligne
records = []
record = {}
current_field = None

for line in lines:
    field = detect_header(line)

    if field:
        if field == 'date_versement' and record:
            records.append(record)
            record = {}

        current_field = field
        parts = line.split(':', 1)
        value = parts[1].strip() if len(parts) == 2 else ''

        if field in multi_fields:
            record.setdefault(field, [])
            if value:
                record[field].append(value)
        else:
            record[field] = value

    else:
        if current_field:
            if current_field in multi_fields:
                record.setdefault(current_field, [])
                record[current_field].append(line.strip())
            else:
                record[current_field] = record.get(current_field, '') + ' ' + line.strip()

if record:
    records.append(record)

# Convertir en DataFrame
for rec in records:
    for field in multi_fields:
        if field in rec and isinstance(rec[field], list):
            rec[field] = '; '.join([x for x in rec[field] if x])

df = pd.DataFrame(records)

# Nettoyage
df = df.fillna('')
df = df[df.apply(lambda row: (row != '').sum() > 2, axis=1)]

# Sauvegarde
df.to_csv('regist_epure.csv', index=False, encoding='utf-8')
print("✅ Champ ANNEE géré. Fichier nettoyé : regist_epure.csv")
