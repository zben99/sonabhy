import pandas as pd
import re

# Charger le fichier
with open('ditec3.csv', 'r', encoding='utf-8', errors='ignore') as f:
    lines = [line.strip() for line in f if line.strip()]

# Séparer les enregistrements à partir des lignes de séparation (~~~~)
records = []
record = []
for line in lines:
    if '~' in line:
        if record:
            records.append(record)
            record = []
    else:
        record.append(line)
if record:
    records.append(record)

# Fonction pour extraire les champs
def parse_record(rec):
    data = {
        'reference': '',
        'code': '',
        'titre': '',
        'contenu': '',
        'analyse': '',
        'date': ''
    }
    for line in rec:
        if re.match(r'^\d{5}$', line):  # référence
            data['reference'] = line
        elif re.match(r'^\d{4}/', line):  # code
            data['code'] = line
        elif 'TITRE' in line:
            data['titre'] += line.split(':', 1)[-1].strip() + ' '
        elif 'CONTENU' in line:
            data['contenu'] += line.split(':', 1)[-1].strip() + ' '
        elif 'ANA' in line:
            data['analyse'] = line.split(':', 1)[-1].strip()
        elif 'DATE' in line or 'JOUR' in line:
            data['date'] += line.split(':', 1)[-1].strip()
        else:
            # Compléments du titre ou contenu
            if data['titre'] and not data['contenu']:
                data['titre'] += line.strip() + ' '
            elif data['contenu']:
                data['contenu'] += line.strip() + ' '
    return data

# Nettoyer les enregistrements
clean_data = [parse_record(rec) for rec in records]

# Convertir en DataFrame
df = pd.DataFrame(clean_data)

# Supprimer les lignes quasiment vides
df = df.fillna('')
df = df[~(df == '').all(axis=1)]
df = df[df.apply(lambda row: (row != '').sum() > 1, axis=1)]

# Sauvegarder
df.to_csv('ditec3_epure.csv', index=False, encoding='utf-8')
print("✅ Base 'ditec3.csv' épurée et enregistrée sous 'ditec3_epure.csv'")
