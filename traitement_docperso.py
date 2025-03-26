import pandas as pd
import re

# Charger les lignes utiles
with open('docperso.csv', 'r', encoding='utf-8', errors='ignore') as f:
    lines = [line.strip() for line in f if line.strip()]

# Séparer les blocs à partir du séparateur
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

# Fonction pour parser un enregistrement
def parse_docperso(block):
    data = {
        'reference': '',
        'code': '',
        'titre': '',
        'contenu': '',
        'analyse': '',
        'date_extreme': '',
        'passage_ged': '',
        'sort_final': ''
    }

    for i, line in enumerate(block):
        if re.match(r'^\d{5}$', line):
            data['reference'] = line
        elif re.match(r'^[A-Z]+/\d+', line):
            data['code'] = line
        elif 'TITRE/DOSSIER' in line:
            data['titre'] += line.split(':', 1)[-1].strip() + ' '
            if i + 1 < len(block):
                data['titre'] += block[i + 1].strip()
        elif 'CONTENU' in line:
            data['contenu'] = line.split(':', 1)[-1].strip()
        elif 'ANA. DIPLOM.' in line or 'ANALYSE DIPLOMATIQUE' in line:
            data['analyse'] += line.split(':', 1)[-1].strip()
        elif 'DATE EXTREME' in line:
            data['date_extreme'] = line.split(':', 1)[-1].strip()
        elif 'PASSAGE GED' in line:
            data['passage_ged'] = line.split(':', 1)[-1].strip()
        elif 'SORT FINAL' in line:
            data['sort_final'] = line.split(':', 1)[-1].strip()

    return data

# Appliquer le parsing à tous les blocs
parsed_data = [parse_docperso(block) for block in records]

# En DataFrame
df = pd.DataFrame(parsed_data)

# Nettoyage : supprimer les lignes presque vides
df = df.fillna('')
df = df[df.apply(lambda row: (row != '').sum() > 2, axis=1)]

# Export
df.to_csv('docperso_epure.csv', index=False, encoding='utf-8')
print("✅ Fichier nettoyé : docperso_epure.csv")
