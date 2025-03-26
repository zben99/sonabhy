import pandas as pd
import re

# Charger le fichier brut
with open('ouref.csv', 'r', encoding='utf-8', errors='ignore') as f:
    lines = [line.strip() for line in f if line.strip()]

# Étape 1 : Séparer dynamiquement les blocs à partir des références (ex : '001', '002', ...)
records = []
record_lines = []
for line in lines:
    if re.fullmatch(r'\d{3}', line):  # début d'un nouveau enregistrement
        if record_lines:
            records.append(record_lines)
            record_lines = []
    record_lines.append(line)
if record_lines:
    records.append(record_lines)

# Étape 2 : Parser chaque bloc
parsed = []
for rec in records:
    entry = {
        'reference': '',
        'cote': '',
        'auteur': '',
        'titre': '',
        'publication': '',
        'pagination': '',
        'descripteurs': ''
    }
    if len(rec) >= 1:
        entry['reference'] = rec[0]
    if len(rec) >= 2:
        entry['cote'] = rec[1]
    if len(rec) >= 3:
        entry['auteur'] = rec[2]
    if len(rec) >= 4:
        entry['titre'] = rec[3]
    if len(rec) >= 5 and rec[4].startswith('-'):
        entry['publication'] = rec[4].strip('- ').strip()
    if len(rec) >= 6 and rec[5].startswith('-'):
        entry['pagination'] = rec[5].strip('- ').strip()
    if len(rec) >= 7 and rec[6].startswith('/'):
        entry['descripteurs'] = rec[6]

    parsed.append(entry)

# Étape 3 : Créer DataFrame propre
df = pd.DataFrame(parsed)

# Supprimer les lignes vides ou très incomplètes
df = df.fillna('')
df = df[df.apply(lambda row: (row != '').sum() > 2, axis=1)]

# Export du résultat
df.to_csv('ouref_epure.csv', index=False, encoding='utf-8')
print("✅ Nettoyage terminé : ouref_epure.csv")
