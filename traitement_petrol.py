import pandas as pd
import re

# Charger les lignes du fichier
with open('petrol.csv', 'r', encoding='utf-8', errors='ignore') as f:
    lines = [line.strip() for line in f if line.strip()]

# Séparer les enregistrements à partir des lignes de séparation (ex: '-----')
records = []
record = []
for line in lines:
    if re.match(r'^-+$', line):
        if record:
            records.append(record)
            record = []
    else:
        record.append(line)
if record:
    records.append(record)

# Fonction d'extraction de chaque enregistrement
def parse_petrol(rec):
    data = {
        'reference': '',
        'cote': '',
        'auteur': '',
        'titre': '',
        'publication': '',
        'pagination': '',
        'type_doc': '',
        'des_mat': '',
        'des_geo': ''
    }

    for line in rec:
        if re.match(r'^\d{4}$', line):
            data['reference'] = line
        elif line.startswith('COTE'):
            data['cote'] = line.split(':', 1)[-1].strip()
        elif line.startswith('AUTEUR'):
            data['auteur'] += line.split(':', 1)[-1].strip() + ' '
        elif 'TITRE' in line:
            data['titre'] += line.split(':', 1)[-1].strip() + ' '
        elif re.match(r'^- .*:\s.*,\s*\w+', line):  # publication
            data['publication'] = line.strip('- ').strip()
        elif re.match(r'^-.*p', line.lower()) or 'pag.' in line.lower():
            data['pagination'] = line.strip('- ').strip()
        elif re.match(r'^-?\(.*\)$', line):  # type de document entre parenthèses
            data['type_doc'] = line.strip('-() ')
        elif 'DES. MAT.' in line:
            data['des_mat'] = line.split(':', 1)[-1].strip()
        elif 'DES. GEO.' in line:
            data['des_geo'] = line.split(':', 1)[-1].strip()
        else:
            # Compléments de titre
            if data['titre']:
                data['titre'] += line.strip() + ' '
    return data

# Appliquer à tous les enregistrements
cleaned_petrol = [parse_petrol(rec) for rec in records]
df_petrol = pd.DataFrame(cleaned_petrol)

# Nettoyage final : supprimer les lignes trop vides
df_petrol = df_petrol.fillna('')
df_petrol = df_petrol[df_petrol.apply(lambda row: (row != '').sum() > 2, axis=1)]

# Sauvegarde
df_petrol.to_csv('petrol_epure.csv', index=False, encoding='utf-8')
print("✅ Fichier nettoyé : petrol_epure.csv")
