import pandas as pd
import re

# Ouvrir le fichier source en mode texte
with open('dadfi2.csv', 'r', encoding='utf-8', errors='ignore') as f:
    lines = f.readlines()

records = []            # Liste qui contiendra chaque enregistrement sous forme de dictionnaire
current_record = None   # Dictionnaire pour l'enregistrement en cours de construction
last_field = None       # Nom du dernier champ traité (utile pour gérer les continuations de texte)

# Ensemble des noms de champs "normaux" attendus dans le fichier
known_fields = {"TITRE/DOSSIER", "CONTENU", "N° PIECES", "N¢ PIECES", 
                "ANA. DIPLOM.", "DATE", "OBSERVATIONS", 
                "PASSAGE GED", "BASE GED", "SORT FINAL", "CLI/FOUR"}

for raw_line in lines:
    # Retirer les sauts de ligne et espaces superflus en fin de ligne
    line = raw_line.rstrip('\n').rstrip('\r')
    # Éliminer les guillemets doubles isolés (artéfacts d'OCR) du début/fin de ligne 
    # et caractères de contrôle éventuels
    line = line.strip().replace('"', '')
    if line == "":
        continue  # Ignorer les lignes vides (s'il y en a)

    # Vérifier si la ligne est un séparateur (suite de ~)
    if re.match(r'^[~\s]+$', line):
        # Nouvelle ligne de tildes -> fin de l'enregistrement courant
        if current_record:
            records.append(current_record)
        current_record = None
        last_field = None
        continue

    # Vérifier si la ligne correspond à un ID (5 chiffres en début d'enregistrement)
    if re.match(r'^\d{5}$', line):
        # Début d'un nouvel enregistrement
        if current_record:
            records.append(current_record)
        current_record = {"ID": line}
        last_field = None
        continue

    # Si nous ne sommes pas dans un enregistrement (cas improbable), passer
    if current_record is None:
        continue

    # Normaliser les séparateurs de champ ';:' en ':' pour éviter les incohérences
    if ';:' in line:
        line = line.replace(';:', ':')

    # Gestion des lignes de continuation du TITRE/DOSSIER sur plusieurs lignes :
    if last_field == "TITRE/DOSSIER":
        # Cas 1 : la ligne précédente de titre se terminait par "N°" et 
        # cette ligne commence par un numéro du type 1234/2008:
        # -> on l'attache directement (pas d'espace, pour coller "N°1234/2008")
        if current_record["TITRE/DOSSIER"].endswith("N°") and re.match(r'^\d+/\d{4}:', line):
            current_record["TITRE/DOSSIER"] += line
            continue
        # Cas 2 : cette ligne comporte un ':' mais son libellé n'est pas un champ connu
        # -> c'est probablement la suite du titre contenant un ':', on la concatène
        if ':' in line:
            field_label = line.split(':', 1)[0].strip()
            if field_label not in known_fields:
                current_record["TITRE/DOSSIER"] += ' ' + line
                continue
        # Cas 3 : ligne sans ':' -> continuation simple du titre
        else:
            current_record["TITRE/DOSSIER"] += ' ' + line.strip()
            continue

    # Si la ligne ne contient pas de ':', c’est soit la ligne du code, soit une continuation d'un champ précédent
    if ':' not in line:
        if last_field is None:
            # Si aucun champ en cours, cette ligne est le code de l'entrée (ex: "0001/SP")
            current_record["Code"] = line.strip()
        else:
            # Sinon, on l'ajoute à la suite du contenu du champ précédent (champ multi-ligne)
            current_record[last_field] += ' ' + line.strip()
        continue

    # À ce stade, la ligne contient un champ avec un libellé et une valeur séparés par ':'
    # On sépare le nom de champ de sa valeur
    field_name, field_value = line.split(':', 1)
    field_name = field_name.strip()
    field_value = field_value.strip()
    # Stocker le champ dans le dictionnaire de l'enregistrement
    current_record[field_name] = field_value
    last_field = field_name

# Ajouter le dernier enregistrement s'il n'a pas été clos par un séparateur
if current_record:
    records.append(current_record)

# Convertir la liste de dictionnaires en DataFrame pandas pour faciliter les traitements suivants
df = pd.DataFrame(records)

# Remplacer le mauvais caractère 'N¢' par 'N°' dans les noms de colonnes
if "N¢ PIECES" in df.columns:
    df.rename(columns={"N¢ PIECES": "N° PIECES"}, inplace=True)

# Supprimer les enregistrements sans titre (incomplets)
df = df[~df["TITRE/DOSSIER"].isna()]

# Supprimer les doublons exacts (mêmes valeurs sur tous les champs hors ID)
df = df.drop_duplicates(subset=[col for col in df.columns if col != "ID"])

# Renommer les colonnes avec des noms plus explicites et sans caractères spéciaux
df.rename(columns={
    "TITRE/DOSSIER": "Titre_dossier",
    "ANA. DIPLOM.": "Analyse_diplomatique",
    "N° PIECES": "Numeros_pieces",
    "PASSAGE GED": "Passage_GED",
    "BASE GED": "Base_GED",
    "SORT FINAL": "Sort_final",
    "CLI/FOUR": "Clients_Fournisseurs"
}, inplace=True)

# Optionnel : supprimer la colonne 'Clients_Fournisseurs' si jugée peu pertinente (très peu d'entrées concernées)
if "Clients_Fournisseurs" in df.columns:
    df.drop(columns=["Clients_Fournisseurs"], inplace=True)

# Nettoyer les espaces redondants à l'intérieur des champs texte (par ex. double espace après une virgule)
for col in ["Titre_dossier", "Contenu", "Date", "Observations"]:
    if col in df.columns:
        df[col] = df[col].str.replace(r'\s{2,}', ' ', regex=True)          # remplace 2+ espaces par 1
        df[col] = df[col].str.replace(r'\s+,', ',', regex=True)            # supprime l'espace avant virgule
        df[col] = df[col].str.replace(r';\s*', '; ', regex=True)           # uniformise l'espace après ';'
        df[col] = df[col].str.strip()                                     # trim final

# Enregistrer le fichier CSV nettoyé sans l'index
df.to_csv("dadfi2_propre.csv", index=False, encoding="utf-8")
