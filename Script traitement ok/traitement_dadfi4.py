import pandas as pd
import re
from datetime import datetime

# Définition des codes de champs attendus (d’après l’analyse du fichier)
champs_codes = {"NFM", "10", "20", "30", "40", "50", "60", "70", 
                "80", "81", "83", "90", "100", "110", "120", "130", "140", "150"}

# Préparation pour champs pouvant se répéter (plusieurs occurrences par enregistrement)
champs_multiples = {"60", "70"}  # ex: mots-clés, types de documents

# Initialisation des variables de parcours
enregistrements = []   # liste qui contiendra tous les enregistrements sous forme de dictionnaires
enreg_courant = {}     # dictionnaire pour l'enregistrement en cours de construction
champ_courant = None   # code du champ en cours de lecture (pour ajouter les lignes de valeur suivantes)

# Lecture du fichier ligne par ligne en préservant les caractères spéciaux
with open("dadfi4.csv", "r", encoding="utf-8") as fichier:
    for ligne in fichier:
        # Retirer le caractère de fin de ligne (\n) sans supprimer les espaces éventuels en fin de ligne
        ligne_sans_saut = ligne.rstrip('\n')
        # Version trimée pour vérifier si c'est un code de champ (on ignore les espaces autour)
        ligne_stripee = ligne_sans_saut.strip()
        # Vérifier si la ligne correspond exactement à un code de champ attendu (ex: "50:")
        if ligne_stripee.endswith(':') and ligne_stripee[:-1] in champs_codes:
            # Identifiant du champ (sans les deux-points)
            code = ligne_stripee[:-1]
            if code == "NFM":
                # Nouveau record détecté (début d’un enregistrement)
                if enreg_courant:  
                    # Si un enregistrement était en cours, on le sauvegarde avant d’en commencer un nouveau
                    enregistrements.append(enreg_courant)
                enreg_courant = {}               # nouvel enregistrement
                enreg_courant["NFM"] = ""        # on ajoutera la valeur du NFM sur la prochaine ligne
                champ_courant = "NFM"
            else:
                # Champ normal (autre que NFM)
                if code in champs_multiples:
                    # Si c’est un champ à occurrences multiples (ex: 60 ou 70)
                    # Initialiser la liste s’il s’agit de la première occurrence
                    if code not in enreg_courant:
                        enreg_courant[code] = [""]
                    else:
                        # Ajouter une nouvelle entrée vide pour ce champ (seconde occurrence ou plus)
                        enreg_courant[code].append("")
                    champ_courant = code  # on continue à remplir ce champ (dernière entrée de la liste)
                else:
                    # Champ à occurrence unique : initialiser la valeur à vide
                    enreg_courant[code] = ""
                    champ_courant = code
        else:
            # La ligne ne correspond pas à un code de champ isolé, c’est soit une valeur (ou partie de valeur), soit une ligne vide
            if champ_courant is None:
                # Si aucun champ n'est en cours, on ignore (ligne isolée inattendue)
                continue
            # Ajouter le texte de la ligne à la valeur du champ en cours.
            # On utilise strip() pour enlever les espaces superflus en début/fin de ligne de valeur.
            texte = ligne_sans_saut.strip()
            if isinstance(enreg_courant[champ_courant], list):
                # Champ à valeurs multiples : ajouter le texte à la dernière entrée de la liste
                if enreg_courant[champ_courant][-1] != "":
                    enreg_courant[champ_courant][-1] += " " + texte
                else:
                    enreg_courant[champ_courant][-1] = texte
            else:
                # Champ simple : concaténer avec un espace si une valeur existe déjà
                if enreg_courant[champ_courant] != "":
                    enreg_courant[champ_courant] += " " + texte
                else:
                    enreg_courant[champ_courant] = texte
    # Fin de fichier : ajouter le dernier enregistrement à la liste
    if enreg_courant:
        enregistrements.append(enreg_courant)

# Conversion de la liste de dictionnaires en DataFrame pandas pour faciliter le nettoyage
df = pd.DataFrame(enregistrements)

# Combinaison des valeurs multiples des champs 60 et 70 en une seule chaîne par enregistrement
if "60" in df.columns:
    df["60"] = df["60"].apply(lambda vals: "; ".join(vals) if isinstance(vals, list) else vals)
if "70" in df.columns:
    df["70"] = df["70"].apply(lambda vals: "; ".join(vals) if isinstance(vals, list) else vals)

# Suppression de la colonne '81' si elle existe (presque aucun enregistrement ne la renseigne)
if "81" in df.columns:
    df.drop(columns="81", inplace=True)

# Uniformisation du format des dates pour les champs 10 et 20 (JJ-MM-AAAA -> AAAA-MM-JJ)
for col in ["10", "20"]:
    if col in df.columns:
        def format_date(val):
            if pd.isna(val) or str(val).strip() == "":
                return ""  # laisser vide si pas de date
            s = str(val).strip().replace('/', '-')  # remplacer '/' par '-' pour uniformiser
            try:
                # Essayer de parser la date en considérant le jour en premier
                date_obj = datetime.strptime(s, "%d-%m-%Y")
            except ValueError:
                return s  # si le format est inattendu, on le laisse tel quel
            return date_obj.strftime("%Y-%m-%d")
        df[col] = df[col].apply(format_date)

# Nettoyage du format du champ "100" (Durée de conservation) : enlever les zéros non significatifs
if "100" in df.columns:
    df["100"] = df["100"].astype(str).apply(lambda x: re.sub(r'^0+(\d)', r'\1', x.strip()) if pd.notna(x) else x)

# Correction des inversions entre champ 100 et 110 si présentes (ex: "10 ANS" saisi en sort final, et "Conservation définitive" en durée)
if "100" in df.columns and "110" in df.columns:
    for idx, row in df.iterrows():
        val100 = str(row["100"]).strip().upper()
        val110 = str(row["110"]).strip().upper()
        # Identifier si 100 contient une valeur textuelle de sort final et 110 une durée en années
        if val100 in {"CONSERVATION DEFINITIVE", "CONSERVATION", "DESTRUCTION", "CD"} and re.match(r'^\d+\s*ANS$', val110):
            # Inversion détectée: on permute les deux valeurs
            df.at[idx, "100"], df.at[idx, "110"] = row["110"], row["100"]

# Standardisation des valeurs du champ 110 (Sort final) : corriger typos et abréviations
if "110" in df.columns:
    def nettoyer_sort_final(val):
        if pd.isna(val):
            return ""
        v = str(val).strip().upper()
        # Différentes formes de "Conservation définitive" à uniformiser
        if v in {"CONSERVATION", "CONSERVATION DEFINITIVE", "CONSERVATTION DEFINITIVE", 
                 "CNSERVATION", "CONSERVAION DEFINITIVE", "CONSERVATON", "CONSERVATION DEFINTIVE", "CONVERVATION", "CD"}:
            return "CONSERVATION DEFINITIVE"
        elif "DESTRUCTION" in v:
            return "DESTRUCTION"
        else:
            return v
    df["110"] = df["110"].apply(nettoyer_sort_final)

# Nettoyage du champ 120 (Observations) : supprimer guillemets entourant le texte et corriger "Nø" en "N°"
if "120" in df.columns:
    def nettoyer_observations(val):
        if pd.isna(val):
            return ""
        text = str(val).strip()
        # Enlever les guillemets ouvrants/fermants s'ils encadrent toute la chaîne
        if text.startswith('"') and text.endswith('"'):
            text = text[1:-1]
        # Remplacer la notation "Nø" par "N°"
        return text.replace("Nø", "N°")
    df["120"] = df["120"].apply(nettoyer_observations)

# Renommage des colonnes avec des noms clairs
noms_colonnes = {
    "NFM": "ID",
    "40": "Cote",
    "50": "Intitulé",
    "80": "Période début",
    "83": "Période fin",
    "140": "Type",
    "150": "Quantité",
    "30": "Fonction",
    "130": "Code fonction",
    "60": "Mots-clés",
    "70": "Types de documents",
    "10": "Date ouverture",
    "20": "Date clôture",
    "90": "Système",
    "100": "Durée conservation",
    "110": "Sort final",
    "120": "Observations"
}
df.rename(columns=noms_colonnes, inplace=True)

# Suppression des doublons d’enregistrements sur la colonne "Cote" (on garde le premier et on élimine les suivants)
if "Cote" in df.columns:
    df.drop_duplicates(subset="Cote", keep="first", inplace=True)

# Enregistrement du nouveau fichier CSV propre
df.to_csv("dadfi4_propre.csv", index=False, encoding="utf-8")
print("Nettoyage terminé - fichier 'dadfi4_propre.csv' généré avec succès.")
