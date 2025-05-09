import re
import csv

fichiers = ["dadfi3-1.csv", "dadfi3-2.csv", "dadfi3-3.csv", "dadfi3-4.csv"]

labels_attendus = ["TITRE/DOSSIER", "CONTENU", "ANA. DIPLOM.", "DATE EXTREME", "ANNEE", 
                   "OBSERVATIONS", "SORT FINAL", "PASSAGE GED"]

separateur_re = re.compile(r'^[~\s]+$')
id_re = re.compile(r'^\d{5}$')

enregistrements = []

for nom_fichier in fichiers:
    with open(nom_fichier, encoding="utf-8", errors="ignore") as f:
        enreg_courant = None
        champ_courant = None
        
        for ligne in f:
            ligne = ligne.rstrip("\n")
            
            if separateur_re.match(ligne):
                if enreg_courant:
                    enregistrements.append(enreg_courant)
                enreg_courant = None
                champ_courant = None
                continue
            
            if enreg_courant is None:
                if id_re.match(ligne):
                    enreg_courant = {}
                    enreg_courant["ID"] = ligne.strip()
                    champ_courant = None
                    continue
                else:
                    continue
            
            if champ_courant is None and "Code" not in enreg_courant:
                enreg_courant["Code"] = ligne.strip()
                champ_courant = None
                continue
            
            idx = ligne.find(':')
            if idx != -1:
                label_brut = ligne[:idx]
                valeur_brut = ligne[idx+1:]
                label_net = label_brut.strip().rstrip(';').rstrip('"').strip()
                
                if label_net.upper() in (lbl.upper() for lbl in labels_attendus):
                    champ_courant = label_net
                    valeur = valeur_brut.lstrip()
                    if valeur.startswith('"') and valeur.endswith('"') and len(valeur) > 1:
                        valeur = valeur[1:-1]
                    enreg_courant[champ_courant] = valeur
                    continue
                else:
                    champ_courant = label_net or "INCONNU"
                    valeur = valeur_brut.lstrip()
                    if valeur.startswith('"') and valeur.endswith('"') and len(valeur) > 1:
                        valeur = valeur[1:-1]
                    enreg_courant[champ_courant] = valeur
                    continue
            
            if champ_courant:
                texte_actuel = enreg_courant.get(champ_courant, "")
                if texte_actuel.endswith('-'):
                    enreg_courant[champ_courant] = texte_actuel[:-1] + ligne.strip()
                else:
                    enreg_courant[champ_courant] = texte_actuel + " " + ligne.strip()
        
        if enreg_courant:
            enregistrements.append(enreg_courant)

enregistrements_propres = []
codes_rencontres = set()

for enreg in enregistrements:
    code = enreg.get("Code", "").strip()
    if code in codes_rencontres:
        continue
    codes_rencontres.add(code)

    enreg_fields = {
        "ID": enreg.get("ID", "").strip(),
        "Code": code,
        "TITRE/DOSSIER": enreg.get("TITRE/DOSSIER", "").strip(),
        "CONTENU": enreg.get("CONTENU", "").strip(),
        "ANA. DIPLOM.": enreg.get("ANA. DIPLOM.", "").strip(),
        "DATE EXTREME": enreg.get("DATE EXTREME", "").strip(),
        "ANNEE": enreg.get("ANNEE", "").strip(),
        "OBSERVATIONS": enreg.get("OBSERVATIONS", "").strip(),
        "SORT FINAL": enreg.get("SORT FINAL", "").strip(),
        "PASSAGE GED": enreg.get("PASSAGE GED", "").strip()
    }

    date_val = enreg_fields["DATE EXTREME"] or enreg_fields["ANNEE"]
    if re.fullmatch(r'\d{8,}', date_val):
        groupes = [date_val[i:i+4] for i in range(0, len(date_val), 4)]
        date_val = " ".join(groupes)
    enreg_fields["DATE"] = date_val.strip()

    for champ in ["TITRE/DOSSIER", "CONTENU", "ANA. DIPLOM.", "OBSERVATIONS", "SORT FINAL"]:
        valeur = enreg_fields.get(champ, "")
        if valeur:
            valeur = re.sub(r'\s*/\s*', '/', valeur)
            valeur = re.sub(r'\s+;', ';', valeur)
            valeur = re.sub(r'\s+,', ',', valeur)
            valeur = re.sub(r'(\d)\s*-\s*(\d)', r'\1-\2', valeur)
            valeur = re.sub(r'\s+', ' ', valeur)
            valeur = valeur.strip()
            valeur = valeur.replace("COSERVATION", "CONSERVATION")
            enreg_fields[champ] = valeur

    pg_val = enreg_fields.get("PASSAGE GED", "")
    if pg_val and pg_val.upper() != "NON":
        enreg_fields["BASE GED"] = pg_val.strip()
        enreg_fields["PASSAGE GED"] = "OUI"
    else:
        enreg_fields["BASE GED"] = ""
        enreg_fields["PASSAGE GED"] = "NON"

    obs_val = enreg_fields.get("OBSERVATIONS", "")
    nb_pieces = ""
    if obs_val:
        match = re.fullmatch(r'\s*(\d+)\s+PIECES?\s*', obs_val, flags=re.IGNORECASE)
        if match:
            nb_pieces = str(int(match.group(1)))
            obs_val = ""
    enreg_fields["N° PIECES"] = nb_pieces
    enreg_fields["OBSERVATIONS"] = obs_val.strip()

    enregistrements_propres.append(enreg_fields)

champs_final = ["ID", "Code", "TITRE/DOSSIER", "CONTENU", "N° PIECES", "DATE", 
                "OBSERVATIONS", "SORT FINAL", "ANA. DIPLOM.", "PASSAGE GED", "BASE GED"]

with open("dadfi3_final_propre.csv", "w", encoding="utf-8", newline="") as sortie:
    writer = csv.DictWriter(sortie, fieldnames=champs_final)
    writer.writeheader()
    for enreg in enregistrements_propres:
        for cle in ['DATE EXTREME', 'ANNEE']:
            enreg.pop(cle, None)
        writer.writerow(enreg)

print(f"Fichier 'dadfi3_final_propre.csv' généré avec {len(enregistrements_propres)} enregistrements.")
