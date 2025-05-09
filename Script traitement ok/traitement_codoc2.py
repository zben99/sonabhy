import fitz  # PyMuPDF
import csv
import re

# 1. Ouvrir le document XPS et extraire tout le texte
doc = fitz.open("codoc2.xps")  # ouvrir le XPS
texte_complet = ""
for page in doc:
    texte_complet += page.get_text()  # extraire le texte de chaque page

# 2. Séparer les enregistrements par la ligne de tildes
lignes = texte_complet.splitlines()
enregistrements = []
enreg_courant = []
for ligne in lignes:
    # Détecter la ligne séparatrice (ici on considère une ligne constituée principalement de '~')
    if ligne.strip().startswith("~"):  
        # Fin de bloc d'enregistrement
        if enreg_courant:
            enregistrements.append(enreg_courant)
        enreg_courant = []  # réinitialiser pour le prochain enregistrement
    else:
        # Ajouter la ligne courante au bloc en cours (si non vide)
        if ligne != "":
            enreg_courant.append(ligne)

# (Optionnel) Si le dernier enregistrement n'est pas suivi d'une ligne de tildes, l'ajouter
if enreg_courant:
    enregistrements.append(enreg_courant)

# 3. Définir les en-têtes des colonnes CSV (d'après les libellés qu’on s’attend à trouver)
headers = ["ID", "Code", "TITRE/DOSSIER", "CONTENU", "ANA. DIPLOM.", "ANNEE", "PASSAGE GED", "SORT FINAL"]

# Préparer la structure pour les données CSV
donnees_csv = []

# 4. Parser chaque enregistrement pour extraire les champs
for enreg in enregistrements:
    if not enreg:  # ignorer les enregistrements vides éventuels
        continue
    # Initialiser un dict pour les champs de cet enregistrement
    champs = {h: "" for h in headers}
    # Traiter la première et deuxième ligne séparément
    if len(enreg) >= 1:
        champs["ID"] = enreg[0].strip()
    if len(enreg) >= 2:
        champs["Code"] = enreg[1].strip()
    # Traiter les lignes suivantes pour les champs nommés
    last_label = None
    for ligne in enreg[2:]:  # à partir de la 3e ligne
        # Vérifier si la ligne contient un libellé de champ (séparé par ":")
        if ":" in ligne:
            # On sépare sur le premier ":" uniquement
            partie_gauche, partie_droite = ligne.split(":", 1)
            label = partie_gauche.strip()
            valeur = partie_droite.strip()
            # Heuristique : si le label est purement numérique, alors ce n'est pas un vrai label mais une continuation
            if re.match(r'^\d*$', label):
                # Continuité du champ précédent
                if last_label:
                    champs[last_label] += " " + (label + ":" + valeur).strip()
                # (Si last_label est None, on pourrait logguer un avertissement d'anomalie)
            else:
                # Nouveau champ détecté
                if label in champs:
                    champs[label] = valeur
                else:
                    # Si le label n'est pas dans notre dictionnaire (nouveau champ inattendu),
                    # on peut l'ajouter dynamiquement
                    champs[label] = valeur
                    # (Éventuellement aussi l’ajouter à headers si on veut conserver l'ordre)
                last_label = label
        else:
            # Pas de ":" dans la ligne -> c'est la suite du dernier champ
            if last_label:
                # Concaténer en séparant par un espace (on peut aussi utiliser "\n" si on veut garder un retour ligne)
                champs[last_label] += " " + ligne.strip()
            # Sinon, si pas de last_label, on pourrait ignorer ou traiter différemment selon le cas.
    # Ajouter les valeurs des champs dans l'ordre des headers pour cet enregistrement
    donnees_csv.append([champs.get(col, "") for col in headers])

# 5. Écrire les données dans un fichier CSV UTF-8
with open("codoc2.csv", "w", newline='', encoding='utf-8') as f_csv:
    writer = csv.writer(f_csv)
    writer.writerow(headers)            # écrire l’en-tête
    writer.writerows(donnees_csv)       # écrire toutes les lignes d'enregistrements
