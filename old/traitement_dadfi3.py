import csv
import re
import itertools

def clean_and_structure_csv(input_files, output_file):
    structured_data = []
    
    for input_file in input_files:
        print(f"Traitement du fichier : {input_file}")
        
        # Lire le fichier brut et nettoyer les lignes
        with open(input_file, "r", encoding="utf-8") as f:
            lines = [line.replace("\f", "").strip().strip('"') for line in f if line.strip() and "~" not in line]
        
        records = []
        current_record = []
        
        # Regrouper les données en enregistrements complets
        for line in lines:
            if re.match(r'^\d+$', line):  # Nouvelle entrée détectée (ID numérique)
                if current_record:
                    records.append(current_record)  # Ajouter l'ancien enregistrement
                current_record = [line]  # Commencer un nouvel enregistrement
            else:
                current_record.append(line)
        
        if current_record:
            records.append(current_record)  # Ajouter le dernier enregistrement
        
        # Vérifier et traiter les enregistrements
        for record in records:
            if len(record) < 2:
                print(f"⚠️ Enregistrement ignoré : {record}")
                continue
        
            id_record = record[0]  # ID
            cote_rangement = record[1]  # Cote de rangement
        
            # Initialisation des champs
            titre = ""
            contenu_dossier = ""
            analyse_diplomatique = ""
            annee = ""
            date_extreme = ""
            passage_ged = ""
            sort_finale = ""
        
            section = "titre"  # Tout commence comme titre par défaut
        
            for line in record[2:]:
                line = " ".join(line.split())  # Supprime les espaces excessifs
        
                if "CONTENU" in line:
                    section = "contenu_dossier"
                    contenu_dossier = line.split(":", 1)[-1].strip()
                elif "ANA. DIPLOM." in line:
                    section = "analyse_diplomatique"
                    analyse_diplomatique = line.split(":", 1)[-1].strip()
                elif "DATE EXTREME" in line:
                    section = "date_extreme"
                    date_extreme = line.split(":", 1)[-1].strip()
                elif "ANNEE" in line:
                    section = "annee"
                    annee = line.split(":", 1)[-1].strip()
                elif "PASSAGE GED" in line:
                    section = "passage_ged"
                    passage_ged = line.split(":", 1)[-1].strip()
                elif "SORT FINAL" in line:
                    section = "sort_finale"
                    sort_finale = line.split(":", 1)[-1].strip()
                else:
                    if section == "titre":
                        titre += " " + line
                    elif section == "contenu_dossier":
                        contenu_dossier += " " + line
                    elif section == "analyse_diplomatique":
                        analyse_diplomatique += " " + line
                    elif section == "date_extreme":
                        date_extreme += " " + line
                    elif section == "annee":
                        annee += " " + line
                    elif section == "passage_ged":
                        passage_ged += " " + line
                    elif section == "sort_finale":
                        sort_finale += " " + line
        
            # Correction : concaténer l'année au titre si mal séparé
            if annee and annee not in titre:
                titre = titre.strip() + " " + annee.strip()
        
            structured_data.append([
                id_record,
                cote_rangement,
                titre.strip(),
                contenu_dossier.strip(),
                analyse_diplomatique.strip(),
                date_extreme.strip(),
                annee.strip(),
                passage_ged.strip(),
                sort_finale.strip()
            ])
    
    # Écrire les données fusionnées nettoyées dans un fichier CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ID", "Cote", "Titre", "Contenu Dossier", "Analyse Diplomatique",
            "Date Extrême", "Année", "Passage GED", "Sort Finale"
        ])
        writer.writerows(structured_data)
    
    print(f"✅ Fichier nettoyé et fusionné sauvegardé sous '{output_file}'.")

# Exemple d'utilisation
input_files = [
    "dadfi3-1.csv",
    "dadfi3-2.csv",
    "dadfi3-3.csv",
    "dadfi3-4.csv"
]
output_file = "dadfi3_epuree.csv"
clean_and_structure_csv(input_files, output_file)
