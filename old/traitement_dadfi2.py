import csv
import re

def clean_and_structure_csv(input_file, output_file):
    # Lire le fichier brut et nettoyer les lignes
    with open(input_file, "r", encoding="utf-8") as f:
        lines = [line.replace("\f", "").strip().strip('"') for line in f if line.strip() and "~" not in line]
    
    # Initialisation des variables
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
    structured_data = []
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
        numero_piece = ""
        date = ""
        passage_ged = ""
        base_ged = ""
        sort_finale = ""
    
        section = ""
    
        for line in record[2:]:
            line = " ".join(line.split())  # Supprime les espaces excessifs
    
            if line.startswith("TITRE/DOSSIER"):
                section = "titre"
                titre = line.split(":", 1)[-1].strip()
            elif "CONTENU" in line:
                section = "contenu_dossier"
                contenu_dossier = line.split(":", 1)[-1].strip()
            elif "ANA. DIPLOM." in line:
                section = "analyse_diplomatique"
                analyse_diplomatique = line.split(":", 1)[-1].strip()
            elif "N° PIECES" in line:
                section = "numero_piece"
                numero_piece = line.split(":", 1)[-1].strip()
            elif "DATE" in line:
                section = "date"
                date = line.split(":", 1)[-1].strip()
            elif "PASSAGE GED" in line:
                section = "passage_ged"
                passage_ged = line.split(":", 1)[-1].strip()
            elif "BASE GED" in line:
                section = "base_ged"
                base_ged = line.split(":", 1)[-1].strip()
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
                elif section == "numero_piece":
                    numero_piece += " " + line
                elif section == "date":
                    date += " " + line
                elif section == "passage_ged":
                    passage_ged += " " + line
                elif section == "base_ged":
                    base_ged += " " + line
                elif section == "sort_finale":
                    sort_finale += " " + line
    
        structured_data.append([
            id_record,
            cote_rangement,
            titre.strip(),
            contenu_dossier.strip(),
            analyse_diplomatique.strip(),
            numero_piece.strip(),
            date.strip(),
            passage_ged.strip(),
            base_ged.strip(),
            sort_finale.strip()
        ])
    
    # Écrire les données nettoyées dans un fichier CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ID", "Cote", "Titre", "Contenu Dossier", "Analyse Diplomatique",
            "Numero/Piece", "Date", "Passage GED", "Base GED", "Sort Finale"
        ])
        writer.writerows(structured_data)
    
    print(f"✅ Fichier nettoyé et sauvegardé sous '{output_file}'.")

# Exemple d'utilisation
input_file = "dadfi2.csv"
output_file = "dadfi2_epuree.csv"
clean_and_structure_csv(input_file, output_file)
