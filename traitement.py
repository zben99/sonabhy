import fitz  # PyMuPDF
import csv
import re
import os

def xps_to_csv(xps_file, csv_file, column_split_pattern=None):
    """
    Convertit un fichier XPS en fichier CSV basique.
    
    xps_file : Chemin vers le fichier .xps
    csv_file : Chemin du fichier CSV de sortie
    column_split_pattern : Expression régulière ou caractère 
                           pour séparer les colonnes (optionnel)
    """
    # Ouvre le fichier XPS
    doc = fitz.open(xps_file)
    
    all_rows = []
    
    for page_index in range(len(doc)):
        page = doc[page_index]
        # Récupère tout le texte brut de la page
        page_text = page.get_text("text")
        
        # Découpe le texte en lignes (selon les retours à la ligne)
        lines = page_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue  # ignore les lignes vides
            
            # Si on a un motif pour découper en colonnes, on l'utilise
            if column_split_pattern:
                # Exemple : split par point-virgule, tabulation, etc.
                # line_columns = re.split(column_split_pattern, line)
                line_columns = re.split(column_split_pattern, line)
            else:
                # Sinon on met la ligne complète dans une seule colonne
                line_columns = [line]
            
            all_rows.append(line_columns)
    
    # Écrit tout dans un CSV
    with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        for row in all_rows:
            writer.writerow(row)

if __name__ == "__main__":
    # Exemple d'utilisation du script
    # Dossier contenant les XPS
    xps_folder = "C:\dev_python\sonabhy\mes_xps"
    # Dossier où stocker les CSV
    csv_folder = "C:\dev_python\sonabhy\mes_csv"
    os.makedirs(csv_folder, exist_ok=True)
    
    # Motif de découpe : ici on suppose qu'il y a des tabulations ou plusieurs espaces
    # pour séparer les colonnes
    pattern = r'\t+|\s{4,}'  # tabulation OU au moins 4 espaces
    
    # Parcours tous les .xps du dossier
    for filename in os.listdir(xps_folder):
        if filename.lower().endswith(".xps"):
            xps_path = os.path.join(xps_folder, filename)
            csv_path = os.path.join(csv_folder, filename.replace(".xps", ".csv"))
            
            # Conversion
            xps_to_csv(xps_path, csv_path, column_split_pattern=pattern)
            
            print(f"Fichier converti : {xps_path} -> {csv_path}")
