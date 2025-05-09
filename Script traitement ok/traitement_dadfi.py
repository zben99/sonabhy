import os
import sys
import fitz                       # PyMuPDF
import pytesseract               # OCR Tesseract wrapper
from PIL import Image
import io
import csv
import re

def process_xps_file(xps_path):
    """Extrait les tableaux d'un fichier XPS et les enregistre en CSV."""
    doc = fitz.open(xps_path)
    all_tables = []  # pour accumuler les tables extraites
    for page_index in range(doc.page_count):
        page = doc[page_index]
        # Tenter de détecter des tableaux sur la page via PyMuPDF
        tables = []
        if hasattr(page, "find_tables"):
            try:
                table_finder = page.find_tables()      # détecte les tableaux sur la page
            except Exception:
                table_finder = None
            if table_finder:
                # `table_finder` peut être un objet contenant les tables détectées
                for table in table_finder:
                    try:
                        data = table.extract()        # extrait le contenu du tableau (liste de lignes)
                    except Exception:
                        # Certains versions utilisent table_finder.tables
                        data = table
                    if data:
                        tables.append(data)
        # Si find_tables n'existe pas ou n'a rien trouvé, on peut essayer une autre stratégie
        if not tables:
            # Vérifier s'il y a du texte brut sur la page
            text = page.get_text().strip()
            if text == "":
                # Aucune texte extrait -> probablement une image scannée, on utilise l'OCR
                # Rendre la page en image (augmentation de résolution pour améliorer l'OCR)
                pix = page.get_pixmap(dpi=300)
                img_bytes = pix.tobytes("png")
                image = Image.open(io.BytesIO(img_bytes))
                # Optionnel: spécifier la langue si le texte est en français: lang="fra"
                ocr_text = pytesseract.image_to_string(image, config="--psm 6")
                # Découper le texte OCR en lignes, puis en colonnes
                lines = ocr_text.strip().splitlines()
                ocr_table = []
                for line in lines:
                    # Utiliser des groupes d'espaces comme séparateurs de colonnes
                    columns = [col.strip() for col in re.split(r'\s{2,}', line) if col.strip()]
                    if columns:
                        ocr_table.append(columns)
                if ocr_table:
                    tables.append(ocr_table)
            else:
                # Il y a du texte extrait mais pas reconnu comme tableau.
                # On traite chaque ligne de texte comme une ligne de CSV (fallback simple).
                lines = text.splitlines()
                text_table = [[cell.strip() for cell in re.split(r'\s{2,}', ln) if cell.strip()] for ln in lines]
                text_table = [row for row in text_table if row]  # supprimer les lignes vides
                if text_table:
                    tables.append(text_table)
        # Ajouter les tables de cette page à la liste globale
        for t in tables:
            all_tables.append(t)
    doc.close()
    # Écriture du fichier CSV de sortie
    csv_path = os.path.splitext(xps_path)[0] + ".csv"
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for idx, table in enumerate(all_tables):
            for row in table:
                # Écrire chaque ligne du tableau dans le CSV
                writer.writerow(row)
            # Insérer une ligne vide entre les tableaux (si multiple tables)
            if idx < len(all_tables) - 1:
                writer.writerow([])
    print(f"Fichier CSV créé: {csv_path}")

# Si le script est exécuté directement, traiter les fichiers passés en arguments ou tous les XPS du dossier courant
if __name__ == "__main__":
    # Récupérer la liste des fichiers XPS à traiter
    if len(sys.argv) > 1:
        xps_files = sys.argv[1:]
    else:
        xps_files = [f for f in os.listdir('.') if f.lower().endswith('.xps')]
    if not xps_files:
        print("Aucun fichier XPS à traiter.")
    else:
        for xps_file in xps_files:
            process_xps_file(xps_file)
