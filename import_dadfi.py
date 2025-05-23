#!/usr/bin/env python3
# coding: utf-8
# ---------------------------------------------------------------------------
#  Import CSV → Maarch RM (DADFI1 à DADFI4)
#  Auteur : Hamiral Redmond
#  Date   : 23 mai 2025
# ---------------------------------------------------------------------------
import csv
import uuid
import json
import psycopg2
from datetime import datetime

# --- Connexion PostgreSQL --------------------------------------------------
DB_CONFIG = {
    "dbname":   "maarchRM",
    "user":     "maarch",
    "password": "maarch",          # ← adapte
    "host":     "192.168.71.69",   # ← adapte
    "port":     "5432",
}

# --- Définition des sources CSV -------------------------------------------
CSV_SOURCES = [
    # dadfi1
    {"fichier": "dadfi1_propre.csv",
     "key_field":   "Référence",         # → originatorArchiveId
     "title_field": "Titre/Dossier",     # → archiveName
     "metadata":   ["Titre/Dossier", "N° Pièces", "Date", "Observations"],
     "desc_class": "DADFI1"},

    # dadfi2
    {"fichier": "dadfi2_propre.csv",
     "key_field":   "Code",
     "title_field": "Titre_dossier",
     "metadata":   ["Titre_dossier", "CONTENU", "Analyse_diplomatique",
                    "DATE", "Numeros_pieces", "OBSERVATIONS",
                    "Passage_GED", "Base_GED", "Sort_final"],
     "desc_class": "DADFI2"},

    # dadfi3
    {"fichier": "dadfi3_propre.csv",
     "key_field":   "Code",
     "title_field": "TITRE/DOSSIER",
     "metadata":   ["TITRE/DOSSIER", "CONTENU", "N° PIECES", "DATE",
                    "OBSERVATIONS", "SORT FINAL", "ANA. DIPLOM.",
                    "PASSAGE GED", "BASE GED"],
     "desc_class": "DADFI3"},

    # dadfi4
    {"fichier": "dadfi4_propre.csv",
     "key_field":   "Cote",
     "title_field": "Intitulé",
     "metadata":   ["Intitulé", "Période début", "Type", "Quantité",
                    "Fonction", "Code fonction", "Mots-clés",
                    "Types de documents", "Date ouverture",
                    "Date clôture", "Système", "Durée conservation",
                    "Sort final", "Période fin", "Observations"],
     "desc_class": "DADFI4"},
]

# ---------------------------------------------------------------------------
#  FONCTION D’IMPORT
# ---------------------------------------------------------------------------
def import_csv(source, cursor) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(source["fichier"], encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            # 1. Identifiants
            archive_id = f"maarchRM_{uuid.uuid4().hex}"
            cote       = row.get(source["key_field"], "").replace("'", "''")[:250]
            titre      = row.get(source["title_field"], "").replace("'", "''")[:250]

            # 2. Plein-texte & métadonnées
            text_parts = [row.get(f, "") for f in source["metadata"] if row.get(f)]
            text_content = " ".join(text_parts).replace("'", "''")

            metadata = {f: row.get(f, "") for f in source["metadata"]}
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")

            # 3. INSERT
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "originatorArchiveId", "archiveName",
    "description", "text",
    "descriptionClass", "originatorOrgRegNumber",
    "serviceLevelReference", "retentionRuleCode", "retentionDuration",
    "finalDisposition", "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{cote}', '{titre}',
    '{metadata_str}', '{text_content}',
    '{source["desc_class"]}', 'GFC',
    'serviceLevel_002', 'GES', 'P5Y',
    'destruction', '{now}', 'preserved', 'none'
);
"""
            cursor.execute(sql)
            print(f"[{source['fichier']}] → {archive_id}")

# ---------------------------------------------------------------------------
#  MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    with psycopg2.connect(client_encoding='utf8', **DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for src in CSV_SOURCES:
                print(f"\n▶ Traitement de {src['fichier']}…")
                import_csv(src, cur)
            conn.commit()

    print("\n✅ Import terminé pour les 4 jeux DADFI.")

# ---------------------------------------------------------------------------
#  Lancement
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
#  Code signé : Hamiral Redmond – « Write code, get things done. »
# ---------------------------------------------------------------------------
