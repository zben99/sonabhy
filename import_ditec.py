#!/usr/bin/env python3
# coding: utf-8
# ---------------------------------------------------------------------------
#  Import CSV → Maarch RM (DITEC1 à DITEC4)
#  Auteur : Hamiral Redmond
#  Date   : 23 mai 2025
# --------------------------

import csv
import uuid
from datetime import datetime
import psycopg2
import json

# --- Configuration PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",      # À adapter
    "host": "192.168.39.69",   # À adapter
    "port": "5432"
}

# --- Définition des fichiers DITEC ---
CSV_SOURCES = [
    {
        "fichier": "ditec1_propre.csv",
        "key_field": "cote",
        "title_field": "titre",
        "metadata": ["ref", "titre", "analyse_diplomatique", "date", "contenu"],
        "desc_class": "DITEC1"
    },
    {
        "fichier": "ditec2_propre.csv",
        "key_field": "cote",
        "title_field": "titre",
        "metadata": ["ref", "titre", "contenu", "analyse_diplomatique", "date"],
        "desc_class": "DITEC2"
    },
    {
        "fichier": "ditec3_propre.csv",
        "key_field": "Code",
        "title_field": "TITRE/DOSSIER",
        "metadata": ["Code", "TITRE/DOSSIER", "CONTENU", "ANA. DIPLOM.", "DATE EXTREME", "ANNEE", "PASSAGE GED", "SORT FINAL"],
        "desc_class": "DITEC3"
    },
    {
        "fichier": "ditec4_propre.csv",
        "key_field": "Code",
        "title_field": "TITRE/DOSSIER",
        "metadata": ["Code", "TITRE/DOSSIER", "CONTENU", "ANA. DIPLOM.", "DATE EXTREME", "ANNEE", "OBSERVATIONS", "PASSAGE GED", "SORT FINAL"],
        "desc_class": "DITEC4"
    }
]

def import_csv(source, cursor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(source["fichier"], encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            archive_id = f"maarchRM_{uuid.uuid4().hex}"
            identifiant = row.get(source["key_field"], "")[:250].replace("'", "''")
            # Nom de l’archive basé sur le titre (limité à 250 caractères)
            archive_name = row.get(source["title_field"], "").replace("'", "''")[:250]

            # Texte complet indexé
            text_parts = [row.get(f, "") for f in source["metadata"] if row.get(f)]
            text_content = " ".join(text_parts).replace("'", "''")

            # Métadonnées JSON
            metadata = {f: row.get(f, "") for f in source["metadata"]}
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")

            # Règles par défaut
            retention_rule = "GES"
            retention_duration = "P5Y"
            final_disposition = "destruction"

            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId",  "originatorArchiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", 
    "serviceLevelReference", "retentionRuleCode", "retentionDuration",
    "finalDisposition", "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{identifiant}', '{archive_name}', '{metadata_str}', '{text_content}',
    '{source["desc_class"]}', 'SPP', 'serviceLevel_002',
    '{retention_rule}', '{retention_duration}', '{final_disposition}',
    '{now}', 'preserved', 'none'
);
"""
            cursor.execute(sql)
            print(f"[{source['fichier']}] Archive insérée → {archive_id}")

def main():
    conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
    cur = conn.cursor()
    for src in CSV_SOURCES:
        print(f"▶ Traitement de {src['fichier']}…")
        import_csv(src, cur)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Importation terminée pour toutes les bases DITEC.")

if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
#  Code signé : Hamiral Redmond – « Write code, get things done. »
# ---------------------------------------------------------------------------
