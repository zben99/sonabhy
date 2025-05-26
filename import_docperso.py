#!/usr/bin/env python3
# coding: utf-8
# ---------------------------------------------------------------------------
#  Import CSV → Maarch RM (docupers)
#  Auteur : Hamiral Redmond
#  Date   : 26 mai 2025
# --------------------------

import csv
import uuid
import json
import logging
from datetime import datetime
import psycopg2

# --- Configuration PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",  # À adapter
    "host": "192.168.39.69",  # À adapter
    "port": "5432"
}

# --- Code organisation archiviste ---
ORG_CODE = "maarchRM_stdoha-d3ic-osx14l"

# --- Logger pour erreurs d'import ---
logging.basicConfig(
    filename='docupers_import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - Ligne %(lineno)d - %(message)s'
)

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture du fichier CSV ---
with open("docperso_propre.csv", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=',', quotechar='"')
    for lineno, row in enumerate(reader, start=2):
        try:
            # 1. Génération de l'ID unique
            archive_id = f"{ORG_CODE}_{uuid.uuid4().hex[:6]}"

            # 2. Identifiant = Code / Nom de l'archive = TITRE/DOSSIER
            originator_id = row.get("Code", "").replace("'", "''")[:100]
            archive_name = row.get("TITRE/DOSSIER", "").replace("'", "''")[:250]

            # 3. Description = JSON complet des champs
            metadata_str = json.dumps(row, ensure_ascii=False).replace("'", "''")

            # 4. Texte plein pour indexation
            text_content = " ".join([v for v in row.values() if v]).replace("'", "''")

            # 5. Règles de conservation (simples par défaut ici)
            retention_rule = "PERS"  # Personnels
            duration = "P30Y"
            disposition = "preservation"

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 6. Requête SQL
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "originatorArchiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "serviceLevelReference",
    "retentionRuleCode", "retentionDuration", "finalDisposition",
    "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{originator_id}', '{archive_name}', '{metadata_str}', '{text_content}',
    'PERS', 'GRH', 'serviceLevel_002',
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            logging.error(f"Erreur à la ligne {lineno}: {e}", exc_info=True)

# --- Finalisation ---
conn.commit()
cur.close()
conn.close()

print("✅ Import DOCPERS terminé (voir docupers_import_errors.log pour les erreurs).")

# ---------------------------------------------------------------------------
#  Code signé : Hamiral Redmond – « Write code, get things done. »
# ---------------------------------------------------------------------------
