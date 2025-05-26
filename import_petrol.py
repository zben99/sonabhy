#!/usr/bin/env python3
# coding: utf-8
# ---------------------------------------------------------------------------
#  Import CSV → Maarch RM (PETROL)
#  Auteur : Hamiral Redmond
#  Date   : 23 mai 2025
# ---------------------------------------------------------------------------


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
    "password": "maarch",       # ← À adapter
    "host": "192.168.39.69",    # ← À adapter
    "port": "5432"
}

# --- Organisation d'origine et classe documentaire ---
ORG_CODE = "OUV"
DESC_CLASS = "NOTICE"

# --- Logger pour erreurs ---
logging.basicConfig(
    filename='petrol_import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - Ligne %(lineno)d - %(message)s'
)

# --- Connexion à la base ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

with open("petrol_propre.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for lineno, row in enumerate(reader, start=2):
        try:
            # 1) Identifiant unique
            archive_id = f"{ORG_CODE}_{uuid.uuid4().hex[:8]}"

            # 2) Cote comme identifiant / titre comme nom d'archive
            originator_id = row.get("cote", "").replace("'", "''")[:100]
            archive_name = row.get("titre", "").replace("'", "''")[:250]

            # 3) Texte indexable (plein texte)
            text_content = " ".join(filter(None, [
                row.get("titre", ""),
                row.get("auteur", ""),
                row.get("publication", ""),
                row.get("pagination", ""),
                row.get("type_doc", ""),
                row.get("des_mat", ""),
                row.get("des_geo", "")
            ])).replace("'", "''")

            # 4) Métadonnées complètes en JSON
            metadata = {
                "reference": row.get("reference", ""),
                "cote": originator_id,
                "titre": archive_name,
                "auteur": row.get("auteur", ""),
                "publication": row.get("publication", ""),
                "pagination": row.get("pagination", ""),
                "type_doc": row.get("type_doc", ""),
                "des_mat": row.get("des_mat", ""),
                "des_geo": row.get("des_geo", "")
            }
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")

            # 5) Logique de conservation selon type_doc
            td = row.get("type_doc", "").lower()
            if "manuel" in td or "rapport" in td:
                retention_rule = "DOC"  # Documentations techniques
                duration = "P10Y"
                disposition = "preservation"
            else:
                retention_rule = "GES"
                duration = "P5Y"
                disposition = "destruction"

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 6) Requête SQL
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId",
    "originatorArchiveId",
    "archiveName",
    "description",
    "text",
    "descriptionClass",
    "originatorOrgRegNumber",
    "serviceLevelReference",
    "retentionRuleCode",
    "retentionDuration",
    "finalDisposition",
    "depositDate",
    "status",
    "fullTextIndexation"
) VALUES (
    '{archive_id}',
    '{originator_id}',
    '{archive_name}',
    '{metadata_str}',
    '{text_content}',
    '{DESC_CLASS}',
    '{ORG_CODE}',
    'serviceLevel_002',
    '{retention_rule}',
    '{duration}',
    '{disposition}',
    '{now}',
    'preserved',
    'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            logging.error(f"Erreur à la ligne {lineno}: {e}", exc_info=True)

# --- Finalisation ---
conn.commit()
cur.close()
conn.close()

print("✅ Importation de petrol_propre.csv terminée avec succès (voir petrol_import_errors.log si erreurs).")


# ---------------------------------------------------------------------------
#  Code signé : Hamiral Redmond – « Write code, get things done. »
# ---------------------------------------------------------------------------
