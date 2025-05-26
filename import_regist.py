#!/usr/bin/env python3
# coding: utf-8
# ---------------------------------------------------------------------------
#  Import CSV → Maarch RM (REGIST)
#  Auteur : Hamiral Redmond
#  Date   : 23 mai 2025
# ---------------------------------------------------------------------------

import csv
import uuid
from datetime import datetime
import psycopg2
import json

# --- Configuration PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",  # À adapter
    "host": "192.168.39.69",
    "port": "5432"
}

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture du fichier CSV ---
with open("regist_propre.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        # Génération d'un archiveId unique
        archive_id = f"maarchRM_{uuid.uuid4().hex[:6]}-{uuid.uuid4().hex[:4]}"

        # Nom de l’archive = titre
        archive_name = row.get("titre", "").replace("'", "''")[:250]
        # Identifiant = cote → placé dans originatorArchiveId
        originator_id = row.get("cote", "").replace("'", "''")[:100]

        # Texte indexé
        text_content = " ".join([
            row.get("titre", ""),
            row.get("contenu", ""),
            row.get("dates_extremes", ""),
            row.get("analyse", "")
        ]).replace("'", "''")

        # Métadonnées JSON
        metadata = {
            "date_versement": row.get("date_versement", ""),
            "date_enregistrement": row.get("date_enregistrement", ""),
            "provenance": row.get("provenance", ""),
            "titre": row.get("titre", ""),
            "contenu": row.get("contenu", ""),
            "analyse": row.get("analyse", ""),
            "dates_extremes": row.get("dates_extremes", ""),
            "annee": row.get("annee", ""),
            "duree_utilite": row.get("duree_utilite", ""),
            "sort_final": row.get("sort_final", ""),
            "initial": row.get("initial", ""),
            "niveau_description": row.get("niveau_description", ""),
            "importance_materielle": row.get("importance_materielle", ""),
            "ged": row.get("ged", "")
        }
        metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")

        # Logique de conservation
        titre_lower = row.get("titre", "").lower()
        if "registre" in titre_lower:
            retention_rule = "REG"
            duration = "P10Y"
            disposition = "preservation"
        else:
            retention_rule = "GES"
            duration = "P5Y"
            disposition = "destruction"

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Requête avec originatorArchiveId = cote
        sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "archiveName", "originatorArchiveId", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "serviceLevelReference",
    "retentionRuleCode", "retentionDuration", "finalDisposition",
    "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{archive_name}', '{originator_id}', '{metadata_str}', '{text_content}',
    'REGISTRE', 'AG', 'serviceLevel_002',
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
        cur.execute(sql)

# --- Commit & Close ---
conn.commit()
cur.close()
conn.close()

print("✅ Importation de regist_propre.csv terminée avec succès.")

# ---------------------------------------------------------------------------
#  Code signé : Hamiral Redmond – « Write code, get things done. »
# ---------------------------------------------------------------------------
