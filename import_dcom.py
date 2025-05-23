#!/usr/bin/env python3
# coding: utf-8
# ----------------------------------------------------------------------------
#  Import CSV → Maarch RM (DCOM1 à DCOM4)
#  Auteur : Hamiral Redmond
#  Date   : 23 mai 2025
# ----------------------------------------------------------------------------
import csv
import uuid
import json
import psycopg2
from datetime import datetime

# --- Connexion PostgreSQL --------------------------------------------------
DB_CONFIG = {
    "dbname":   "maarchRM",
    "user":     "maarch",
    "password": "maarch",         # ← adapte si besoin
    "host":     "192.168.71.69",  # ← adapte si besoin
    "port":     "5432",
}

# --- Sources CSV -----------------------------------------------------------
CSV_SOURCES = [
    {
        "fichier":    "dcom1_propre.csv",
        "key_field":  "ref",     # → originatorArchiveId
        "title_field":"titre",   # → archiveName
        "metadata":   ["titre", "contenu", "date"],
        "desc_class": "DCOM1"
    },
    {
        "fichier":    "dcom2_propre.csv",
        "key_field":  "ref",
        "title_field":"titre",
        "metadata":   ["titre", "contenu", "analyse_diplomatique", "date"],
        "desc_class": "DCOM2"
    },
    {
        "fichier":    "dcom3_propre.csv",
        "key_field":  "ref",
        "title_field":"titre",
        "metadata":   ["titre", "contenu", "analyse_diplomatique", "annee"],
        "desc_class": "DCOM3"
    },
    {
        "fichier":    "dcom4_propre.csv",
        "key_field":  "cote_rangement",
        "title_field":"titre",
        "metadata":   [
            "titre", "dates_extremes", "niveau_description",
            "importance_materielle", "provenance_ou_processus",
            "initial_processus", "mots_cles_matieres",
            "analyse_diplomatique", "date_versement",
            "date_enregistrement", "passage_ged",
            "duree_utilite_admin", "sort_final", "notes"
        ],
        "desc_class": "DCOM4"
    },
]

def import_csv(source, cursor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(source["fichier"], encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # 1. Identifiants
            archive_id = f"maarchRM_{uuid.uuid4().hex}"
            cote       = row.get(source["key_field"], "").replace("'", "''")[:250]
            titre      = row.get(source["title_field"], "").replace("'", "''")[:250]

            # 2. Construction du plein-texte
            text_parts   = [row.get(f, "") for f in source["metadata"] if row.get(f)]
            text_content = " ".join(text_parts).replace("'", "''")

            # 3. Métadonnées JSON
            metadata     = {f: row.get(f, "") for f in source["metadata"]}
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")

            # 4. INSERT
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
    '{source["desc_class"]}', 'APP',
    'serviceLevel_002', 'GES', 'P5Y',
    'destruction', '{now}', 'preserved', 'none'
);
"""
            cursor.execute(sql)
            print(f"[{source['fichier']}] → {archive_id}")

def main():
    with psycopg2.connect(client_encoding='utf8', **DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for src in CSV_SOURCES:
                print(f"\n▶ Traitement de {src['fichier']}…")
                import_csv(src, cur)
        conn.commit()
    print("\n✅ Import terminé pour les 4 jeux DCOM.")

if __name__ == "__main__":
    main()

# ----------------------------------------------------------------------------
#  Code signé : Hamiral Redmond – « Write code, get things done. »
# ----------------------------------------------------------------------------
