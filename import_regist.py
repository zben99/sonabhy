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
    "host": "192.168.1.69",
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

        # Utiliser la cote comme nom d'archive
        archive_name = row["cote"].replace("'", "''")[:250]

        # Construction du texte complet pour indexation
        text_content = " ".join([
            row.get("titre", ""),
            row.get("contenu", ""),
            row.get("dates_extremes", ""),
            row.get("analyse", "")
        ]).replace("'", "''")

        # Préparation des métadonnées JSON
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
        metadata_str = json.dumps(metadata).replace("'", "''")

        # Exemple de détection de règle de conservation selon le titre
        titre_lower = row.get("titre", "").lower()
        if "registre" in titre_lower:
            retention_rule = "REG"       # code fictif, à adapter
            duration = "P10Y"            # 10 ans
            disposition = "preservation"
        else:
            retention_rule = "GES"
            duration = "P5Y"             # 5 ans
            disposition = "destruction"

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId",
    "archiverOrgRegNumber", "archivalProfileReference", "serviceLevelReference",
    "retentionRuleCode", "retentionDuration", "finalDisposition",
    "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{archive_name}', '{metadata_str}', '{text_content}',
    'REGISTRE', 'JURRR', 'maarchRM_stdoha-d3ic-osx14l',
    'maarchRM_stdoha-d3ic-osx14l', 'DOSIP', 'serviceLevel_002',
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
        cur.execute(sql)

# --- Commit & Close ---
conn.commit()
cur.close()
conn.close()

print("Importation de regist_propre.csv terminée avec succès.")
