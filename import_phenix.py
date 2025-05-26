import csv
import uuid
import json
import logging
from datetime import datetime
import psycopg2

# --- Connexion PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",
    "host": "192.168.39.69",
    "port": "5432"
}

# --- Paramètres internes Maarch ---
ORG_CODE = "CIE"
CLASS_CODE = "CORRESPONDANCE"

# --- Journalisation des erreurs ---
logging.basicConfig(
    filename='phenix_import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - Ligne %(lineno)d - %(message)s'
)

conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture du fichier ---
with open("phenix_propre.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for lineno, row in enumerate(reader, start=2):
        try:
            archive_id = f"{ORG_CODE}_{uuid.uuid4().hex[:8]}"
            originator_id = row.get("COTE DE RANGEMENT", "").replace("'", "''")[:100]
            archive_name = row.get("TITRE", "").replace("'", "''")[:250]

            # Texte indexable
            text_content = " ".join([v for v in row.values() if v]).replace("'", "''")

            # Description (métadonnées)
            metadata_str = json.dumps(row, ensure_ascii=False).replace("'", "''")

            # Profil de conservation (simple ici)
            retention_rule = "GES"
            duration = "P5Y"
            disposition = "destruction"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "originatorArchiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId",
    "serviceLevelReference", "retentionRuleCode", "retentionDuration",
    "finalDisposition", "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{originator_id}', '{archive_name}', '{metadata_str}', '{text_content}',
    '{CLASS_CODE}', '{ORG_CODE}', '{ORG_CODE}',
    'serviceLevel_002', '{retention_rule}', '{duration}',
    '{disposition}', '{now}', 'preserved', 'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            logging.error(f"Erreur ligne {lineno}: {e}", exc_info=True)

conn.commit()
cur.close()
conn.close()

print("✅ Importation Phenix terminée (voir phenix_import_errors.log si besoin).")
