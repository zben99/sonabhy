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
    "password": "maarch",  # ← À adapter
    "host": "192.168.39.69",  # ← À adapter
    "port": "5432"
}

# --- Identifiants internes Maarch RM ---
ORG_CODE = "OUV"
CLASS_CODE = "OUVRAGE"

# --- Journalisation des erreurs ---
logging.basicConfig(
    filename='ouvrag_import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - Ligne %(lineno)d - %(message)s'
)

# --- Connexion à la base ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture CSV nettoyé ---
with open("ouvrag_propre_nettoye.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for lineno, row in enumerate(reader, start=2):
        try:
            archive_id = f"{ORG_CODE}_{uuid.uuid4().hex[:8]}"
            originator_id = row.get("COTE DE RANGEMENT", "").replace("'", "''")[:100]
            archive_name = row.get("TITRE", "").replace("'", "''")[:250]

            # Métadonnées JSON
            metadata_str = json.dumps(row, ensure_ascii=False).replace("'", "''")

            # Texte complet pour indexation
            text_content = " ".join([v for v in row.values() if v]).replace("'", "''")

            # Politique de conservation par défaut
            retention_rule = "GES"
            retention_duration = "P10Y"
            final_disposition = "preservation"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Requête SQL
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "originatorArchiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId",
    "serviceLevelReference", "retentionRuleCode", "retentionDuration",
    "finalDisposition", "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{originator_id}', '{archive_name}', '{metadata_str}', '{text_content}',
    '{CLASS_CODE}', '{ORG_CODE}', '{ORG_CODE}',
    'serviceLevel_002', '{retention_rule}', '{retention_duration}',
    '{final_disposition}', '{now}', 'preserved', 'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            logging.error(f"Erreur à la ligne {lineno}: {e}", exc_info=True)

# --- Finalisation ---
conn.commit()
cur.close()
conn.close()

print("✅ Importation OUVRAG terminée (voir ouvrag_import_errors.log pour les erreurs).")
