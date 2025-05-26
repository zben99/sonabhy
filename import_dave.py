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
    "password": "maarch",       # À adapter
    "host": "192.168.39.69",     # À adapter
    "port": "5432"
}

# --- Organisation productrice ---
ORG_CODE = "AUDDD"  # ← Adapter si besoin

# --- Logger pour erreurs d’import ---
logging.basicConfig(
    filename='dave_import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - Ligne %(lineno)d - %(message)s'
)

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture du CSV ---
with open("dave_propre.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for lineno, row in enumerate(reader, start=2):
        try:
            # 1) Identifiant unique
            archive_id = f"{ORG_CODE}_{uuid.uuid4().hex[:6]}"

            # 2) Extraction des champs
            cote       = row.get("cote", "").replace("'", "''")[:100]
            auteur     = row.get("auteur", "").replace("'", "''")[:250]
            titre      = row.get("titre", "").replace("'", "''")[:250]
            adresse    = row.get("adresse", "").replace("'", "''")
            descr_tech = row.get("description_tech", "").replace("'", "''")
            descr_mat  = row.get("descrip_matiere", "").replace("'", "''")

            # 3) Texte indexé
            text_content = f"{cote} {auteur} {titre} {adresse} {descr_tech} {descr_mat}".replace("'", "''")

            # 4) Métadonnées JSON
            metadata = {
                "COTE": cote,
                "AUTEUR": auteur,
                "TITRE": titre,
                "ADRESSE": adresse,
                "DESCRIPTION_TECH": descr_tech,
                "DESCRIP_MATIERE": descr_mat
            }
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")

            # 5) Règle de conservation selon support
            dt_lower = descr_tech.lower()
            if any(x in dt_lower for x in ["dvd", "cd-rom", "disquette", "cassette"]):
                retention_rule = "AUD"
                duration = "P10Y"
            else:
                retention_rule = "AUD"
                duration = "P5Y"
            disposition = "destruction"

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 6) Requête SQL complète
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "originatorArchiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId", "serviceLevelReference",
    "retentionRuleCode", "retentionDuration", "finalDisposition",
    "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{cote}', '{titre}', '{metadata_str}', '{text_content}',
    'CJ', '{ORG_CODE}', '{ORG_CODE}', 'serviceLevel_002',
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            logging.error(f"Erreur ligne CSV {lineno}: {e}", exc_info=True)

# --- Commit & fermeture ---
conn.commit()
cur.close()
conn.close()

print("✅ Importation DAVE terminée (voir dave_import_errors.log pour les erreurs).")
