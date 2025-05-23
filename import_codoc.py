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
    "host": "192.168.71.69",
    "port": "5432"
}

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture du fichier CSV ---
with open("codoc_propre.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
    for row in reader:
        archive_id = f"maarchRM_{uuid.uuid4().hex[:6]}-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:6]}"
        titre = row["TITRE"].replace("'", "''")[:250]
        code = row["CODE"].replace("'", "''")
        contenu = row["CONTENU"].replace("'", "''")
        ana = row["ANA_DIPLOM"].replace("'", "''")
        date_extreme = row["DATE"].replace("'", "''")
        text_content = f"{titre} {code} {date_extreme} {contenu} {ana}".replace("'", "''")

        metadata = {
            "CODE": row["CODE"],
            "TITRE": row["TITRE"],
            "CONTENU": row["CONTENU"],
            "ANA_DIPLOM": row["ANA_DIPLOM"],
            "DATE EXTREME": row["DATE"]
        }
        metadata_str = json.dumps(metadata).replace("'", "''")

        # Détection de la règle de conservation
        titre_lower = titre.lower()
        if "personnel" in titre_lower:
            retention_rule = "DIP"
            duration = "P90Y"
            disposition = "destruction"
        elif "manuel" in titre_lower:
            retention_rule = "V078"
            duration = "P10Y"
            disposition = "preservation"
        elif "appel d'offre" in titre_lower or "appel d’offres" in titre_lower:
            retention_rule = "V012"
            duration = "P10Y"
            disposition = "preservation"
        elif "facture" in titre_lower or "bordereau" in titre_lower:
            retention_rule = "COM"
            duration = "P10Y"
            disposition = "destruction"
        else:
            retention_rule = "GES"
            duration = "P5Y"
            disposition = "destruction"

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "originatorArchiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgRegNumber",
     "serviceLevelReference",
    "retentionRuleCode", "retentionDuration", "finalDisposition",
    "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{code}','{titre}', '{metadata_str}', '{text_content}',
    'CJ', 'JURRR',  'JURRR',
     'serviceLevel_002',
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
        cur.execute(sql)

# --- Commit & Close ---
conn.commit()
cur.close()
conn.close()

print("Importation terminée avec succès.")
