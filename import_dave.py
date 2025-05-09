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
    "password": "maarch",   # À adapter
    "host": "192.168.1.69", # À adapter si besoin
    "port": "5432"
}

# --- Setup du logger pour les erreurs d’import ---
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
            # Génération d’un ID unique
            archive_id = f"maarchRM_{uuid.uuid4().hex[:6]}-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:6]}"

            # Extraction et nettoyage des champs
            cote         = row.get("cote", "").replace("'", "''")[:50]
            auteur       = row.get("auteur", "").replace("'", "''")[:250]
            titre        = row.get("titre", "").replace("'", "''")[:250]
            adresse      = row.get("adresse", "").replace("'", "''")
            descr_tech   = row.get("description_tech", "").replace("'", "''")
            descr_mat    = row.get("descrip_matiere", "").replace("'", "''")

            # Texte complet pour indexation
            text_content = f"{cote} {auteur} {titre} {adresse} {descr_tech} {descr_mat}".replace("'", "''")

            # Métadonnées JSON
            metadata = {
                "COTE": cote,
                "AUTEUR": auteur,
                "TITRE": titre,
                "ADRESSE": adresse,
                "DESCRIPTION_TECH": descr_tech,
                "DESCRIP_MATIERE": descr_mat
            }
            metadata_str = json.dumps(metadata).replace("'", "''")

            # Détection du profil audiovisuel
            dt_lower = descr_tech.lower()
            if any(x in dt_lower for x in ["dvd", "cd-rom", "disquette", "cassette"]):
                retention_rule = "AUD"
                duration       = "P10Y"
                disposition    = "destruction"
            else:
                retention_rule = "AUD"
                duration       = "P5Y"
                disposition    = "destruction"

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Construction de la requête
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId", "originatorOwnerOrgRegNumber",
    "archiverOrgRegNumber", "archivalProfileReference", "serviceLevelReference",
    "retentionRuleCode", "retentionDuration", "finalDisposition",
    "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{titre}', '{metadata_str}', '{text_content}',
    'CJ',        -- Classe documentaire audiovisuelle
    'JURRR',      -- Org. d’origine
    'maarchRM_stdoha-d3ic-osx14l',  -- Org propriétaire (maarchRM)
    'JURRR',      -- Org. propriétaire registre
    'JURRR',      -- Org. archivage
    'DOSIP',      -- Profil d’archivage
    'serviceLevel_002',
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            # En cas d’erreur, on log l’exception et la ligne du CSV
            logging.error(f"Erreur ligne CSV {lineno}: {e}", exc_info=True)

# --- Commit & fermeture ---
conn.commit()
cur.close()
conn.close()

print("Importation terminée (voir dave_import_errors.log pour les erreurs).")
