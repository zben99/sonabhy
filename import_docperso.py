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
    "password": "maarch",      # ← À adapter
    "host": "192.168.1.69",    # ← À adapter si besoin
    "port": "5432"
}

# --- Code orgRegNumber (organisation archiviste) ---
ORG_CODE = "maarchRM_stdoha-d3ic-osx14l"

# --- Logger pour les lignes CSV problématiques ---
logging.basicConfig(
    filename='docperso_import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - Ligne %(lineno)d - %(message)s'
)

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

with open("docperso_propre.csv", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=',', quotechar='"')
    for lineno, row in enumerate(reader, start=2):
        try:
            # 1) ID d’archive unique
            archive_id = f"{ORG_CODE}_{uuid.uuid4().hex[:6]}"

            # 2) Titre de l’archive (champ à ajuster selon votre CSV)
            # On prend par exemple un champ 'titre' ou on compose avec 'nom'+'prenom'
            if row.get("titre"):
                archive_name = row["titre"]
            else:
                archive_name = f"{row.get('nom','')} {row.get('prenom','')}".strip() or "DocPerso"
            archive_name = archive_name.replace("'", "''")[:250]

            # 3) Métadonnées JSON : on embarque tous les champs
            metadata_str = json.dumps(row, ensure_ascii=False).replace("'", "''")

            # 4) Texte pour indexation plein-texte
            text_content = " ".join([v for v in row.values() if v]).replace("'", "''")

            # 5) Détermination automatique d’une règle de conservation
            td = row.get("type_doc", "").lower()
            if "naissance" in td or "identité" in td:
                retention_rule = "DIP"    # dossiers personnels
                duration       = "P90Y"
                disposition    = "destruction"
            else:
                retention_rule = "GES"
                duration       = "P5Y"
                disposition    = "destruction"

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 6) Construction de la requête SQL
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId", "originatorOwnerOrgRegNumber",
    "archiverOrgRegNumber", "archivalProfileReference", "serviceLevelReference",
    "retentionRuleCode", "retentionDuration", "finalDisposition",
    "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{archive_name}', '{metadata_str}', '{text_content}',
    'PERS',             -- classe documentaire « Documents personnels »
   'JURRR',      -- Org. d’origine
    'maarchRM_stdoha-d3ic-osx14l',  -- Org propriétaire (maarchRM)
    'JURRR',      -- Org. propriétaire registre
    'JURRR',      -- Org. archivage
    'DOSIP',            -- profil d’archivage
    'serviceLevel_002', -- niveau de service
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            logging.error(f"Erreur à la ligne {lineno}: {e}", exc_info=True)

# --- Commit et fermeture ---
conn.commit()
cur.close()
conn.close()

print("Import DOCPERSO terminé (voir docperso_import_errors.log pour les erreurs).")
