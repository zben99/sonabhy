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
    "host": "192.168.1.69",     # ← À adapter si besoin
    "port": "5432"
}

# --- Code orgRegNumber pour OUREF (à remplacer par votre valeur) ---
ORG_CODE = "maarchRM_ouref-xxxxxx"

# --- Setup du logger pour les erreurs d’import ---
logging.basicConfig(
    filename='ouref_import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - Ligne %(lineno)d - %(message)s'
)

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture du CSV ---
with open("ouref_propre.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for lineno, row in enumerate(reader, start=2):
        try:
            # 1) Génération d’un ID archive unique
            archive_id = f"{ORG_CODE}_{uuid.uuid4().hex[:6]}"

            # 2) Extraction & nettoyage des champs
            reference    = row.get("reference", "").replace("'", "''")
            cote         = row.get("cote", "").replace("'", "''")
            auteur       = row.get("auteur", "").replace("'", "''")
            titre        = row.get("titre", "").replace("'", "''")[:250]
            publication  = row.get("publication", "").replace("'", "''")
            pagination   = row.get("pagination", "").replace("'", "''")
            descripteurs = row.get("descripteurs", "").replace("'", "''")

            # 3) Texte intégral pour indexation plein-texte
            text_content = " ".join([reference, cote, auteur, titre, publication, pagination, descripteurs])

            # 4) Métadonnées JSON
            metadata = {
                "reference":    reference,
                "cote":         cote,
                "auteur":       auteur,
                "titre":        titre,
                "publication":  publication,
                "pagination":   pagination,
                "descripteurs": descripteurs
            }
            metadata_str = json.dumps(metadata).replace("'", "''")

            # 5) Règle de conservation par défaut (à affiner métier)
            retention_rule = "GES"
            duration       = "P5Y"
            disposition    = "destruction"

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 6) Construction & exécution de la requête
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
    'DOSIP',              -- profil d’archivage (exemple)
    'serviceLevel_002',   -- niveau de service
    '{retention_rule}', '{duration}', '{disposition}',
    '{now}', 'preserved', 'none'
);
"""
            cur.execute(sql)

        except Exception as e:
            logging.error(f"Erreur à la ligne {lineno}: {e}", exc_info=True)

# --- Commit & fermeture ---
conn.commit()
cur.close()
conn.close()

print("Import OUREF terminé (voir ouref_import_errors.log pour les erreurs).")
