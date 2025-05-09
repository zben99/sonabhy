import csv
import uuid
import logging
from datetime import datetime
import psycopg2
from psycopg2 import sql

# --- Configuration PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",  # À adapter
    "host": "192.168.1.69",
    "port": "5432"
}

# --- Configuration du logger ---
logging.basicConfig(
    filename='import_codoc2_errors.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

def determine_retention(titre):
    t = titre.lower()
    if "personnel" in t:
        return ("DIP", "P90Y", "destruction")
    elif "manuel" in t:
        return ("V078", "P10Y", "preservation")
    elif "appel d'offre" in t or "appel d’offres" in t:
        return ("V012", "P10Y", "preservation")
    elif "facture" in t or "bordereau" in t:
        return ("COM", "P10Y", "destruction")
    else:
        return ("GES", "P5Y", "destruction")

def main():
    # Connexion PostgreSQL
    conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
    cur = conn.cursor()

    with open("codoc2_propre.csv", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for lineno, row in enumerate(reader, start=2):
            try:
                # Génération d'un archiveId unique
                archive_id = f"maarchRM_{uuid.uuid4().hex[:6]}-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:6]}"

                titre = row.get("titre_dossier", "").strip()[:250]
                code = row.get("code", "").strip()
                contenu = row.get("contenu", "").strip()
                ana = row.get("analyse_diplomatique", "").strip()
                annee = row.get("annee", "").strip()
                passage_ged = row.get("passage_ged", "").strip()
                observations = row.get("observations", "").strip()
                date_extreme = row.get("date_extreme", "").strip()
                sort_final = row.get("sort_final", "").strip()

                text_content = " ".join([titre, code, annee, contenu, ana])
                metadata = {
                    "CODE": code,
                    "TITRE_DOSSIER": titre,
                    "CONTENU": contenu,
                    "ANALYSE_DIPLOMATIQUE": ana,
                    "ANNEE": annee,
                    "PASSAGE_GED": passage_ged,
                    "OBSERVATIONS": observations,
                    "DATE_EXTREME": date_extreme,
                    "SORT_FINAL": sort_final
                }

                retention_rule, duration, disposition = determine_retention(titre)
                deposit_date = datetime.now()

                insert_query = sql.SQL("""
                    INSERT INTO "recordsManagement"."archive" (
                      "archiveId", "archiveName", "description", "text",
                      "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId",
                      "originatorOwnerOrgRegNumber", "archiverOrgRegNumber",
                      "archivalProfileReference", "serviceLevelReference",
                      "retentionRuleCode", "retentionDuration", "finalDisposition",
                      "depositDate", "status", "fullTextIndexation"
                    ) VALUES (
                      %s, %s, %s, %s,
                      %s, %s, %s,
                      %s, %s,
                      %s, %s,
                      %s, %s, %s,
                      %s, %s, %s
                    )
                """)

                cur.execute(insert_query, [
                    archive_id,
                    titre,
                    json.dumps(metadata, ensure_ascii=False),
                    text_content,
                    'CJ',                                # descriptionClass (à adapter si besoin)
                    'JURRR',                             # originatorOrgRegNumber
                    'maarchRM_stdoha-d3ic-osx14l',      # originatorOwnerOrgId
                    'JURRR',                             # originatorOwnerOrgRegNumber
                    'maarchRM_stdoha-d3ic-osx14l',      # archiverOrgRegNumber
                    'DOSIP',                             # archivalProfileReference
                    'serviceLevel_002',                  # serviceLevelReference
                    retention_rule,
                    duration,
                    disposition,
                    deposit_date,
                    'preserved',
                    'none'
                ])

                conn.commit()
                logger.info(f"Ligne {lineno} importée : archiveId={archive_id}")

            except Exception as e:
                conn.rollback()
                logger.error(f"Échec ligne {lineno} | données={row} | erreur={e}")

    cur.close()
    conn.close()
    print("Importation terminée (voir import_codoc2_errors.log pour les erreurs).")

if __name__ == "__main__":
    import json
    main()
