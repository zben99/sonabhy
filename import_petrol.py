import csv
import uuid
from datetime import datetime
import psycopg2
import json

# --- Configuration PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",       # À adapter
    "host": "192.168.1.69",     # À adapter
    "port": "5432"
}

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
cur = conn.cursor()

# --- Lecture du fichier CSV ---
with open("petrol_propre.csv", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        # Générer un identifiant Maarch unique
        archive_id = f"maarchRM_{uuid.uuid4().hex}"

        # Utiliser la cote comme nom d'archive (max 250 car.)
        archive_name = row["cote"].replace("'", "''")[:250]

        # Construire un champ 'text' pour la recherche en texte intégral
        text_content = " ".join(filter(None, [
            row.get("titre", ""),
            row.get("auteur", ""),
            row.get("publication", ""),
            row.get("pagination", ""),
            row.get("des_mat", ""),
            row.get("des_geo", "")
        ])).replace("'", "''")

        # Préparer les métadonnées JSON
        metadata = {
            "reference":       row.get("reference", ""),
            "auteur":          row.get("auteur", ""),
            "titre":           row.get("titre", ""),
            "publication":     row.get("publication", ""),
            "pagination":      row.get("pagination", ""),
            "type_doc":        row.get("type_doc", ""),
            "des_mat":         row.get("des_mat", ""),
            "des_geo":         row.get("des_geo", "")
        }
        metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")

        # Définir une règle de conservation par défaut (à adapter si besoin)
        retention_rule = "GES"       # code profil général
        retention_duration = "P5Y"   # 5 ans
        final_disposition = "destruction"

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId",
    "archiveName",
    "description",
    "text",
    "descriptionClass",
    "originatorOrgRegNumber",
    "originatorOwnerOrgId",
    "archiverOrgRegNumber",
    "archivalProfileReference",
    "serviceLevelReference",
    "retentionRuleCode",
    "retentionDuration",
    "finalDisposition",
    "depositDate",
    "status",
    "fullTextIndexation"
) VALUES (
    '{archive_id}',
    '{archive_name}',
    '{metadata_str}',
    '{text_content}',
    'NOTICE',
    'JURRR',
    'maarchRM_stdoha-d3ic-osx14l',
    'maarchRM_stdoha-d3ic-osx14l',
    'DOSIP',
    'serviceLevel_002',
    '{retention_rule}',
    '{retention_duration}',
    '{final_disposition}',
    '{now}',
    'preserved',
    'none'
);
"""
        cur.execute(sql)

# --- Commit & fermeture ---
conn.commit()
cur.close()
conn.close()

print("Importation de petrol_propre.csv terminée avec succès.")
