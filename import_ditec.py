import csv
import uuid
from datetime import datetime
import psycopg2
import json

# --- Configuration PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",      # À adapter
    "host": "192.168.1.69",    # À adapter
    "port": "5432"
}

# --- Définition des fichiers DITEC ---
CSV_SOURCES = [
    {
        "fichier": "ditec1_propre.csv",
        "key_field": "cote",
        "metadata": ["ref", "titre", "analyse_diplomatique", "date", "contenu"],
        "desc_class": "DITEC1"
    },
    {
        "fichier": "ditec2_propre.csv",
        "key_field": "cote",
        "metadata": ["ref", "titre", "contenu", "analyse_diplomatique", "date"],
        "desc_class": "DITEC2"
    },
    {
        "fichier": "ditec3_propre.csv",
        "key_field": "code",
        "metadata": ["reference", "titre", "contenu", "analyse", "date"],
        "desc_class": "DITEC3"
    }
]

def import_csv(source, cursor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(source["fichier"], encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # 1) Génération d'un archiveId unique
            archive_id = f"maarchRM_{uuid.uuid4().hex}"
            # 2) Nom d’archive à partir de la cote ou code
            key_val = row.get(source["key_field"], "").replace("'", "''")[:250]
            # 3) Texte plein-texte
            text_parts = [row.get(f, "") for f in source["metadata"] if row.get(f)]
            text_content = " ".join(text_parts).replace("'", "''")
            # 4) Métadonnées JSON
            metadata = {f: row.get(f, "") for f in source["metadata"]}
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")
            # 5) Règles de conservation par défaut
            retention_rule     = "GES"
            retention_duration = "P5Y"
            final_disposition  = "destruction"
            # Requête d’insertion
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId",
    "archiverOrgRegNumber", "archivalProfileReference",
    "serviceLevelReference", "retentionRuleCode", "retentionDuration",
    "finalDisposition", "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{key_val}', '{metadata_str}', '{text_content}',
    '{source["desc_class"]}', 'JURRR', 'maarchRM_stdoha-d3ic-osx14l',
    'maarchRM_stdoha-d3ic-osx14l', 'DOSIP', 'serviceLevel_002',
    '{retention_rule}', '{retention_duration}', '{final_disposition}',
    '{now}', 'preserved', 'none'
);
"""
            cursor.execute(sql)
            print(f"[{source['fichier']}] importé → {archive_id}")

def main():
    conn = psycopg2.connect(client_encoding='utf8', **DB_CONFIG)
    cur  = conn.cursor()
    for src in CSV_SOURCES:
        print(f"▶ Traitement de {src['fichier']}…")
        import_csv(src, cur)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Import terminé pour les bases DITEC.")

if __name__ == "__main__":
    main()
