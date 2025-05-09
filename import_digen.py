import csv
import uuid
from datetime import datetime
import psycopg2
import json

# --- Configuration de la connexion PostgreSQL ---
DB_CONFIG = {
    "dbname": "maarchRM",
    "user": "maarch",
    "password": "maarch",       # À adapter
    "host": "192.168.1.69",     # À adapter
    "port": "5432"
}

# --- Liste des CSV à traiter ---
CSV_SOURCES = [
    {
        "fichier": "digen1_propre.csv",
        "key_field": "ref",
        "metadata": ["date", "cote", "titre", "contenu", "observations"],
        "desc_class": "DIGEN1"
    },
    {
        "fichier": "digen2_propre.csv",
        "key_field": "ref",
        "metadata": ["cote", "titre", "contenu", "analyse_diplomatique", "date"],
        "desc_class": "DIGEN2"
    },
    {
        "fichier": "digen3_propre.csv",
        "key_field": "ref",
        "metadata": ["cote", "titre", "contenu", "analyse_diplomatique", "date"],
        "desc_class": "DIGEN3"
    },
    {
        "fichier": "digen4_propre.csv",
        "key_field": "cote_rangement",
        "metadata": [
            "titre",
            "dates_extremes",
            "niveau_description",
            "importance_materielle",
            "provenance_ou_processus",
            "initial_processus",
            "mots_cles_matieres",
            "analyse_diplomatique",
            "date_versement",
            "date_enregistrement",
            "passage_ged",
            "duree_utilite_admin",
            "sort_final",
            "notes"
        ],
        "desc_class": "DIGEN4"
    }
]

def import_csv(source, cursor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(source["fichier"], encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # 1) ID unique
            archive_id = f"maarchRM_{uuid.uuid4().hex}"
            # 2) Nom d’archive depuis la clé
            key_val = row.get(source["key_field"], "").replace("'", "''")[:250]
            # 3) Texte plein-texte
            text_parts = [row.get(f, "") for f in source["metadata"] if row.get(f)]
            text_content = " ".join(text_parts).replace("'", "''")
            # 4) Métadonnées JSON
            metadata = {f: row.get(f, "") for f in source["metadata"]}
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")
            # 5) Conservation par défaut
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
        print(f"\n▶ Traitement de {src['fichier']}…")
        import_csv(src, cur)
    conn.commit()
    cur.close()
    conn.close()
    print("\n✅ Import terminé pour les 4 bases Digen.")

if __name__ == "__main__":
    main()
