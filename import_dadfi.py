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

# --- Définition des sources CSV ---
# fichier      : nom du CSV
# key_field    : colonne à utiliser pour archiveName
# metadata     : colonnes à stocker en JSON + pour full-text
# desc_class   : descriptionClass pour ce type
CSV_SOURCES = [
    {
        "fichier": "dadfi1_propre.csv",
        "key_field": "Référence",
        "metadata": ["Titre/Dossier", "N° Pièces", "Date", "Observations"],
        "desc_class": "DADFI1"
    },
    {
        "fichier": "dadfi2_propre.csv",
        "key_field": "Code",
        "metadata": [
            "Titre_dossier", "CONTENU", "Analyse_diplomatique",
            "DATE", "Numeros_pieces", "OBSERVATIONS",
            "Passage_GED", "Base_GED", "Sort_final"
        ],
        "desc_class": "DADFI2"
    },
    {
        "fichier": "dadfi3_propre.csv",
        "key_field": "Code",
        "metadata": [
            "TITRE/DOSSIER", "CONTENU", "N° PIECES", "DATE",
            "OBSERVATIONS", "SORT FINAL", "ANA. DIPLOM.",
            "PASSAGE GED", "BASE GED"
        ],
        "desc_class": "DADFI3"
    },
    {
        "fichier": "dadfi4_propre.csv",
        "key_field": "Cote",
        "metadata": [
            "Intitulé", "Période début", "Type", "Quantité",
            "Fonction", "Code fonction", "Mots-clés",
            "Types de documents", "Date ouverture",
            "Date clôture", "Système", "Durée conservation",
            "Sort final", "Période fin", "Observations"
        ],
        "desc_class": "DADFI4"
    }
]

def import_csv(source, cursor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(source["fichier"], encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # 1) Génération d'un archiveId unique
            archive_id = f"maarchRM_{uuid.uuid4().hex}"
            
            # 2) Nom d'archive depuis la clé (Référence/Code/Cote)
            key = row.get(source["key_field"], "").replace("'", "''")[:250]
            
            # 3) Construction du texte full-text
            text_parts = [row.get(f, "") for f in source["metadata"] if row.get(f)]
            text_content = " ".join(text_parts).replace("'", "''")
            
            # 4) Préparation du JSON de métadonnées
            metadata = { f: row.get(f, "") for f in source["metadata"] }
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")
            
            # 5) Valeurs par défaut pour la conservation
            retention_rule = "GES"
            retention_duration = "P5Y"
            final_disposition = "destruction"
            
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "archiveName", "description", "text",
    "descriptionClass", "originatorOrgRegNumber", "originatorOwnerOrgId",
    "archiverOrgRegNumber", "archivalProfileReference",
    "serviceLevelReference", "retentionRuleCode", "retentionDuration",
    "finalDisposition", "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{key}', '{metadata_str}', '{text_content}',
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
    cur = conn.cursor()
    for src in CSV_SOURCES:
        print(f"\n▶ Traitement de {src['fichier']}…")
        import_csv(src, cur)
    conn.commit()
    cur.close()
    conn.close()
    print("\n✅ Import terminé pour les 4 jeux de données.")

if __name__ == "__main__":
    main()
