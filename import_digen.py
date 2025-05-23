#!/usr/bin/env python3
# coding: utf-8
# ---------------------------------------------------------------------------
#  Import CSV → Maarch RM (DIGEN1 à DIGEN4)
#  Auteur : Hamiral Redmond
#  Date   : 23 mai 2025
# ---------------------------------------------------------------------------

import csv, uuid, json, psycopg2
from datetime import datetime

# --- Connexion PostgreSQL --------------------------------------------------
DB_CONFIG = {
    "dbname":   "maarchRM",
    "user":     "maarch",
    "password": "maarch",           # <-- adapte ici
    "host":     "192.168.71.69",    # <-- idem
    "port":     "5432",
}
# --- Définition des sources CSV -------------------------------------------
CSV_SOURCES = [
    {"fichier": "digen1_propre.csv", "key_field": "ref",
     "metadata": ["date", "cote", "titre", "contenu", "observations"],
     "desc_class": "DIGEN1"},
    {"fichier": "digen2_propre.csv", "key_field": "ref",
     "metadata": ["cote", "titre", "contenu", "analyse_diplomatique", "date"],
     "desc_class": "DIGEN2"},
    {"fichier": "digen3_propre.csv", "key_field": "ref",
     "metadata": ["cote", "titre", "contenu", "analyse_diplomatique", "date"],
     "desc_class": "DIGEN3"},
    {"fichier": "digen4_propre.csv", "key_field": "cote_rangement",
     "metadata": ["titre", "dates_extremes", "niveau_description",
                  "importance_materielle", "provenance_ou_processus",
                  "initial_processus", "mots_cles_matieres",
                  "analyse_diplomatique", "date_versement",
                  "date_enregistrement", "passage_ged",
                  "duree_utilite_admin", "sort_final", "notes"],
     "desc_class": "DIGEN4"},
]

def import_csv(source, cursor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(source["fichier"], encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # 1) Clé métier (cote | ref | cote_rangement) → identifiant producteur
            identifiant = row.get(source["key_field"], "")[:250].replace("'", "''")
            # 2) Titre → nom d'archive (champ affiché dans l'IHM)
            titre = row.get("titre", "")[:250].replace("'", "''")
            # 3) Contenu plein-texte
            text_parts = [row.get(f, "") for f in source["metadata"] if row.get(f)]
            text_content = " ".join(text_parts).replace("'", "''")
            # 4) Métadonnées JSON
            metadata = {f: row.get(f, "") for f in source["metadata"]}
            metadata_str = json.dumps(metadata, ensure_ascii=False).replace("'", "''")
            # 5) Clé technique unique
            archive_id = f"maarchRM_{uuid.uuid4().hex}"
            # 6) Insertion
            sql = f"""
INSERT INTO "recordsManagement"."archive" (
    "archiveId", "originatorArchiveId", "archiveName",
    "description", "text",
    "descriptionClass", "originatorOrgRegNumber",
    "serviceLevelReference", "retentionRuleCode", "retentionDuration",
    "finalDisposition", "depositDate", "status", "fullTextIndexation"
) VALUES (
    '{archive_id}', '{identifiant}', '{titre}',
    '{metadata_str}', '{text_content}',
    '{source["desc_class"]}', 'AG',
    'serviceLevel_002', 'GES', 'P5Y',
    'destruction', '{now}', 'preserved', 'none'
);
"""
            cursor.execute(sql)
            print(f"[{source['fichier']}] → {archive_id}")

def main():
    with psycopg2.connect(client_encoding='utf8', **DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for src in CSV_SOURCES:
                print(f"\n▶ Traitement de {src['fichier']}…")
                import_csv(src, cur)
            conn.commit()
    print("\n✅ Import terminé pour les 4 bases DIGEN.")

if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
#  Code signé : Hamiral Redmond – « Write code, get things done. »
# ---------------------------------------------------------------------------
