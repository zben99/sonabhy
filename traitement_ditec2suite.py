import pandas as pd

# Charger le fichier CSV
file_path = 'ditec2_nettoye_final.csv'  # Remplace par le chemin de ton fichier
df = pd.read_csv(file_path)

# Nettoyage : convertir les NaN en chaînes vides pour unifier la vérification
df = df.fillna('')

# Supprimer les lignes :
# 1. Où toutes les cellules sont vides
# 2. Où seulement une cellule est non vide
df = df[~(df == '').all(axis=1)]  # toutes vides
df = df[df.apply(lambda row: (row != '').sum() > 1, axis=1)]  # une seule valeur non vide

# Sauvegarde
output_path = 'ditec2_epure.csv'
df.to_csv(output_path, index=False, encoding='utf-8')

print(f"✅ Lignes vides ou quasi-vides supprimées. Fichier enregistré : {output_path}")
