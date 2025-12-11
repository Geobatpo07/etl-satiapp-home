# ETL SATIAP Home

Pipeline ETL automatisé pour extraire, transformer et charger des données de feedback depuis un fichier CSV vers une base Access, puis uploader vers SharePoint.

## Architecture

```
etl-satiap-home/
├── pyproject.toml          # Configuration du projet et dépendances
├── README.md               # Documentation
├── config/                 # Configuration
│   ├── __init__.py
│   └── settings.py         # Paramètres (chemins, SharePoint, etc.)
├── etl/                    # Modules ETL
│   ├── __init__.py
│   ├── extractor.py        # Chargement CSV
│   ├── transformer.py      # Transformations VBA + Power Query
│   ├── loader.py           # Écriture vers Excel
│   └── uploader.py         # Upload vers SharePoint
├── data/                   # Données
│   ├── datafeadback.csv    # Fichier source (à fournir)
│   └── output/             # Dossier de sortie
│       └── output.xlsx     # Fichier Excel généré
└── etl.py                  # Point d'entrée principal
```

## Installation

### Prérequis

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) installé
- Microsoft Access ODBC Driver (inclus avec Windows)
- Token d'accès Microsoft Graph API pour SharePoint

### Installation des dépendances

```powershell
# Cloner ou naviguer vers le projet
cd "c:\Users\gblaguerre\Documents\Data Analytics Projects\etl-satiap-home"

# Installer les dépendances avec uv
uv sync
```

## Configuration

Éditez `config/settings.py` et configurez les paramètres suivants :

```python
# Chemins des fichiers
CSV_PATH = "data/datafeedback.csv"
EXCEL_OUTPUT = "data/output/output.xlsx"

# Configuration SharePoint
SHAREPOINT_SITE = "https://votreentreprise.sharepoint.com/sites/votre-site"
SITE_NAME = "votre-site"
DOCUMENT_LIBRARY = "Documents partages"

# Authentification Graph API
ACCESS_TOKEN = "votre-token-graph-api"
```

### Obtenir un token Graph API

1. Enregistrez une application dans Azure AD
2. Accordez les permissions `Sites.ReadWrite.All`
3. Générez un token d'accès
4. Copiez le token dans `settings.py`

## Utilisation

### Exécution complète du pipeline

```powershell
uv run etl.py
```

Cette commande exécute le pipeline complet :
1. ✅ Charge le fichier CSV
2. ✅ Applique toutes les transformations (VBA + Power Query)
3. ✅ Crée le fichier Excel avec formatage
4. ✅ Upload vers SharePoint

### Exécution par étapes (debug)

```python
# Dans un shell Python
uv run python

>>> from etl.extractor import load_csv
>>> from etl.transformer import transform
>>> from etl.loader import write_to_excel
>>> from etl.uploader import upload_to_sharepoint

>>> df = load_csv()
>>> df_transformed = transform(df)
>>> write_to_excel(df_transformed)
>>> upload_to_sharepoint()
```

## Transformations appliquées

### Transformations VBA
- Correction d'encodage (Ã´→ô, Ã©→é, etc.)
- Suppression de la colonne AC
- Déplacement des blocs V:AA et AJ:AR vers la fin
- Suppression des colonnes vides
- Insertion et déplacement de colonnes
- Renommage de colonnes (AH, AO, AT)

### Transformations Power Query
- Promotion des en-têtes
- Combinaison de colonnes multiples (hôpital, satisfaction, etc.)
- Filtrage des Respondent ID
- Suppression de colonnes inutiles (Email, Names, etc.)
- Mapping des ratings vers étoiles (1-5)
- Standardisation des textes

## Dépendances

- **pandas** : Manipulation de données
- **openpyxl** : Création et formatage de fichiers Excel
- **requests** : API SharePoint
- **python-dotenv** : Gestion de configuration

## Troubleshooting

### Erreur : "Module not found: openpyxl"
- Exécutez `uv sync` pour installer toutes les dépendances
- Ou installez manuellement : `uv pip install openpyxl`

### Erreur : "Authentication failed"
- Vérifiez que votre `ACCESS_TOKEN` est valide
- Les tokens expirent généralement après 1 heure

### Erreur : "Column not found"
- Vérifiez que votre CSV contient les colonnes attendues
- Ajustez les noms de colonnes dans `transformer.py` si nécessaire

## Logs

Le pipeline affiche des logs détaillés dans la console :
- ✅ Succès des étapes
- ⚠️ Avertissements
- ❌ Erreurs avec détails

## License

Projet interne SATIAP Home

## Auteur

Créé par Geovany LAGUERRE | Data Science & Analytics Engineer