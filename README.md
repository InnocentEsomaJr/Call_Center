# Dashboard Call Center (Streamlit + PostgreSQL)

Tableau de bord interactif pour le suivi des appels et alertes.

Le projet supporte maintenant 2 modes:
- `PostgreSQL` (recommande): les donnees sont lues depuis la base SQL.
- `Upload Excel direct`: lecture temporaire des fichiers uploades dans la session.

## 1) Fonctions principales

- KPI dynamiques: Province, Tot appels, Resolu, Non resolu, Hommes, Femmes, ND
- Filtres globaux: periode, province, genre, incident/pathologie, categorie d'appel
- Pages:
  - `Informations generales`
  - `Autres details d'informations`
  - `Details alertes`
- Graphiques interactifs avec valeurs affichees
- Basculement automatique des graphiques province -> territoires si une seule province est selectionnee
- Carte geolocalisee des appels par province (RDC)
- Analyses avancees:
  - completude des champs
  - performance de resolution
  - comparaison avec periode precedente
  - profil des appels

## 2) Installation

```bash
cd "c:\Users\EMI_Jr\OneDrive\Documents\Python\Call_Center"
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 3) Lancement

```bash
streamlit run app.py
```

## 4) Mode PostgreSQL

Dans la barre laterale:
1. Choisir `Source des donnees -> PostgreSQL`
2. Renseigner:
   - Host
   - Port
   - Base
   - Utilisateur
   - Mot de passe
   - Schema
3. Cliquer `Tester la connexion`

L'application cree automatiquement les tables si elles n'existent pas:
- `public.call_center_records`

Si la base cible (ex: `call_center`) n'existe pas encore, l'application tente de la creer automatiquement.

Vous pouvez aussi creer le schema manuellement avec:
- `sql/schema_call_center.sql`

## 5) Import Excel vers PostgreSQL

Toujours depuis la barre laterale:
1. Ouvrir `Importer Excel vers PostgreSQL`
2. Choisir le mode:
   - `append`: ajoute les nouvelles lignes
   - `replace`: remplace les lignes de la table cible
3. Uploader un ou plusieurs fichiers Excel (`.xls`, `.xlsx`) dans un seul champ.
   - Le type (appels/alertes) est detecte automatiquement.
4. Cliquer `Importer vers PostgreSQL`

Les fichiers sont standardises automatiquement avant ecriture SQL.

Nouveautes import:
- anti-doublons par `file_hash` (meme contenu) + controle nom/periode
- rejet si la colonne `date` est absente/non reconnue
- detection des colonnes manquantes
- detection des lignes incompletes
- rapport d'import detaille (lignes lues, inserees, doublons, statut, message)

## 6) Variables d'environnement (optionnel, recommande)

Vous pouvez pre-remplir la connexion:

```bash
setx PGHOST "localhost"
setx PGPORT "5432"
setx PGDATABASE "call_center"
setx PGUSER "postgres"
setx PGPASSWORD "votre_mot_de_passe"
setx PGSCHEMA "public"
setx PGSSLMODE "prefer"
```

Puis redemarrer le terminal.

## 6.1) Fichier local de connexion (ignore par git)

Vous pouvez aussi creer un fichier local `pg_config.local.json` (non versionne).

Exemple (modele disponible dans `pg_config.local.example.json`):

```json
{
  "host": "localhost",
  "port": "5432",
  "database": "call_center",
  "user": "postgres",
  "password": "VOTRE_MOT_DE_PASSE",
  "schema": "public",
  "sslmode": "prefer"
}
```

## 7) Structure SQL utilisee

### Table `call_center_records`
- `date` (timestamp)
- `heure` (text)
- `numero` (text)
- `province` (text)
- `territoire` (text)
- `item` (text)
- `details` (text)
- `details_appel` (text)
- `incident` (text)
- `type_pathologie` (text)
- `categorie` (text)
- `genre` (text)
- `statut` (text)
- `resolution` (text)
- `record_count` (double precision)
- `source_file` (text)
- `sheet_name` (text)
- `source_kind` (text: `calls` / `alerts`)
- `imported_at` (timestamptz)

### Table `import_audit`
- `dataset_type`, `file_name`, `file_hash`
- `date_min`, `date_max`
- `total_rows`, `rows_inserted`, `duplicate_rows`
- `missing_columns`, `missing_rows`
- `status`, `message`, `imported_at`

## 8) Dictionnaire de donnees

Nouveau fichier:
- `data_dictionary.py`

Contenu:
- normalisation province/territoire (referentiel RDC base sur la liste fournie)
- mapping territoire -> province
- dictionnaire de pathologies (alias -> libelle canonique)

## 9) Etats de l'application

1. `PostgreSQL vide`:
- message de guidage pour importer les Excel

2. `Donnees chargees`:
- KPI, filtres et graphiques actifs

3. `Filtres appliques`:
- toutes les vues se recalculent dynamiquement

4. `Resultat vide`:
- message explicite en remplacement du graphique

## 10) Depannage rapide

Probleme: connexion PostgreSQL refusee
- verifier host/port/user/password/base
- verifier que le service PostgreSQL tourne

Probleme: carte vide
- les provinces doivent correspondre au referentiel RDC (alias inclus)

Probleme: table vide apres import
- verifier les colonnes Excel (mapping automatique base sur alias)
- verifier les erreurs affichees dans le panneau d'import

Probleme: dashboard ne montre rien
- verifier qu'il y a des donnees importees pour la periode filtree

## 11) Mode admin import (lecture seule pour les autres)

Le panneau `Importer Excel vers PostgreSQL` peut etre verrouille par mot de passe admin.

Configurer dans les secrets/env:
- `ADMIN_IMPORT_PASSWORD`

Exemple local Windows:

```bash
setx ADMIN_IMPORT_PASSWORD "MonCodeAdminFort"
```

Effet:
- si `ADMIN_IMPORT_PASSWORD` est defini: import bloque tant que le code admin n'est pas saisi
- si non defini: import ouvert pour tous

## 12) Gestion des utilisateurs (admin / utilisateur)

L'application integre maintenant une connexion en barre laterale avec 2 roles:
- `administrateur`: acces complet (configuration PostgreSQL, import Excel -> PostgreSQL, tous les boutons)
- `utilisateur`: lecture seule (visualisation sections + export Excel final)

### Source des comptes (priorite)

1. PostgreSQL: table `public.dashboard_users` (source principale)
2. Secrets/env (fallback)
3. Fallback dev (`admin/admin`, `user/user`) si rien n'est configure

La table `dashboard_users` est creee automatiquement et peut etre administree dans:
- barre laterale -> `Gestion utilisateurs dashboard` (profil administrateur)

Mode d'authentification:
- `DASHBOARD_AUTH_SOURCE=postgres` (defaut, recommande)
- `DASHBOARD_AUTH_SOURCE=auto` (autorise fallback secrets/env si PostgreSQL indisponible)

### Configuration initiale recommandee (seeding)

```bash
setx DASHBOARD_ADMIN_USERNAME "admin_cc"
setx DASHBOARD_ADMIN_PASSWORD "MotDePasseAdminFort"
setx DASHBOARD_USER_USERNAME "user_cc"
setx DASHBOARD_USER_PASSWORD "MotDePasseUserFort"
```

Ces comptes servent de seed initial si la table `dashboard_users` est vide.

Option fallback (avancee) avec JSON:

```bash
setx DASHBOARD_USERS_JSON "[{\"username\":\"admin_cc\",\"password\":\"MotDePasseAdminFort\",\"role\":\"administrateur\"},{\"username\":\"user_cc\",\"password\":\"MotDePasseUserFort\",\"role\":\"utilisateur\"}]"
```

Vous pouvez aussi stocker les mots de passe en SHA-256:
- format: `sha256:<hexdigest>`

Important:
- En production, remplacez les comptes par defaut et utilisez des mots de passe forts.
- Vous pouvez stocker un hash au format `sha256:<hexdigest>`.

## 13) Export PostgreSQL -> DHIS2 (serveur dev)

Nouveau script: `dhis2_export.py`

Objectif:
- lire PostgreSQL (table unique `call_center_records`)
- calculer:
  - total alertes
  - total appels
  - resolu / non resolu
  - repartition par sexe
  - repartition par categorie
  - repartition par pathologie
- pousser vers DHIS2 via `/api/dataValueSets`

### Variables obligatoires DHIS2

```bash
setx DHIS2_URL "https://<votre-serveur-dhis2>"
setx DHIS2_USERNAME "admin"
setx DHIS2_PASSWORD "district"
setx DHIS2_ORG_UNIT_UID "<UID_ORGUNIT>"
```

### UIDs data elements (minimaux)

```bash
setx DHIS2_DE_ALERTES_TOTAL "<UID>"
setx DHIS2_DE_APPELS_TOTAL "<UID>"
setx DHIS2_DE_RESOLU "<UID>"
setx DHIS2_DE_NON_RESOLU "<UID>"
setx DHIS2_DE_HOMMES "<UID>"
setx DHIS2_DE_FEMMES "<UID>"
setx DHIS2_DE_ND "<UID>"
```

### Mappings categories/pathologies (optionnels mais recommandes)

```bash
setx DHIS2_CATEGORY_DATAELEMENT_MAP "{\"Alerte\":\"<UID>\",\"Questions/Preoccupations\":\"<UID>\"}"
setx DHIS2_PATHOLOGY_DATAELEMENT_MAP "{\"Cholera\":\"<UID>\",\"MonkeyPox\":\"<UID>\"}"
```

### Execution

```bash
python dhis2_export.py --start-date 2026-02-18 --end-date 2026-02-25 --period 20260225
```

`--period` doit correspondre au format de period DHIS2 de votre configuration (jour/mois).

## 14) Deploiement Streamlit Cloud + PostgreSQL

### Cas recommande (production)

Utiliser une base PostgreSQL hebergee en ligne (Neon, Supabase, Render, RDS...).

1. Creer la base distante
2. Executer `sql/schema_call_center.sql`
3. Configurer les secrets Streamlit:
   - `PGHOST`
   - `PGPORT`
   - `PGDATABASE`
   - `PGUSER`
   - `PGPASSWORD`
   - `PGSCHEMA`
   - `PGSSLMODE` (`require` souvent recommande)
   - `ADMIN_IMPORT_PASSWORD` (optionnel mais recommande)
4. Deployer l'app sur Streamlit Cloud

### Utiliser ta base locale de ton PC (possible, mais fragile)

Ce n'est pas direct avec `localhost`. Streamlit Cloud doit atteindre une adresse publique.

Conditions obligatoires:
- ton PC reste allume 24/7
- Postgres ecoute sur une interface reseau (pas seulement localhost)
- ouverture/forward du port 5432 (ou tunnel TCP securise)
- firewall + `pg_hba.conf` configures pour autoriser uniquement l'IP/tunnel voulu
- TLS recommande

En pratique:
1. Exposer ton Postgres via IP publique + routeur (ou tunnel type Cloudflare Tunnel/Tailscale/ngrok TCP)
2. Tester depuis un reseau externe
3. Mettre cette adresse publique dans les secrets Streamlit

Attention securite:
- ne jamais exposer Postgres publiquement sans restriction IP + mot de passe fort + SSL
- pour un usage multi-utilisateurs, une base cloud geree reste nettement plus sure et stable
