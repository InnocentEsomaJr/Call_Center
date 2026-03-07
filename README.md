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
- `public.call_center_calls`
- `public.call_center_alerts`

Si la base cible (ex: `call_center`) n'existe pas encore, l'application tente de la creer automatiquement.

Vous pouvez aussi creer le schema manuellement avec:
- `sql/schema_call_center.sql`

## 5) Import Excel vers PostgreSQL

Toujours depuis la barre laterale:
1. Ouvrir `Importer Excel vers PostgreSQL`
2. Choisir le mode:
   - `append`: ajoute les nouvelles lignes
   - `replace`: remplace les lignes de la table cible
3. Uploader un ou plusieurs fichiers:
   - APPELS (`.xls`, `.xlsx`)
   - ALERTES (`.xls`, `.xlsx`)
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

## 7) Structure SQL utilisee

### Table `call_center_calls`
- `date` (timestamp)
- `province` (text)
- `territoire` (text)
- `details` (text)
- `incident` (text)
- `categorie` (text)
- `genre` (text)
- `statut` (text)
- `record_count` (double precision)
- `source_file` (text)
- `sheet_name` (text)
- `imported_at` (timestamptz)

### Table `call_center_alerts`
- `date` (timestamp)
- `location` (text)
- `indicator` (text)
- `value` (double precision)
- `details` (text)
- `source_file` (text)
- `sheet_name` (text)
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
- verifier qu'il y a des donnees APPELS pour la periode filtree

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

## 12) Deploiement Streamlit Cloud + PostgreSQL

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
