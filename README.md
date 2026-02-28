# Dashboard Call Center (Python / Streamlit)

Tableau de bord interactif de gestion des appels et alertes, alimente par des fichiers Excel importes manuellement.

## 1) Objectif du projet

Ce dashboard permet de:

- centraliser les donnees d'appels et d'alertes
- filtrer rapidement par periode, province, genre, incident et categorie
- suivre les indicateurs cles (KPI)
- analyser la repartition des appels et alertes
- visualiser la geolocalisation des appels par province

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

## 4) Etats de l'application (important)

Le dashboard fonctionne par etats clairs:

1. Etat "Attente d'import"
- Aucun fichier n'est charge par defaut.
- L'utilisateur doit importer les fichiers depuis la barre laterale.
- Un message d'information s'affiche tant que les 2 types de fichiers ne sont pas presents.

2. Etat "Donnees chargees"
- Le tableau de bord s'active apres import d'au moins un fichier APPELS et un fichier ALERTES.
- Le bandeau KPI horizontal apparait sous le titre.
- Les filtres principaux deviennent operationnels.

3. Etat "Filtres appliques"
- Toute selection met a jour les KPI, tableaux, graphiques et carte.
- Si un filtre ne retourne rien, un message explicite s'affiche.
- Les analyses avancees (completude, resolution, comparaison periode precedente, profil horaire) se recalculent automatiquement.

4. Etat "Resultat vide"
- Si une vue n'a pas de donnees (ex: tendance vide), une alerte visuelle remplace le graphique.

## 5) Import des fichiers (options)

Le module d'import accepte maintenant plusieurs fichiers.

- APPELS: import multiple (`.xls`, `.xlsx`)
- ALERTES: import multiple (`.xls`, `.xlsx`)
- Les fichiers importes sont fusionnes automatiquement (concat)

Regles:

- pas de lecture automatique depuis disque
- pas de mode demo automatique
- import obligatoire depuis l'interface

## 6) Filtres disponibles

Filtres principaux:

- Selectionner la periode
- Province
- Genre
- Incident/Pathologie
- Categorie d'appel

Filtres page alertes:

- Localite
- Indicateur

## 7) Sections du dashboard (navigation)

1. `Informations generales`
- KPI verticaux (panneau complet)
- tableau des details d'appel (nature/type)
- proportion d'appel par province (barres)
- geolocalisation des appels (carte agrandie)
- evolution quotidienne des appels
- analyses avancees interactives par filtres

2. `Autres details d'informations`
- repartition par genre (donut)
- repartition par incident (barres)
- categories des appels (barres)
- evolution du volume d'appels

3. `Details alertes`
- filtres alertes
- indicateurs globaux alertes
- alertes par localite
- alertes par indicateur
- evolution des alertes
- tableau detail des alertes

## 8) Definition des KPI

Le dashboard calcule les indicateurs suivants:

- `Province`: nombre de provinces distinctes dans le jeu filtre
- `Tot appels`: somme de `record_count`
- `Resolu`: volume des appels avec statut resolu
- `Non resolu`: `Tot appels - Resolu`
- `Hommes`: volume des appels genre homme
- `Femmes`: volume des appels genre femme
- `ND`: volume des appels genre non defini

## 9) Structure des donnees attendue (mapping souple)

Le mapping des colonnes est automatique avec alias.

Exemples reconnus pour APPELS:

- Date: `date`, `periode`, `period`
- Heure: `heure`, `hour`
- Province: `province`
- Territoire: `territoire`, `zone`, `district`
- Details appel: `details`, `nature`, `description`, `details de l'appel`
- Incident/pathologie: `incident`, `pathologie`, `type`
- Categorie: `categorie`, `category`
- Genre: `genre`, `sexe`
- Statut: `statut`, `status`, `resolution`
- Compte: `record count`, `count`, `nombre`, `value`

Exemples reconnus pour ALERTES:

- Date: `date`, `periode`, `mois`
- Heure: `heure`, `hour`
- Localite: `province`, `organisation unit`, `territoire`
- Indicateur: `indicateur`, `data`, `type`, `categorie`
- Valeur: `value`, `count`, `nombre`

## 10) Regles techniques utiles

- Les dates Excel numeriques (ex: `46077`) sont converties automatiquement.
- Les heures fractionnaires Excel sont converties en horaire.
- Les agregations temporelles sont stabilisees par jour pour eviter les erreurs de version pandas.
- La geolocalisation est basee sur un referentiel de coordonnees de provinces RDC.

## 11) Depannage rapide

Probleme: "Importez au moins un fichier APPELS et un fichier ALERTES"
- Cause: un des deux types de fichier manque.
- Action: importer au moins un fichier de chaque type dans la barre laterale.

Probleme: pas de carte geolocalisation
- Cause: noms de province non reconnus dans le referentiel.
- Action: harmoniser les noms de province dans Excel.

Probleme: graphiques vides
- Cause: filtres trop restrictifs ou periode hors donnees.
- Action: reinitialiser les filtres et verifier les dates importees.
