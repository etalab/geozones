# GeoZones - Spécificités françaises

Le fichier [france.py](france.py) traite les spécificités françaises.

Les niveaux suivants sont gérés : régions, départements, EPCI,
collectivités d’outre-mer, arrondissements, communes et cantons.

Ils utilisent les [GeoIDs](https://github.com/etalab/geoids).

Les sources de données sont :

- [les exports OpenStreetMap](http://osm13.openstreetmap.fr/~cquest/openfla/export/) pour les arrondissements, EPCI, départements, régions, communes et cantons
- [les contours des IRIS issus de l’INSEE](https://www.data.gouv.fr/fr/datasets/contour-des-iris-insee-tout-en-un/)
- [les contours des pays de TheMaticMapping](http://thematicmapping.org/downloads/) pour l’outre-mer
- [la base officielle des code postaux de La Poste](https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/) pour les codes postaux
- [la liste des EPCI](http://www.collectivites-locales.gouv.fr/liste-et-composition-2015) pour les EPCI
- [le COG de l’INSEE](https://www.insee.fr/fr/information/2666684) pour les arrondissements

Les arrondissements de Paris, Lyon et Marseille sont dynamiquement
rattachés. Leurs populations sont agrégées dynamiquement.

DBPedia est utilisée pour compléter les populations, aires et URLs des
drapeaux et blasons existants et sous licence libre. Le projet
[GeoLogos](https://github.com/etalab/geologos/) permet ensuite de les
télécharger depuis Wikimedia.

L’utilisation de [GeoHisto](https://github.com/etalab/geohisto) permet
de gérer les historiques des régions, départements, collectivités
d’outre-mer et communes.

Les communes utilisent par exemple les exports OSM de 2017, 2016, 2015
et 2013 ce qui permet d’avoir un historique des contours géographiques
pour ces périodes. Il en va de même pour les régions en 2014 et 2016.

## Démarrage

Il y a plusieurs façon de mettre en place un environnement de dévelopment, nous en décrivons une.

En partant du principe que vous avez Virtualenv et MongoDB d'installé sur votre poste:

```bash
$ git clone https://github.com/etalab/geozones.git
$ cd geozones
$ virtualenv -p /bin/python3 .
$ source bin/activate
$ pip install -r requirements.pip
$ ./geozones.py
```

## Modèle

Il y a deux modèles principaux:

- les niveaux hierarchiques
- les zones/territoires

GeoZones utilise utilise MongoDB comme stockage de travail.

### Niveaux

Ils définissent la relation entre le code des niveaux et leurs noms.
Ils ne sont pas stockées en base mais sont exportés avec les attributs suivants:

| Attribut    | Description                                                                             |
|-------------|-----------------------------------------------------------------------------------------|
| id          | Un identifiant textuel (ex: `country`, `fr:commune`...)                                 |
| label       | La representation textuelle humaine en anglais (ex: `World`). __\*__                    |
| admin_level | Un indice d'échelle administrative (0 étant le plus grand niveaux et 100 le plus petit) |
| parents     | La liste des identifiants des parents connus d'un niveau                                |

__\*__: Les libellés sont optionnellement traductibles


#### Niveaux communs


| identifiant      | niveau | description                                                            |
|------------------|--------|------------------------------------------------------------------------|
| `country-group`  | 10     | Groupe de pays (`World`, `UE`...)                                      |
| `contry`         | 20     | Un pays                                                                |
| `country-subset` | 30     | Un sous-ensemble administratif d'un pays (ex: `France metropolitaine`) |


#### Niveaux français

| identifiant         | niveau | description                    |
|---------------------|--------|--------------------------------|
| `fr:region`         | 40     | Régions françaises             |
| `fr:epci`           | 68     | EPCI                           |
| `fr:departement`    | 60     | Départements français          |
| `fr:collectivite`   | 60     | Collectivités d'outremer       |
| `fr:arrondissement` | 70     | Arrondissements français       |
| `fr:commune`        | 80     | Communes française             |
| `fr:canton`         | 98     | Cantons français               |
| `fr:iris`           | 98     | Iris INSEE                     |

### Zones

Une zone est un polygonal geospatial pour un niveau donnée. Il a au moins un code unique (sur son niveau uniquement) and un nom. Il peut avoir plusieurs clés connues, pas nécéssairement uniques (ex: le code postal peut être paratagé par plusieurs communes).

Les libéllés sont optionnellement traductibles.

Certaines zones sont définies comme étant l'aggrégation d'autres zones. Elles sont appelées _aggregation_ dans geozones et construite après que toutes les données ai été chargées.

Les attributs suivants sont exportés dans le GeoJSON produit:

| Attribut   | Description                                                     |
|------------|-----------------------------------------------------------------|
| id         | Un identifiant unique définit par `<code>:<code>[@<creation>]`  |
| code       | L'identifiant unique de la zone sur ce niveau                   |
| level      | L'identifiant du niveau                                         |
| name       | Le nom d'affichage (peut être traductible)                      |
| population | Population estimée/approximative _(optionnel)_                  |
| area       | L'aire estimée/approximative en km² _(optionnel)_               |
| wikipedia  | Une référence Wikipedia _(optionnel)_                           |
| dbpedia    | Une référence DBPedia _(optionnel)_                             |
| flag       | Une référence DBPedia à un drapeau _(optionnel)_                |
| blazon     | Une référence DBPedia à un blason _(optionnel)_                 |
| keys       | Un dictionnaire des clés connues pour cette zone                |
| parents    | Une liste de tous les parents connus                            |
