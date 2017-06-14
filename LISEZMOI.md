# GeoZones - Spécificités françaises

Le fichier [france.py](france.py) traite les spécificités françaises.

Les niveaux suivants sont gérés : régions, départements, EPCI,
collectivités d’outre-mer, arrondissements, communes et cantons.

Ils utilisent les [GeoIDs](https://github.com/etalab/geoids).

Les sources de données sont :

* [les exports OpenStreetMap](http://osm13.openstreetmap.fr/~cquest/openfla/export/)
  pour les arrondissements, EPCI, départements, régions, communes et cantons
* [les contours des IRIS issus de l’INSEE](https://www.data.gouv.fr/fr/datasets/contour-des-iris-insee-tout-en-un/)
* [les contours des pays de TheMaticMapping](http://thematicmapping.org/downloads/)
  pour l’outre-mer
* [la base officielle des code postaux de La Poste](https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/)
  pour les codes postaux
* [la liste des EPCI](http://www.collectivites-locales.gouv.fr/liste-et-composition-2015)
  pour les EPCI
* [le COG de l’INSEE](https://www.insee.fr/fr/information/2666684)
  pour les arrondissements

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
