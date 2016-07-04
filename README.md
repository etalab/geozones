GeoZones
========

Simplistic spatial/administrative referential.

This project is a set of tools to produce a shared spatial/administrative referential based on open datasets.

The purpose is to be embeddable in applications for autocompletion. There is no purpose of universality (country levels are not comparable) nor precision (most sourced datasets have a 100m precision).

These tools work on and exports _WGS84_ spatial data.

Requirements
------------

This project uses MongoDB 2.6+ and GDAL as main tooling. Build tools are written in Python 3 and make use of:

-   click
-   PyMongo
-   Fiona
-   Shapely

The web interface requires Flask.

Translations requires Babel and Transifex client.

Getting started
---------------

There are many way of getting a development environment started.

Assuming you have Virtualenv and MongoDB installed and configured on you computer:

``` sourceCode
$ git clone https://github.com/etalab/geozones.git
$ cd geozones
$ virtualenv -p /bin/python3 .
$ source bin/activate
$ pip install -r requirements.pip
$ ./geozones.py
```

Model
-----

There are two main models:

-   level hierarchies
-   zone/territories

GeoZones use MongoDB as working storage.

### Levels

They define relationships between levels and their names. They are not stored into any database.

### Zones

A zone is a spatial polygon for a given level. It has at least one unique code (unique on its level) and a name. It can have many known keys, that are not necessarily unique (ie. postal codes can be shared by many towns).

Labels are optionally translatable.

Some zones are defined as an aggregation of other zones. They are called _aggregation_ in geozones and builded after all data are loaded.

The following properties are exported in the GeoJSON output:

Property | Description
---------|------------
id | A unique identifier defined by `<level>/<code>`
code | The zone unique identifier in this level
level | The level identifier
name | The zone display name (can be translatable)
population | Estimated/approximative population *(optional)*
area | Estimated/approximative area in km2 *(optional)*
wikipedia | A Wikipedia reference *(optional)*
dbpedia | A DBPedia reference *(optional)*
flag | A DBPedia reference to a flag *(optional)*
blazon | A DBPedia reference to a blazon *(optional)*
keys | A dictionary of known keys/code for this zone
parents | A list of every known parent zone identifier

> Note that you can choose via the keys option which properties you would like to export during the `dist`ribution step.

Translations
------------

Level names and some territories are translatable. They are provided as _gettext_ files. Translations are handled on [transifex](https://www.transifex.com/projects/p/geozones/).

Here’s the workflow:

``` sourceCode
# Extract translatabls labels
$ pybabel extract -F babel.cfg -o translations/geozones.pot .
# Push updated translations template to Transifex
$ tx push -s
# Fetch last translations from Transifex
$ tx pull
# Compile translations for packaging/distribution
$
```

To add an extra language:

``` sourceCode
$ pybabel init -D geozones -i translations/geozones.pot -d translations -l <language code>
$ tx push -t -l <language code>
```

Commands
========

A set of commands are provided for the build process. You can list them all with:

``` sourceCode
$ ./geozones.py --help
```

`download`
----------

Download the required datasets. Datasets will be stored into a `downloads` subdirectory.

`load`
------

Load and process datasets into database.

`aggregate`
-----------

Perform zones aggregations for zones defined as aggregation of others.

`postprocess`
-------------

Perform some non geospatial processing (ex: set the postal codes, attach the parents…).

`dist`
------

Dump the produced dataset as GeoJSON files for distribution. Files are dumped in a _build_ subdirectory.

`full`
------

All in one task equivalent to:

``` sourceCode
# Perform all tasks from download to distibution
$ ./geozones.py download load aggregate postprocess dist
```

`explore`
---------

Serve a _web interface_ to explore the generated data.

`status`
--------

Display some useful informations and statistics.

Commands are chainable so you can write:

``` sourceCode
# Perform all tasks from download to distibution
$ ./geozones.py download load -d aggregate postprocess dist dist -s status
```

Options
=======

`serialization`
---------------

You can export data in (Geo)JSON or [msgpack](http://msgpack.org/) formats.

The _msgpack_ format consumes more CPU on deserialization but does not take many gigabytes of RAM given that it can iterate over data without loading the whole file.

Reused datasets
===============

-   [NaturalEarth administrative boundaries](http://www.naturalearthdata.com/downloads/110m-cultural-vectors/110m-admin-0-countries/)
-   [The Matic Mapping country boundaries](http://thematicmapping.org/downloads/world_borders.php)
-   [OpenStreetMap french regions boundaries](http://www.data.gouv.fr/datasets/contours-des-regions-francaises-sur-openstreetmap/)
-   [OpenStreetMap french counties boundaries](http://www.data.gouv.fr/datasets/contours-des-departements-francais-issus-d-openstreetmap/)
-   [OpenStreetMap french EPCIs boundaries](http://www.data.gouv.fr/datasets/contours-des-epci-2014/)
-   [OpenStreetMap french districts boundaries](http://www.data.gouv.fr/datasets/contours-des-arrondissements-francais-issus-d-openstreetmap/)
-   [OpenStreetMap french towns boundaries](http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/)
-   [OpenStreetMap french cantons boundaries](http://www.data.gouv.fr/fr/datasets/contours-osm-des-cantons-electoraux-departementaux-2015/)
-   [IGN/ISEE IRIS aggregated version](https://www.data.gouv.fr/fr/datasets/contour-des-iris-insee-tout-en-un/)
-   [French postal codes database](https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/)


Possible improvements
=====================

Build
-----

- Incremental downloads, maybe with checksum check
- Global post-processor
- Post-processor dependencies
- Audit trail
- Distribute GeoZone as a standalone python executable
- Some quality check tools

Fields
------

- Global weight = f(population, area, level)

Output
------

- Different precision output
- Localized JSON outputs (Output are english only right now)
- Translations as distributable JSON (as an alternative to the current PO/MO format)
- Translations as Python package
- Model versioning
- Statistics/coverages in levels

Web interface
-------------

-   Querying
-   Only fetch zones for viewport (less intensive for lower layers)
-   A full web-service as a separate project
