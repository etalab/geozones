GeoZones
========

Simplistic spatial/administrative referential.

This project is a set of tools to produce a shared spatial/administrative referential
based on open datasets.

The purpose is to be embeddable in applications for autocompletion.
There is no purpose of universality (country levels are not comparables)
nor precision (most sourced datasets have a 100m precision).

These tools work on and exports WGS84 spatial data.


Requirements
------------

This project use MongoDB 2.6+ and GDAL as main tooling.
Build tools are written in Python 3 and make use of:

- click
- PyMongo
- Fiona
- Shapely

The web interface requires Flask

Translations requires Babel and Transifex client

Getting started
---------------

There is many way of getting a developement environement started.

Assuming you have Virtualenv and MongoDB installed and configured on you computer:

.. code-block:: shell

    $ git clone https://github.com/etalab/geozones.git
    $ cd geozones
    $ virtualenv -p /bin/python3 .
    $ source bin/activate
    $ pip install -r requirements.pip
    $ ./geozones.py


Model
-----

There is two main models:

- level hierarchies
- zone/territories

GeoZones use MongoDB as working storage.

Levels
~~~~~~

They define relationship between levels and their names.
They are not stored into database.

Zones
~~~~~

A zone is a spatial polygon for a given level.
It has at least one unique code (unique on its level) and a name.
It can have many known keys, that are not necessary unique
(ie. postal codes can be shared by many town)

Labels are optionnaly translatables.

Some zones are defined as an aggregation of other zones.
They are called aggregation in geozones and builded after all data are loaded.

The following properties are exported in the GeoJSON output:

:id:
    A unique identifier defined by ``<level>/<code>``

:code:
    The zone unique identifier in this level

:level:
    The level identifier

:name:
    The zone display name (can be translatable)

:population:
    Estimated/approximative population *(optional)*

:area:
    Estimated/approximative area in km2 *(optional)*

:wikipedia:
    A Wikipedia/DBPedia reference *(optional)*

:keys:
    A dictionnary of known keys/code for this zone

:parents:
    A list of every known parent zone identifier


Translations
------------

Level names and some territories are translatables.
They are providen as gettext files.
Translations are handled on `transifex <https://www.transifex.com/projects/p/geozones/>`_.

Here's the workflow:

.. code-block:: shell

    # Extract translatables labels
    $ pybabel extract -F babel.cfg -o translations/geozones.pot .
    # Push updated translations template to Transifex
    $ tx push -s
    # Fetch last translations from Transifex
    $ tx pull
    # Compile translations for packaging/distribution
    $ pybabel compile -D geozones -d translations

To add an extra language:

.. code-block:: shell

    $ pybabel init -D geozones -i translations/geozones.pot -d translations -l <language code>
    $ tx push -t -l <language code>


Commands
--------

A set of commands are providen for the build process.
You can list all of them with:

.. code-block:: shell

    $ ./geozones.py --help


``download``
~~~~~~~~~~~~

Download the required datasets.
Datasets will be stored into a ``downloads`` subdirectory.


``load``
~~~~~~~~

Load and process datasets into database.


``aggregate``
~~~~~~~~~~~~~

Perform zones aggregations for zones defined as aggregation of others.

``postprocess``
~~~~~~~~~~~~~~~

Perform some non geospatial processing (ex: set the postal codes, attach the parents...).


``dist``
~~~~~~~~

Dump the produced dataset as GeoJSON files for distribution.
Files are dumped in a build subdirectory.


``full``
~~~~~~~~

All in one task equivalent to:

.. code-block:: shell

    # Perform all tasks from download to distibution
    $ ./geozones.py download load aggregate postprocess dist


``explore``
~~~~~~~~~~~

Serve a webinterface to explore the generated data.


``info``
~~~~~~~~

Display some useful informations and statistics


Commands are chainables so you can write:

.. code-block:: shell

    # Perform all tasks from download to distibution
    $ ./geozones.py download load -d aggregate postprocess dist dist -s info


Reused datasets
---------------

- `NaturalEarth administratives boundaries <http://www.naturalearthdata.com/downloads/110m-cultural-vectors/110m-admin-0-countries/>`_
- `The Matic Mapping country boundaries <http://thematicmapping.org/downloads/world_borders.php>`_
- `OpenStreetMap french regions boundaries <http://www.data.gouv.fr/datasets/contours-des-regions-francaises-sur-openstreetmap/>`_
- `OpenStreetMap french counties boundaries <http://www.data.gouv.fr/datasets/contours-des-departements-francais-issus-d-openstreetmap/>`_
- `OpenStreetMap french EPCIs boundaries <http://www.data.gouv.fr/datasets/contours-des-epci-2014/>`_
- `OpenStreetMap french districts boundaries <http://www.data.gouv.fr/datasets/contours-des-arrondissements-francais-issus-d-openstreetmap/>`_
- `OpenStreetMap french towns boundaries <http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/>`_
- `OpenStreetMap french cantons boundaries <http://www.data.gouv.fr/fr/datasets/contours-osm-des-cantons-electoraux-departementaux-2015/>`_
- `IGN/ISEE IRIS agregated version <https://www.data.gouv.fr/fr/datasets/contour-des-iris-insee-tout-en-un/>`_
- `French postal codes database <https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/>`_
- `DGCL EPCIs list <http://www.collectivites-locales.gouv.fr/liste-et-composition-2015>`_
- `INSEE COG <http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement.asp>`_


Possible improvements
---------------------

Datasets
~~~~~~~~

- Use enriched version of French towns boundaries (when updated for 2015)

Build
~~~~~

- Incremental downloads, maybe with checksum check
- Global postprocessor
- Postprocessor dependencies
- Audit trail
- Better and lightweight DBPedia retrieval (using http://dbpedia.org/sparql)
- Distribute GeoZone as a standalone python executable
- Allow to execute a single postprocessor

Fields
~~~~~~

- Global weight = f(population, area, level)
- Images/Logos/Shields/Flags: it would be nice to have the official pictogram associated with each zone (DBPedia)

Output
~~~~~~

- Different precision output
- Localized JSON outputs (Output are english only right now)
- Different outputs formats (Only JSON/GeoJSON is outputted)
- Translations as distributable JSON (as an alternative to the current PO/MO format)
- Translations as Python package
- Model versionning
- Statistics in levels

Web interface
~~~~~~~~~~~~~

- Querying
- Only fetch zones for viewport (less intensive for lower layers)
