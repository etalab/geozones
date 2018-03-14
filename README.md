# GeoZones

Simplistic spatial/administrative referential.

_Pour une documentation relative aux niveaux administratifs français, veuillez consulter le fichier [LISEZMOI.md](LISEZMOI.md)._

This project is a set of tools to produce a shared spatial/administrative referential based on open datasets.

The purpose is to be embeddable in applications for autocompletion. There is no purpose of universality (country levels are not comparable) nor precision (most sourced datasets have a 100m precision).

These tools work on and exports _WGS84_ spatial data.

## Requirements

This project uses MongoDB 2.6+ and GDAL as main tooling. Build tools are written in Python 3 and make use of:

- click
- PyMongo
- Fiona
- Shapely

The web interface requires Flask.

Translations requires Babel and Transifex client.

## Getting started

There are many way of getting a development environment started.

Assuming you have Virtualenv and MongoDB installed and configured on you computer:

```bash
$ git clone https://github.com/etalab/geozones.git
$ cd geozones
$ virtualenv -p /bin/python3 .
$ source bin/activate
$ pip install -e .
$ geozones -h
```

## Model

There are two main models:

- level hierarchies
- zone/territories

GeoZones use MongoDB as working storage.

### Levels

They define relationships between levels and their names.
They are not stored into the database but they are exported with the following properties:

| Property    | Description                                                                 |
|-------------|-----------------------------------------------------------------------------|
| id          | A string identifier for the level (ie. `country`, `fr:commune`...)          |
| label       | The humain string representation in English (ie. `World`). __\*__           |
| admin_level | An administrative scale index (0 is the biggest and 100 the smallest level) |
| parents     | The list of known parent levels identifier                                  |

__\*__: Labels are optionally translatables


You can contribute your country specific levels.
Currently geozones support the following levels:

#### Common levels


| identifier       | administrative level | description
|------------------|----------------------|--------------
| `country-group`  | 10                   | Groups of countries (`World`, `UE`...)
| `contry`         | 20                   | A country
| `country-subset` | 30                   | An administrative subset of a country


#### French levels

| identifier          | administrative level | description                    |
|---------------------|----------------------|--------------------------------|
| `fr:region`         | 40                   | Regions of France              |
| `fr:epci`           | 68                   | Intercommunality of France     |
| `fr:departement`    | 60                   | Departements of France         |
| `fr:collectivite`   | 60                   | French overseas collectivities |
| `fr:arrondissement` | 70                   | Arrondissements of France      |
| `fr:commune`        | 80                   | Communes of France             |
| `fr:canton`         | 98                   | Cantons of France              |
| `fr:iris`           | 98                   | Iris of France                 |

### Zones

A zone is a spatial polygon for a given level. It has at least one unique code (unique on its level) and a name. It can have many known keys, that are not necessarily unique (ie. postal codes can be shared by many towns).

Labels are optionally translatable.

Some zones are defined as an aggregation of other zones. They are called _aggregation_ in geozones and built after all data are loaded.

The following properties are exported in the GeoJSON output:

| Property   | Description                                                 |
|------------|-------------------------------------------------------------|
| id         | A unique identifier defined by `<level>:<code>[@creation]`  |
| code       | The zone unique identifier in this level                    |
| level      | The level identifier                                        |
| name       | The zone display name (can be translatable)                 |
| population | Estimated/approximative population _(optional)_             |
| area       | Estimated/approximative area in km² _(optional)_            |
| wikipedia  | A Wikipedia reference _(optional)_                          |
| dbpedia    | A DBPedia reference _(optional)_                            |
| flag       | A DBPedia reference to a flag _(optional)_                  |
| blazon     | A DBPedia reference to a blazon _(optional)_                |
| keys       | A dictionary of known keys/code for this zone               |
| parents    | A list of every known parent zone identifier                |

> Note that you can choose via the keys option which properties you would like to export during the `dist`ribution step.

## Translations

Level names and some territories are translatable. They are provided as _gettext_ files. Translations are handled on [transifex](https://www.transifex.com/projects/p/geozones/).

Here’s the workflow:

```bash
# Ensure you have the optionnal tools to process translations
$ pip install -e .[i18n]
# Extract translatabls labels
$ pybabel extract -F babel.cfg -o geozones/translations/geozones.pot .
# Push updated translations template to Transifex
$ tx push -s
# Fetch last translations from Transifex
$ tx pull
# Compile translations for packaging/distribution
$
```

To add an extra language:

```bash
$ pybabel init -D geozones -i geozones/translations/geozones.pot -d geozones/translations -l <language code>
$ tx push -t -l <language code>
```

## Commands

A set of commands are provided for the build process. You can list them all with:

```bash
$ geozones --help
```

### `download`

Download the required datasets. Datasets will be stored into a `downloads` subdirectory.

### `load`

Load and process datasets into database.

### `aggregate`

Perform zones aggregations for zones defined as aggregation of others.

### `postprocess`

Perform some non geospatial processing (ex: set the postal codes, attach the parents…).

`--exclude` and `--only` options make possible to run a set of postprocess function(s).

### `dist`

Dump the produced dataset as GeoJSON files for distribution. Files are dumped in a _build_ subdirectory.

### `full`

All in one task equivalent to:

```bash
# Perform all tasks from download to distibution
$ geozones download preload load aggregate postprocess dist
```

### `explore`

Serve a _web interface_ to explore the generated data.

### `status`

Display some useful informations and statistics.

Commands are chainable so you can write:

```bash
# Perform all tasks from download to distibution
$ geozones download load -d aggregate postprocess dist dist -s status
```

### `sourceslist`

Generate a datasets donwload list for external usage.

This allows using an external download manager by example.

**Ex:** using 10 parallels threads with curl:

```bash
mkdir download && cd download && geozones sourceslist | xargs -P 10 -n 1 curl -O
```

### `logos`

Fetch zones logos/flags/blazons from Wikipedia when available.

## Options

### `serialization`

You can export data in (Geo)JSON or [msgpack](http://msgpack.org/) formats.

The _msgpack_ format consumes more CPU on deserialization but does not take many gigabytes of RAM given that it can iterate over data without loading the whole file.

## Reused datasets

- [NaturalEarth administrative boundaries](http://www.naturalearthdata.com/downloads/110m-cultural-vectors/110m-admin-0-countries/)
- [The Matic Mapping country boundaries](http://thematicmapping.org/downloads/world_borders.php)
- [OpenStreetMap french regions boundaries](http://www.data.gouv.fr/datasets/contours-des-regions-francaises-sur-openstreetmap/)
- [OpenStreetMap french counties boundaries](http://www.data.gouv.fr/datasets/contours-des-departements-francais-issus-d-openstreetmap/)
- [OpenStreetMap french EPCIs boundaries](http://www.data.gouv.fr/datasets/contours-des-epci-2014/)
- [OpenStreetMap french districts boundaries](http://www.data.gouv.fr/datasets/contours-des-arrondissements-francais-issus-d-openstreetmap/)
- [OpenStreetMap french towns boundaries](http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/)
- [OpenStreetMap french cantons boundaries](http://www.data.gouv.fr/fr/datasets/contours-osm-des-cantons-electoraux-departementaux-2015/)
- [IGN/ISEE IRIS aggregated version](https://www.data.gouv.fr/fr/datasets/contour-des-iris-insee-tout-en-un/)
- [French postal codes database](https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/)


## Possible improvements

### Build

- Incremental downloads, maybe with checksum check
- Global post-processor
- Post-processor dependencies
- Audit trail
- Distribute GeoZone as a standalone python executable
- Some quality check tools

### Fields

- Global weight = f(population, area, level)

### Output

- Different precision output
- Localized JSON outputs (Output are english only right now)
- Translations as distributable JSON (as an alternative to the current PO/MO format)
- Translations as Python package
- Model versioning
- Statistics/coverages in levels

### Web interface

- Querying
- Only fetch zones for viewport (less intensive for lower layers)
- A full web-service as a separate project
