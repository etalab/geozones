import csv
import io
from zipfile import ZipFile

from geo import Level, country, country_subset
from tools import info, success, warning, unicodify
from dbpedia import DBPedia


_ = lambda s: s


region = Level('fr/region', _('French region'), country)
epci = Level('fr/epci', _('French intermunicipal (EPCI)'), region)
county = Level('fr/departement', _('French county'), region)
district = Level('fr/district', _('French district'), county)
town = Level('fr/commune', _('French town'), district, epci)
canton = Level('fr/canton', _('French canton'), county)

# Not opendata yet
iris = Level('fr/iris', _('Iris (Insee districts)'), town)

# Cities with districts
PARIS_DISTRICTS = ['fr/commune/751{0:0>2}'.format(i) for i in range(1, 21)]

MARSEILLE_DISTRICTS = ['fr/commune/132{0:0>2}'.format(i) for i in range(1, 17)]

LYON_DISTRICTS = ['fr/commune/6938{0}'.format(i) for i in range(1, 9)]

# Overseas territories as counties
OVERSEAS = {
    'pm': ('975', 'Saint-Piere-et-Miquelon'),
    'bl': ('977', 'Saint-Barthélemy'),
    'mf': ('978', 'Saint-Martin'),
    'wf': ('986', 'Wallis-et-Futuna'),
    'pf': ('987', 'Polynésie française'),
    'nc': ('988', 'Nouvelle-Calédonie'),
    'tf': ('984', 'Terres australes et antarctiques françaises'),
}

FR_METRO_COUNTIES = (
    ['{0:0>2}'.format(i) for i in range(1, 20)]
    + ['2a', '2b']
    + ['{0:0>2}'.format(i) for i in range(21, 96)])

FR_DOM_COUNTIES = ('971', '972', '973', '974', '976')

FR_DOMTOM_COUNTIES = (
    '971', '972', '973', '974', '975', '976', '977', '978', '984', '986',
    '987', '988'
)

town.aggregate(
    '75056', 'Paris', PARIS_DISTRICTS,
    parents=['country/fr', 'country-group/ue', 'country-group/world',
             'fr/departement/75'],
    keys={'insee': '75056'})

town.aggregate(
    '13055', 'Marseille', MARSEILLE_DISTRICTS,
    parents=['country/fr', 'country-group/ue', 'country-group/world',
             'fr/departement/13'],
    keys={'insee': '13055'})

town.aggregate(
    '69123', 'Lyon', LYON_DISTRICTS,
    parents=['country/fr', 'country-group/ue', 'country-group/world',
             'fr/departement/69'],
    keys={'insee': '69123'})

country_subset.aggregate(
    'fr/metropole', _('Metropolitan France'),
    ['fr/departement/{0}'.format(code) for code in FR_METRO_COUNTIES],
    parents=['country/fr', 'country-group/ue', 'country-group/world'])

country_subset.aggregate(
    'fr/dom', 'DOM',
    ['fr/departement/{0}'.format(code) for code in FR_DOM_COUNTIES],
    parents=['country/fr', 'country-group/ue', 'country-group/world'])

country_subset.aggregate(
    'fr/domtom', _('Overseas France'),
    ['fr/departement/{0}'.format(code) for code in FR_DOMTOM_COUNTIES],
    parents=['country/fr', 'country-group/ue', 'country-group/world'])


@district.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/arrondissements-20131220-100m-shp.zip')
def extract_french_district(polygon):
    '''
    Extract a french district informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/contours-des-arrondissements-francais-issus-d-openstreetmap/
    '''
    props = polygon['properties']
    code = props['insee_ar'].lower()
    return {
        'code': code,
        'name': props['nom'],
        'area': props['surf_km2'],
        'wikipedia': props['wikipedia'],
        'parents': ['country/fr', 'country-group/ue', 'country-group/world'],
        'keys': {
            'insee': code,
        }
    }


@epci.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/epci-20150303-100m-shp.zip')
def extract_french_epci(polygon):
    '''
    Extract a french EPCI informations from a MultiPolygon.
    Based on data from http://www.data.gouv.fr/datasets/contours-des-epci-2014/
    '''
    props = polygon['properties']
    siren = props['siren_epci'].lower()
    return {
        'code': siren,
        'name': unicodify(props.get('nom_osm') or props['nom_epci']),
        'population': props['ptot_epci'],
        'area': props['surf_km2'],
        'wikipedia': unicodify(props['wikipedia']),
        'parents': ['country/fr', 'country-group/ue', 'country-group/world'],
        'keys': {
            'siren': siren,
            'osm': props['osm_id'],
            'type_epci': props['type_epci']
        }
    }


@county.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/departements-20140306-100m-shp.zip')
def extract_french_county(polygon):
    '''
    Extract a french county informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/contours-des-departements-francais-issus-d-openstreetmap/
    '''
    props = polygon['properties']
    name = props['nom']
    code = props['code_insee'].lower()
    return {
        'code': code,
        'name': unicodify(name),
        'wikipedia': unicodify(props['wikipedia']),
        'parents': ['country/fr', 'country-group/ue', 'country-group/world'],
        'keys': {
            'insee': code,
            'nuts3': props['nuts3'],
        }
    }


@county.extractor('http://thematicmapping.org/downloads/TM_WORLD_BORDERS-0.3.zip')
def extract_overseas_county(polygon):
    '''
    Extract overseas county from their WorldBorder country.
    Based on data from http://thematicmapping.org/downloads/world_borders.php

    '''
    props = polygon['properties']
    iso = props['ISO2'].lower()
    if iso in OVERSEAS:
        code, name = OVERSEAS[iso]
        return {
            'code': code,
            'name': unicodify(name),
            'population': props['POP2005'],
            'area': int(props['AREA']),
            'parents': ['country/fr', 'country-group/ue',
                        'country-group/world'],
            'keys': {
                'insee': code,
                'iso2': iso,
                'iso3': props['ISO3'].lower(),
                'un': props['UN'],
            }
        }


def preprocess_regions_from(filename):
    result = {}
    with open(filename) as regions:
        for region in csv.DictReader(regions, delimiter=','):
            result[region['insee_code']] = region
    return result


# TODO: grab directly from Github?
_regions = preprocess_regions_from('../geohisto/exports/regions/regions.csv')


@region.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20161121-shp.zip', simplify=0.01)
def extract_new_french_region(polygon):
    '''
    Extract new french region informations from a MultiPolygon.
    Based on data from:
    https://www.data.gouv.fr/fr/datasets/projet-de-redecoupages-des-regions/
    '''
    props = polygon['properties']
    region = _regions[props['code_insee']]
    return {
        'id': region['id'],
        'code': region['insee_code'],
        'name': unicodify(region['name']),
        'area': region['surface'],
        'population': region['population'],
        'wikipedia': unicodify(region['wikipedia']),
        'parents': ['country/fr', 'country-group/ue', 'country-group/world'],
        'keys': {
            'insee': region['insee_code'],
            'nuts2': region['nuts_code']
        },
        'ancestors': region['ancestors'].split(';'),
        'validity': {
            'start': region['start_datetime'],
            'end': region['end_datetime']
        }
    }


@region.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20140306-100m-shp.zip')
def extract_old_french_region(polygon):
    '''
    Extract a french region informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/contours-des-regions-francaises-sur-openstreetmap/
    '''
    props = polygon['properties']
    code_insee = props['code_insee']
    region = _regions[code_insee]
    return {
        'id': region['id'],
        'code': region['insee_code'],
        'name': unicodify(region['name']),
        'area': region['surface'],
        'population': region['population'],
        'wikipedia': unicodify(region['wikipedia']),
        'parents': ['country/fr', 'country-group/ue', 'country-group/world'],
        'keys': {
            'insee': region['insee_code'],
            'nuts2': region['nuts_code']
        },
        'successors': region['successors'].split(';'),
        'validity': {
            'start': region['start_datetime'],
            'end': region['end_datetime']
        }
    }


@town.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20160119-shp.zip', simplify=0.0005)
def extract_french_town(polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    code = props['insee'].lower()
    return {
        'code': code,
        'name': unicodify(props['nom']),
        'wikipedia': unicodify(props['wikipedia']),
        'area': int(props['surf_ha']),
        'parents': ['country/fr', 'country-group/ue', 'country-group/world'],
        'keys': {
            'insee': code,
        }
    }


@town.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/arrondissements-municipaux-20160128-shp.zip')
def extract_french_arrondissements(polygon):
    '''
    Extract a french arrondissements informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    code = props['insee'].lower()
    return {
        'code': code,
        'name': unicodify(props['nom']),
        'wikipedia': unicodify(props['wikipedia']),
        'area': int(props['surf_ha']),
        'parents': ['country/fr', 'country-group/ue', 'country-group/world'],
        'keys': {
            'insee': code,
        }
    }


@canton.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/cantons-2015-shp.zip', simplify=0.005)
def extract_french_canton(polygon):
    '''
    Extract a french canton informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/fr/datasets/contours-osm-des-cantons-electoraux-departementaux-2015/
    '''
    props = polygon['properties']
    code = props['ref'].lower()
    county_code = props['dep'].lower()
    county_id = 'fr/departement/{0}'.format(county_code)
    return {
        'code': code,
        'name': unicodify(props['nom']),
        'population': props['population'],
        'wikipedia': unicodify(props['wikipedia']),
        'parents': ['country/fr', 'country-group/ue', 'country-group/world',
                    county_id],
        'keys': {
            'ref': code,
            'jorf': props['jorf']
        }
    }


@iris.extractor('https://www.data.gouv.fr/s/resources/contour-des-iris-insee-tout-en-un/20150428-161348/iris-2013-01-01.zip')
def extract_iris(polygon):
    '''
    Extract French IrisBased on data from:
    http://professionnels.ign.fr/contoursiris
    '''
    props = polygon['properties']
    code = props['DCOMIRIS']
    town_code = props['DEPCOM'].lower()
    town_id = 'fr/commune/{0}'.format(town_code)
    name = unicodify(props['NOM_IRIS']).title()
    return {
        'code': code,
        'name': name,
        'parents': ['country/fr', 'country-group/ue', 'country-group/world',
                    town_id],
        '_type': props['TYP_IRIS'],
        'keys': {
            'iris': code
        },

    }


@town.postprocessor('http://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true')
def process_postal_codes(db, filename):
    '''
    Extract postal codes from:
    https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/
    '''
    info('Processing french postal codes')
    processed = 0
    with open(filename, encoding='cp1252') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        # skip header
        next(reader, None)
        for insee, _, postal, _, _ in reader:
            ops = {'$addToSet': {'keys.postal': postal}}
            if db.find_one_and_update({'level': town.id, 'code': insee}, ops):
                processed += 1
    success('Processed {0} french postal codes', processed)


@epci.postprocessor('http://www.collectivites-locales.gouv.fr/files/files/epcicom2015.csv')
def attach_epci(db, filename):
    '''
    Attach EPCI towns to their EPCI from:
    http://www.collectivites-locales.gouv.fr/liste-et-composition-2015
    '''
    info('Processing EPCI town list')
    processed = 0
    # epci_region = {}
    with open(filename, encoding='cp1252') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            siren = row['siren_epci']
            insee = row['insee'].lower()
            # region = row['region']
            # epci_region[siren] = region
            epci_id = 'fr/epci/{0}'.format(siren)
            if db.find_one_and_update(
                    {'level': town.id, 'code': insee},
                    {'$addToSet': {'parents': epci_id}}):
                processed += 1
    success('Attached {0} french town to their EPCI', processed)

    # info('Attach EPCI to counties and regions')
    # processed = 0
    # pipeline = [
    #     {'$match': {'level': town.id: 'parents': {'$regex': epci.id}}},
    #     {'$unwind': '$parents'},
    #     {'$match': {'parents': {'$regex': '^(' + county.id + '|' + region.id + ')'}}},
    #     {'$group': {'_id': '$parents', 'population': {'$sum': '$population'}}}
    # ]
    # for result in db.aggregate(pipeline):
    #     if db.find_one_and_update({'_id': result['_id']},
    #         {'$set': {'population': result['population']}}):
    #         processed += 1
    # success('Computed population for {0} french districts', processed)

    # processed = 0
    # for siren, region in epci_region.items():
    #     insee_region = region.replace('r', '')
    #     region_id = 'fr/region/{0}'.format(insee_region)
    #     if db.find_one_and_update({'level': epci.id, 'code': siren},
    #         {'$addToSet': {'parents': region_id, 'parents': region_id}}):
    #         processed += 1
    # success('Attached {0} french EPCI to their region', processed)


@town.postprocessor('https://www.insee.fr/fr/statistiques/fichier/2114819/france2016-txt.zip')
def process_insee_cog(db, filename):
    '''Use informations from INSEE COG to attach parents.
    http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement.asp
    '''
    info('Processing INSEE COG')
    processed = 0
    counties = {}
    districts = {}
    with ZipFile(filename) as cogzip:
        with cogzip.open('france2016.txt') as tsvfile:
            tsvio = io.TextIOWrapper(tsvfile, encoding='cp1252')
            reader = csv.DictReader(tsvio, delimiter='\t')
            for row in reader:
                # Lower everything, from 2B to 2b for instance.
                region_code = row['REG'].lower()
                county_code = row['DEP'].lower()
                district_code = row['AR'].lower()
                town_code = row['COM'].lower()
                insee_code = ''.join((county_code, town_code))

                region_id = 'fr/region/{0}'.format(region_code)
                county_id = 'fr/departement/{0}'.format(county_code)

                parents = [region_id, county_id]

                if county_id not in counties:
                    counties[county_id] = region_id

                if district_code:
                    district_code = ''.join((county_code, district_code))
                    district_id = 'fr/district/{0}'.format(district_code)
                    parents.append(district_id)
                    if district_id not in districts:
                        districts[district_id] = [region_id, county_id]

                if db.find_one_and_update(
                        {'level': town.id, 'code': insee_code},
                        {'$addToSet': {'parents': {'$each': parents}}}):
                    processed += 1
    success('Attached {0} french towns to their parents', processed)

    processed = 0
    for district_id, parents in districts.items():
        if db.find_one_and_update(
                {'_id': district_id},
                {'$addToSet': {
                    'parents': {'$each': parents},
                }}):
            processed += 1
    success('Attached {0} french districts to their parents', processed)

    processed = 0
    for county_id, parent in counties.items():
        if db.find_one_and_update(
                {'_id': county_id},
                {'$addToSet': {'parents': parent}}):
            processed += 1
    success('Attached {0} french counties to their parents', processed)


@town.postprocessor()
def town_with_districts(db, filename):
    info('Attaching Paris town districts')
    paris = db.find_one({'_id': 'fr/commune/75056'})
    parents = paris['parents']
    parents.append(paris['_id'])
    result = db.update_many(
        {'_id': {'$in': PARIS_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Paris', result.modified_count)

    info('Attaching Marseille town districts')
    marseille = db.find_one({'_id': 'fr/commune/13055'})
    parents = marseille['parents']
    parents.append(marseille['_id'])
    result = db.update_many(
        {'_id': {'$in': MARSEILLE_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Marseille', result.modified_count)

    info('Attaching Lyon town districts')
    lyon = db.find_one({'_id': 'fr/commune/69123'})
    parents = lyon['parents']
    parents.append(lyon['_id'])
    result = db.update_many(
        {'_id': {'$in': LYON_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Lyon', result.modified_count)


@town.postprocessor()
def fetch_missing_data_from_dbpedia(db, filename):
    info('Fetching DBPedia data')
    processed = 0
    for zone in db.find({
            'wikipedia': {'$exists': True, '$ne': None},
            '$or': [
                {'population': None},
                {'population': {'$exists': False}},
                {'area': None},
                {'area': {'$exists': False}},
            ]
            }, no_cursor_timeout=True):

        dbpedia = DBPedia(zone['wikipedia'])
        metadata = {
            'dbpedia': dbpedia.resource_url,
        }
        metadata.update(dbpedia.fetch_population_or_area())
        metadata.update(dbpedia.fetch_flag_or_blazon())
        if db.find_one_and_update({'_id': zone['_id']},
                                  {'$set': metadata}):
            processed += 1
    success('Fetched DBPedia data for {0} zones', processed)


@town.postprocessor()
def compute_town_with_districts_population(db, filename):
    info('Computing Paris town districts population')
    districts = db.find({'_id': {'$in': PARIS_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'fr/commune/75056'},
        {'$set': {'population': population}})
    success('Computed population for Paris')

    info('Computing Marseille town districts population')
    districts = db.find({'_id': {'$in': MARSEILLE_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'fr/commune/13055'},
        {'$set': {'population': population}})
    success('Computed population for Marseille')

    info('Computing Lyon town districts population')
    districts = db.find({'_id': {'$in': LYON_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'fr/commune/69123'},
        {'$set': {'population': population}})
    success('Computed population for Lyon')


# Need to be the last processed
@town.postprocessor()
def attach_counties_to_subcountries(db, filename):
    info('Attaching French Metropolitan counties')
    ids = ['fr/departement/{0}' .format(c) for c in FR_METRO_COUNTIES]
    result = db.update_many(
        {'$or': [{'_id': {'$in': ids}}, {'parents': {'$in': ids}}]},
        {'$addToSet': {'parents': 'country-subset/fr/metro'}}
    )
    success('Attached {0} French Metropolitan children', result.modified_count)

    info('Attaching French DOM counties')
    ids = ['fr/departement/{0}' .format(c) for c in FR_DOM_COUNTIES]
    result = db.update_many(
        {'$or': [{'_id': {'$in': ids}}, {'parents': {'$in': ids}}]},
        {'$addToSet': {'parents': 'country-subset/fr/dom'}}
    )
    success('Attached {0} French DOM children', result.modified_count)

    info('Attaching French DOM/TOM counties')
    ids = ['fr/departement/{0}' .format(c) for c in FR_DOMTOM_COUNTIES]
    result = db.update_many(
        {'$or': [{'_id': {'$in': ids}}, {'parents': {'$in': ids}}]},
        {'$addToSet': {'parents': 'country-subset/fr/domtom'}}
    )
    success('Attached {0} French DOM/TOM children', result.modified_count)


@canton.postprocessor()
def attach_canton_parents(db, filename):
    info('Attaching French Canton to their parents')
    canton_processed = 0
    for zone in db.find({'level': canton.id}):
        candidates_ids = [p for p in zone['parents']
                          if p.startswith(county.id)]
        if len(candidates_ids) < 1:
            warning('No parent candidate found for: {0}', zone['_id'])
            continue
        county_id = candidates_ids[0]
        county_zone = db.find_one({'_id': county_id})
        ops = {
            '$addToSet': {'parents': {'$each': county_zone['parents']}},
            '$unset': {'_dep': 1}
        }
        if db.find_one_and_update({'_id': zone['_id']}, ops):
            canton_processed += 1

    success('Attached {0} french cantons to their parents', canton_processed)


@iris.postprocessor()
def attach_and_clean_iris(db, filename):
    info('Attaching French IRIS to their region')
    processed = 0
    for zone in db.find({'level': iris.id}):
        candidates_ids = [p for p in zone['parents'] if p.startswith(town.id)]
        if len(candidates_ids) < 1:
            warning('No parent candidate found for: {0}', zone['_id'])
            continue
        town_id = candidates_ids[0]
        town_zone = db.find_one({'_id': town_id})
        if not town_zone:
            warning('Town {0} not found', town_id)
            continue
        if zone.get('_type') == 'Z':
            name = town_zone['name']
        else:
            name = ''.join((town_zone['name'], ' (', zone['name'], ')'))
        ops = {
            '$addToSet': {'parents': {'$each': town_zone['parents']}},
            '$set': {'name': name},
            '$unset': {'_town': 1, '_type': 1}
        }
        if db.find_one_and_update({'_id': zone['_id']}, ops):
            processed += 1
    success('Attached {0} french IRIS to their parents', processed)


@district.postprocessor()
def compute_district_population(db, filename):
    info('Computing french district population by aggregation')
    processed = 0
    pipeline = [
        {'$match': {'level': town.id}},
        {'$unwind': '$parents'},
        {'$match': {'parents': {'$regex': district.id}}},
        {'$group': {'_id': '$parents', 'population': {'$sum': '$population'}}}
    ]
    for result in db.aggregate(pipeline):
        if result.get('population'):
            if db.find_one_and_update(
                    {'_id': result['_id']},
                    {'$set': {'population': result['population']}}):
                processed += 1
    success('Computed population for {0} french districts', processed)


@county.postprocessor()
def compute_county_area_and_population(db, filename):
    info('Computing french counties areas and population by aggregation')
    processed = 0
    pipeline = [
        {'$match': {'level': town.id}},
        {'$unwind': '$parents'},
        {'$match': {'parents': {'$regex': county.id}}},
        {'$group': {
            '_id': '$parents',
            'area': {'$sum': '$area'},
            'population': {'$sum': '$population'}
        }}
    ]
    for result in db.aggregate(pipeline):
        if db.find_one_and_update(
                {'_id': result['_id']},
                {'$set': {
                    'area': result['area'],
                    'population': result['population']
                }}):
            processed += 1
    success('Computed area and population for {0} french counties', processed)


@region.postprocessor()
def compute_region_population(db, filename):
    info('Computing french regions population by aggregation')
    processed = 0
    pipeline = [
        {'$match': {'level': town.id}},
        {'$unwind': '$parents'},
        {'$match': {'parents': {'$regex': region.id}}},
        {'$group': {'_id': '$parents', 'population': {'$sum': '$population'}}}
    ]
    for result in db.aggregate(pipeline):
        if result.get('population'):
            if db.find_one_and_update(
                    {'_id': result['_id']},
                    {'$set': {'population': result['population']}}):
                processed += 1
    success('Computed population for {0} french regions', processed)
