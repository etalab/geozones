import csv
import io
from zipfile import ZipFile

from francehisto import (
    retrieve_zone, retrieve_current_counties, retrieve_current_county,
    retrieve_current_region
)
from geo import Level, country
from tools import info, success, warning, unicodify
from dbpedia import DBPedia


_ = lambda s: s


region = Level('fr/region', _('French region'), country)
epci = Level('fr/epci', _('French intermunicipal (EPCI)'), region)
county = Level('fr/departement', _('French county'), region)
district = Level('fr/arrondissement', _('French district'), county)
town = Level('fr/commune', _('French town'), district, epci)
canton = Level('fr/canton', _('French canton'), county)

# Not opendata yet
iris = Level('fr/iris', _('Iris (Insee districts)'), town)

# Cities with districts
PARIS_DISTRICTS = ['COM751{0:0>2}@1942-01-01'.format(i) for i in range(1, 21)]

MARSEILLE_DISTRICTS = [
    'COM132{0:0>2}@1942-01-01'.format(i) for i in range(1, 17)]

LYON_DISTRICTS = ['COM6938{0}@1942-01-01'.format(i) for i in range(1, 9)]

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

FR_DOM_COUNTIES = ('971', '972', '973', '974', '976')

FR_DOMTOM_COUNTIES = (
    '971', '972', '973', '974', '975', '976', '977', '978', '984', '986',
    '987', '988'
)


@district.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/arrondissements-20131220-100m-shp.zip')  # NOQA
def extract_french_district(db, polygon):
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


@epci.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/epci-20150303-100m-shp.zip')  # NOQA
def extract_french_epci(db, polygon):
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


@county.extractor('http://thematicmapping.org/downloads/TM_WORLD_BORDERS-0.3.zip')  # NOQA
def extract_overseas_county(db, polygon):
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


@county.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/departements-20170102-shp.zip', simplify=0.005)  # NOQA
def extract_french_county(db, polygon):
    '''
    Extract a french county informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/contours-des-departements-francais-issus-d-openstreetmap/
    '''
    props = polygon['properties']
    code_insee = props['code_insee']
    if code_insee == '69D':  # Not handled at the geohisto level (yet?).
        code_insee = '69'
    zone = retrieve_zone(db, county.id, code_insee, '9999-12-31')
    if not zone:
        return
    zone['keys']['nuts3'] = props['nuts3']
    zone['wikipedia'] = unicodify(props['wikipedia']
                                  .encode('latin-1').decode('utf-8'))
    return zone


@region.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20140306-100m-shp.zip')  # NOQA
def extract_2014_french_region(db, polygon):
    '''
    Extract a french region informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/contours-des-regions-francaises-sur-openstreetmap/
    '''
    props = polygon['properties']
    return retrieve_zone(db, region.id, props['code_insee'], '2015-12-31')


@region.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20161121-shp.zip', simplify=0.01)  # NOQA
def extract_2016_french_region(db, polygon):
    '''
    Extract new french region informations from a MultiPolygon.
    Based on data from:
    https://www.data.gouv.fr/fr/datasets/projet-de-redecoupages-des-regions/
    '''
    props = polygon['properties']
    return retrieve_zone(db, region.id, props['code_insee'], '9999-12-31')


@town.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20170111-shp.zip', simplify=0.0005)  # NOQA
def extract_2017_french_town(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, town.id, props['insee'], '9999-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_ha'])
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@town.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20160119-shp.zip', simplify=0.0005)  # NOQA
def extract_2016_french_town(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, town.id, props['insee'], '2016-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_ha'])
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@town.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20131220-100m-shp.zip')  # NOQA
def extract_2014_french_town(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, town.id, props['insee'], '2014-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_m2']) / 10**6
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@town.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20150101-100m-shp.zip')  # NOQA
def extract_2015_french_town(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, town.id, props['insee'], '2015-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_m2']) / 10**6
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@town.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/arrondissements-municipaux-20160128-shp.zip')  # NOQA
def extract_french_arrondissements(db, polygon):
    '''
    Extract a french arrondissements informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, town.id, props['insee'], '9999-12-31')
    zone['wikipedia'] = unicodify(props['wikipedia'])
    zone['area'] = int(props['surf_ha'])
    return zone


@canton.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/cantons-2015-shp.zip', simplify=0.005)  # NOQA
def extract_french_canton(db, polygon):
    '''
    Extract a french canton informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/fr/datasets/contours-osm-des-cantons-electoraux-departementaux-2015/
    '''
    props = polygon['properties']
    code = props['ref'].lower()
    parents = ['country/fr', 'country-group/ue', 'country-group/world']
    county = retrieve_current_county(db, props['dep'])
    if county:
        parents.append(county['_id'])
    return {
        'code': code,
        'name': unicodify(props['nom']),
        'population': props['population'],
        'wikipedia': unicodify(props['wikipedia']),
        'parents': parents,
        'keys': {
            'ref': code,
            'jorf': props['jorf']
        }
    }


@iris.extractor('https://www.data.gouv.fr/s/resources/contour-des-iris-insee-tout-en-un/20150428-161348/iris-2013-01-01.zip')  # NOQA
def extract_iris(db, polygon):
    '''
    Extract French IrisBased on data from:
    http://professionnels.ign.fr/contoursiris
    '''
    props = polygon['properties']
    code = props['DCOMIRIS']
    parents = ['country/fr', 'country-group/ue', 'country-group/world']
    town = retrieve_zone(db, 'fr/commune', props['DEPCOM'], after='2013-01-01')
    if town:
        parents.append(town['_id'])
    name = unicodify(props['NOM_IRIS']).title()
    return {
        'code': code,
        'name': name,
        'parents': parents,
        '_type': props['TYP_IRIS'],
        'keys': {
            'iris': code
        },

    }


@town.postprocessor('http://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true')  # NOQA
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
        for insee, _1, postal, _2, _3, _4 in reader:
            ops = {'$addToSet': {'keys.postal': postal}}
            if db.find_one_and_update({'level': town.id, 'code': insee}, ops):
                processed += 1
    success('Processed {0} french postal codes', processed)


@epci.postprocessor('http://www.collectivites-locales.gouv.fr/files/files/epcicom2015.csv')  # NOQA
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


@town.postprocessor('https://www.insee.fr/fr/statistiques/fichier/2114819/france2016-txt.zip')  # NOQA
def process_insee_cog(db, filename):
    '''Use informations from INSEE COG to attach parents.
    http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement.asp
    '''
    info('Processing INSEE COG')
    processed = 0
    districts = {}
    with ZipFile(filename) as cogzip:
        with cogzip.open('france2016.txt') as tsvfile:
            tsvio = io.TextIOWrapper(tsvfile, encoding='cp1252')
            reader = csv.DictReader(tsvio, delimiter='\t')
            for row in reader:
                region_code = row['REG']
                county_code = row['DEP']
                district_code = row['AR']
                region = retrieve_current_region(db, region_code)
                county = retrieve_current_county(db, county_code)

                if district_code:
                    district_code = ''.join((county_code, district_code))
                    district_id = 'fr/arrondissement/{0}'.format(district_code)
                    if district_id not in districts and region and county:
                        districts[district_id] = [region['_id'], county['_id']]

    for district_id, parents in districts.items():
        if db.find_one_and_update(
                {'_id': district_id},
                {'$addToSet': {
                    'parents': {'$each': parents},
                }}):
            processed += 1
    success('Attached {0} french districts to their parents', processed)


@town.postprocessor()
def town_with_districts(db, filename):
    info('Attaching Paris town districts')
    paris = db.find_one({'_id': 'COM75056@1942-01-01'})
    parents = paris['parents']
    parents.append(paris['_id'])
    result = db.update_many(
        {'_id': {'$in': PARIS_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Paris', result.modified_count)

    info('Attaching Marseille town districts')
    marseille = db.find_one({'_id': 'COM13055@1942-01-01'})
    parents = marseille['parents']
    parents.append(marseille['_id'])
    result = db.update_many(
        {'_id': {'$in': MARSEILLE_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Marseille', result.modified_count)

    info('Attaching Lyon town districts')
    lyon = db.find_one({'_id': 'COM69123@1942-01-01'})
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
    query = {
        'wikipedia': {'$exists': True, '$ne': None},
        '$or': [
            {'population': None},
            {'population': {'$exists': False}},
            {'area': None},
            {'area': {'$exists': False}},
            {'flag': None},
            {'flag': {'$exists': False}},
            {'blazon': None},
            {'blazon': {'$exists': False}},
        ]
    }
    for zone in db.find(query, no_cursor_timeout=True):
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
        {'_id': 'COM75056@1942-01-01'},
        {'$set': {'population': population}})
    success('Computed population for Paris')

    info('Computing Marseille town districts population')
    districts = db.find({'_id': {'$in': MARSEILLE_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'COM13055@1942-01-01'},
        {'$set': {'population': population}})
    success('Computed population for Marseille')

    info('Computing Lyon town districts population')
    districts = db.find({'_id': {'$in': LYON_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'COM69123@1942-01-01'},
        {'$set': {'population': population}})
    success('Computed population for Lyon')


# Need to be the last processed
@town.postprocessor()
def attach_counties_to_subcountries(db, filename):
    info('Attaching French Metropolitan counties')
    ids = [county['_id'] for county in retrieve_current_counties(db)]
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
                          if p.startswith('DEP')]
        if len(candidates_ids) < 1:
            warning('No parent candidate found for: {0}', zone['_id'])
            continue
        county_id = candidates_ids[0]
        county_zone = db.find_one({'_id': county_id})
        if not county_zone:
            warning('No county found for: {0}', county_id)
            continue
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
        candidates_ids = [p for p in zone['parents'] if p.startswith('COM')]
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
        {'$match': {'parents': {'$regex': 'DEP'}}},
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
        {'$match': {'level': county.id}},
        {'$unwind': '$parents'},
        {'$match': {'parents': {'$regex': 'REG'}}},
        {'$group': {'_id': '$parents', 'population': {'$sum': '$population'}}}
    ]
    for result in db.aggregate(pipeline):
        if result.get('population'):
            if db.find_one_and_update(
                    {'_id': result['_id']},
                    {'$set': {'population': result['population']}}):
                processed += 1
    success('Computed population for {0} french regions', processed)
