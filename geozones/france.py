import csv

from .francehisto import (
    retrieve_zone, retrieve_current_departement, retrieve_current_region,
    retrieve_current_metro_departements, retrieve_current_drom_departements,
    retrieve_current_collectivite, retrieve_current_collectivites
)
from .model import Level, country, country_subset
from .tools import info, success, warning, unicodify, iter_over_cog
from .dbpedia import DBPedia
from .__main__ import DB


_ = lambda s: s


region = Level('fr:region', _('French region'), 40, country)
epci = Level('fr:epci', _('French intermunicipal (EPCI)'), 68, region)
departement = Level('fr:departement', _('French county'), 60, region)
collectivite = Level('fr:collectivite', _('French overseas collectivities'),
                     60, region)
district = Level('fr:arrondissement', _('French district'), 70, departement)
commune = Level('fr:commune', _('French town'), 80, district, epci)
canton = Level('fr:canton', _('French canton'), 98, departement)

# Not opendata yet
iris = Level('fr:iris', _('Iris (Insee districts)'), 98, commune)

# Cities with districts
PARIS_DISTRICTS = [
    'fr:commune:751{0:0>2}@1942-01-01'.format(i) for i in range(1, 21)]

MARSEILLE_DISTRICTS = [
    'fr:commune:132{0:0>2}@1942-01-01'.format(i) for i in range(1, 17)]

LYON_DISTRICTS = [
    'fr:commune:6938{0}@1942-01-01'.format(i) for i in range(1, 9)]


country_subset.aggregate(
    'fr:metro', _('Metropolitan France'),
    [zone['_id'] for zone in retrieve_current_metro_departements(DB())],
    parents=['country:fr', 'country-group:ue', 'country-group:world'])

country_subset.aggregate(
    'fr:drom', 'DROM',
    [zone['_id'] for zone in retrieve_current_drom_departements(DB())],
    parents=['country:fr', 'country-group:ue', 'country-group:world'])

country_subset.aggregate(
    'fr:dromcom', 'DROM-COM',
    [zone['_id'] for zone in retrieve_current_drom_departements(DB())] +
    [zone['_id'] for zone in retrieve_current_collectivites(DB())],
    parents=['country:fr', 'country-group:ue', 'country-group:world'])


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
        'parents': ['country:fr', 'country-group:ue', 'country-group:world'],
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
        '_id': 'fr:epci:{}'.format(siren),
        'code': siren,
        'name': props.get('nom_osm') and
        unicodify(props.get('nom_osm').encode('latin-1').decode('utf-8')) or
        unicodify(props['nom_epci'].encode('latin-1').decode('utf-8')),
        'population': props['ptot_epci'],
        'area': props['surf_km2'],
        'wikipedia': props['wikipedia'] and unicodify(
            props['wikipedia'].encode('latin-1').decode('utf-8')) or '',
        'parents': ['country:fr', 'country-group:ue', 'country-group:world'],
        'keys': {
            'siren': siren,
            'osm': props['osm_id'],
            'type_epci': props['type_epci']
        }
    }


@collectivite.extractor('http://thematicmapping.org/downloads/TM_WORLD_BORDERS-0.3.zip')  # NOQA
def extract_overseas_collectivities(db, polygon):
    '''
    Extract overseas collectivities from their WorldBorder country.
    Based on data from http://thematicmapping.org/downloads/world_borders.php
    '''
    props = polygon['properties']
    iso2 = props['ISO2'].lower()
    collectivite = retrieve_current_collectivite(db, iso2)
    if collectivite:
        return {
            '_id': collectivite['_id'],
            'code': collectivite['code'],
            'name': collectivite['name'],
            'population': props['POP2005'],
            'area': int(props['AREA']),
            'parents': ['country:fr', 'country-group:ue',
                        'country-group:world'],
            'keys': {
                'insee': collectivite['code'],
                'iso2': iso2,
                'iso3': props['ISO3'].lower(),
                'un': props['UN'],
            }
        }


@departement.extractor('https://www.data.gouv.fr/s/resources/contours-des-departements-francais-issus-d-openstreetmap/20170614-200948/departements-20170102-simplified.zip')  # NOQA
def extract_french_departement(db, polygon):
    '''
    Extract a french departement informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/contours-des-departements-francais-issus-d-openstreetmap/
    '''
    props = polygon['properties']
    code_insee = props['code_insee']
    if code_insee == '69D':  # Not handled at the geohisto level (yet?).
        code_insee = '69'
    zone = retrieve_zone(db, departement.id, code_insee, after='2016-01-01')
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


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20170111-shp.zip', simplify=0.0005)  # NOQA
def extract_2017_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, commune.id, props['insee'], '9999-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_ha'])
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20160119-shp.zip', simplify=0.0005)  # NOQA
def extract_2016_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, commune.id, props['insee'], '2016-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_ha'])
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20150101-100m-shp.zip')  # NOQA
def extract_2015_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, commune.id, props['insee'], '2015-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_m2']) / 10**6
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20131220-100m-shp.zip')  # NOQA
def extract_2014_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, commune.id, props['insee'], '2014-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_m2']) / 10**6
    zone['wikipedia'] = (props['wikipedia'] and
                         props['wikipedia'].encode('latin-1').decode('utf-8'))
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/arrondissements-municipaux-20160128-shp.zip')  # NOQA
def extract_french_arrondissements(db, polygon):
    '''
    Extract a french arrondissements informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = retrieve_zone(db, commune.id, props['insee'], '9999-12-31')
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
    parents = ['country:fr', 'country-group:ue', 'country-group:world']
    departement = retrieve_current_departement(db, props['dep'])
    if departement:
        parents.append(departement['_id'])
    return {
        'code': code,
        'name': unicodify(props['nom'].encode('latin-1').decode('utf-8')),
        'population': props['population'],
        'wikipedia': props['wikipedia'] and unicodify(
            props['wikipedia'].encode('latin-1').decode('utf-8')),
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
    parents = ['country:fr', 'country-group:ue', 'country-group:world']
    commune = retrieve_zone(
        db, 'fr:commune', props['DEPCOM'], after='2013-01-01')
    if commune:
        parents.append(commune['_id'])
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


@commune.postprocessor('http://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true')  # NOQA
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
            if db.find_one_and_update({
                'level': commune.id, 'code': insee
            }, ops):
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
    with open(filename, encoding='cp1252') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            siren = row['siren_epci']
            insee = row['insee'].lower()
            epci_id = 'fr:epci:{0}'.format(siren)
            if db.find_one_and_update(
                    {'level': commune.id, 'code': insee},
                    {'$addToSet': {'parents': epci_id}}):
                processed += 1
    success('Attached {0} french towns to their EPCI', processed)


@commune.postprocessor('https://www.insee.fr/fr/statistiques/fichier/2666684/france2017-txt.zip')  # NOQA
def process_insee_cog(db, filename):
    '''Use informations from INSEE COG to attach parents.
    https://www.insee.fr/fr/information/2666684
    '''
    info('Processing INSEE COG')
    processed = 0
    districts = {}
    for row in iter_over_cog(filename, 'France2017.txt'):
        region_code = row['REG']
        departement_code = row['DEP']
        district_code = row['AR']
        region = retrieve_current_region(db, region_code)
        departement = retrieve_current_departement(db, departement_code)

        if district_code:
            district_code = ''.join((departement_code, district_code))
            district_id = 'fr:arrondissement:{0}'.format(district_code)
            if district_id not in districts and region and departement:
                districts[district_id] = [region['_id'], departement['_id']]

    for district_id, parents in districts.items():
        if db.find_one_and_update(
                {'_id': district_id},
                {'$addToSet': {
                    'parents': {'$each': parents},
                }}):
            processed += 1
    success('Attached {0} french districts to their parents', processed)


@commune.postprocessor()
def commune_with_districts(db, filename):
    info('Attaching Paris town districts')
    paris = db.find_one({'_id': 'fr:commune:75056@1942-01-01'})
    parents = paris['parents']
    parents.append(paris['_id'])
    result = db.update_many(
        {'_id': {'$in': PARIS_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Paris', result.modified_count)

    info('Attaching Marseille town districts')
    marseille = db.find_one({'_id': 'fr:commune:13055@1942-01-01'})
    parents = marseille['parents']
    parents.append(marseille['_id'])
    result = db.update_many(
        {'_id': {'$in': MARSEILLE_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Marseille', result.modified_count)

    info('Attaching Lyon town districts')
    lyon = db.find_one({'_id': 'fr:commune:69123@1942-01-01'})
    parents = lyon['parents']
    parents.append(lyon['_id'])
    result = db.update_many(
        {'_id': {'$in': LYON_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Lyon', result.modified_count)


@commune.postprocessor()
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


@commune.postprocessor()
def compute_commune_with_districts_population(db, filename):
    info('Computing Paris town districts population')
    districts = db.find({'_id': {'$in': PARIS_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'fr:commune:75056@1942-01-01'},
        {'$set': {'population': population}})
    success('Computed population for Paris')

    info('Computing Marseille town districts population')
    districts = db.find({'_id': {'$in': MARSEILLE_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'fr:commune:13055@1942-01-01'},
        {'$set': {'population': population}})
    success('Computed population for Marseille')

    info('Computing Lyon town districts population')
    districts = db.find({'_id': {'$in': LYON_DISTRICTS}})
    population = sum(district.get('population', 0) for district in districts)
    db.find_one_and_update(
        {'_id': 'fr:commune:69123@1942-01-01'},
        {'$set': {'population': population}})
    success('Computed population for Lyon')


# Need to be the last processed
@commune.postprocessor()
def attach_counties_to_subcountries(db, filename):
    info('Attaching French Metropolitan counties')
    ids = [departement['_id']
           for departement in retrieve_current_metro_departements(db)]
    result = db.update_many(
        {'$or': [{'_id': {'$in': ids}}, {'parents': {'$in': ids}}]},
        {'$addToSet': {'parents': 'country-subset:fr:metro'}}
    )
    success('Attached {0} French Metropolitan children', result.modified_count)

    info('Attaching French DROM counties')
    ids = [departement['_id']
           for departement in retrieve_current_drom_departements(db)]
    result = db.update_many(
        {'$or': [{'_id': {'$in': ids}}, {'parents': {'$in': ids}}]},
        {'$addToSet': {'parents': 'country-subset:fr:drom'}}
    )
    success('Attached {0} French DROM children', result.modified_count)


@canton.postprocessor()
def attach_canton_parents(db, filename):
    info('Attaching French Canton to their parents')
    canton_processed = 0
    for zone in db.find({'level': canton.id}):
        candidates_ids = [p for p in zone['parents']
                          if p.startswith('fr:departement:')]
        if len(candidates_ids) < 1:
            warning('No parent candidate found for: {0}', zone['_id'])
            continue
        departement_id = candidates_ids[0]
        departement_zone = db.find_one({'_id': departement_id})
        if not departement_zone:
            warning('No county found for: {0}', departement_id)
            continue
        ops = {
            '$addToSet': {'parents': {'$each': departement_zone['parents']}},
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
        candidates_ids = [p for p in zone['parents']
                          if p.startswith('fr:commune:')]
        if len(candidates_ids) < 1:
            warning('No parent candidate found for: {0}', zone['_id'])
            continue
        commune_id = candidates_ids[0]
        commune_zone = db.find_one({'_id': commune_id})
        if not commune_zone:
            warning('Town {0} not found', commune_id)
            continue
        if zone.get('_type') == 'Z':
            name = commune_zone['name']
        else:
            name = ''.join((commune_zone['name'], ' (', zone['name'], ')'))
        ops = {
            '$addToSet': {'parents': {'$each': commune_zone['parents']}},
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
        {'$match': {'level': commune.id}},
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


@departement.postprocessor()
def compute_departement_area_and_population(db, filename):
    info('Computing french counties areas and population by aggregation')
    processed = 0
    pipeline = [
        {'$match': {'level': commune.id}},
        {'$unwind': '$parents'},
        {'$match': {'parents': {'$regex': 'fr:departement'}}},
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
        {'$match': {'level': departement.id}},
        {'$unwind': '$parents'},
        {'$match': {'parents': {'$regex': 'fr:region'}}},
        {'$group': {'_id': '$parents', 'population': {'$sum': '$population'}}}
    ]
    for result in db.aggregate(pipeline):
        if result.get('population'):
            if db.find_one_and_update(
                    {'_id': result['_id']},
                    {'$set': {'population': result['population']}}):
                processed += 1
    success('Computed population for {0} french regions', processed)
