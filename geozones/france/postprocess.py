from collections import Counter
from itertools import groupby

from pymongo import DESCENDING

from ..dbpedia import execute_sparql_query, dbpedia_to_wikipedia
from ..dbpedia import RDF_PREFIXES, SPARQL_PREFIXES, SPARQL_BY_URI
from ..tools import info, success, warning, progress, error
from ..tools import aggregate_multipolygons, geom_to_multipolygon, chunker

from .model import canton, departement, epci, commune, arrondissement, iris, region, collectivite
from .model import droms, departements_metropole
from .model import PARIS_DISTRICTS, LYON_DISTRICTS, MARSEILLE_DISTRICTS

'''
WARNING:
    In this file, order matters. First declared is first processed (by level)
'''


@commune.postprocessor(
    'http://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true',
    filename='laposte-hexasmal.csv', delimiter=';', encoding='cp1252'
)
def process_postal_codes(db, data):
    '''
    Extract postal codes from:
    https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/
    '''
    processed = 0
    for row in progress(data, 'Processing french postal codes'):
        match = db.find_one_and_update(
            {'level': commune.id, 'code': row['Code_commune_INSEE']},
            {'$addToSet': {'keys.postal': row['Code_postal']}}
        )
        if match:
            processed += 1
    success('Processed {0} french postal codes', processed)


def _get_parent(db, level, row, field, date):
    code = row
    for part in field.split('.'):
        if not code.get(part):
            return
        code = code[part]
    code = code.lower()
    parent = db.zone(level.id, code, date)
    if parent:
        return parent['_id']
    else:
        error('Unable to find zone {0}:{1}@{2}', level.id, code, date)


@commune.postprocessor('https://github.com/etalab/decoupage-administratif/releases/download/v0.5.0/communes.json')
def attach_current_french_communes_parents(db, data):
    processed = 0
    now = '2019-01-01'
    for row in progress(data, 'Updating current french communes metadata'):
        parents = [
            id for id in (
                _get_parent(db, level, row, field, now)
                for level, field in (
                    (region, 'region'),
                    (departement, 'departement'),
                    (arrondissement, 'arrondissement'),
                    (collectivite, 'collectiviteOutremer.code')
                )
            ) if id
        ]
        ops = {}
        if parents:
            ops['$addToSet'] = {'parents': {'$each': parents}}
        if row.get('population'):
            ops['$set'] = {'population': row['population']}
        if not ops:
            continue
        zone = db.update_zone(commune.id, row['code'], '2019-01-01', ops)
        if zone:
            processed += 1
    success('Updated {0} french communes', processed)


# @commune.postprocessor('https://www.insee.fr/fr/statistiques/fichier/2666684/france2017-txt.zip')  # NOQA
# def process_insee_cog(db, filename):
#     '''Use informations from INSEE COG to attach parents.
#     https://www.insee.fr/fr/information/2666684
#     '''
#     info('Processing INSEE COG')
#     processed = 0
#     districts = {}
#     for row in iter_over_cog(filename, 'France2017.txt'):
#         region_code = row['REG']
#         departement_code = row['DEP']
#         district_code = row['AR']
#         region = retrieve_current_region(db, region_code)
#         departement = retrieve_current_departement(db, departement_code)

#         if district_code:
#             district_code = ''.join((departement_code, district_code))
#             district_id = 'fr:arrondissement:{0}'.format(district_code)
#             if district_id not in districts and region and departement:
#                 districts[district_id] = [region['_id'], departement['_id']]

#     for district_id, parents in districts.items():
#         if db.find_one_and_update(
#                 {'_id': district_id},
#                 {'$addToSet': {
#                     'parents': {'$each': parents},
#                 }}):
#             processed += 1
#     success('Attached {0} french districts to their parents', processed)


@commune.postprocessor()
def commune_with_districts(db):
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


# Properties to retrieve from DBPedia
# for French zones having a DBPedia URI
URI_RDF_PROPERTIES_FR = (
    'fr:logo',
    'fr:blason',
    'fr:superficie',
    'fr:population',
    'o:populationTotal',
    'o:area',
    'o:blazon',
    'o:flag',
    'o:emblem',
)

# Number of zones to retrieve by SPARQL query
SPARQL_CHUNK_SIZE = 150


def reduce_uri_row(row):
    prop = row['property']['value']
    for prefix, url in RDF_PREFIXES.items():
        prop = prop.replace(url, prefix)
    return {
        'uri': row['uri']['value'],
        'property': prop,
        'value': row['value']['value'],
    }


def group_by_uri(results):
    reduced = map(reduce_uri_row, results)
    ordered = sorted(reduced, key=lambda r: r['uri'])
    for uri, rows in groupby(ordered, lambda r: r['uri']):
        yield uri, dict(
            (row['property'], row['value']) for row in rows
        )


@commune.postprocessor()
def fetch_missing_data_from_dbpedia(db):
    info('Fetching DBPedia data')
    count = Counter()
    pipeline = [
        {'$match': {
            'level': commune.id,
            'dbpedia': {'$ne': None},
            '$or': [
                {'population': None},
                {'area': None},
                {'flag': None, 'blazon': None, 'logo': None},
            ]
        }},
        {'$group': {'_id': '$dbpedia'}},  # Groups URIs together
    ]

    uris = map(lambda r: r['_id'], db.aggregate_with_progress(pipeline))

    for uris in chunker(uris, SPARQL_CHUNK_SIZE):

        query = SPARQL_BY_URI.format(
            prefixes=SPARQL_PREFIXES,
            uris=' '.join(map(lambda u: '<{0}>'.format(u), uris)),
            properties=' '.join(URI_RDF_PROPERTIES_FR)
        )

        for uri, data in group_by_uri(execute_sparql_query(query)):
            # DBPedia URI, logo, flag, blazon are (almost) valid at any time
            # and so we update every occurence once
            count['uri'] += 1
            metadata = {}
            if 'fr:logo' in data:
                metadata['logo'] = data['fr:logo'].replace(' ', '_')
            if 'o:flag' in data:
                metadata['flag'] = data['o:flag'].replace(' ', '_')
            if 'o:blazon' in data:
                metadata['blazon'] = data['o:blazon'].replace(' ', '_')

            if metadata:
                result = db.update_many({'dbpedia': uri}, {'$set': metadata})
                count['logos'] += result.modified_count

            # Population and area are only valid on latest zones
            # We want population and/or area from French DBPedia or
            # their international counterparts as fallbacks.
            metadata = {}
            if 'fr:population' in data or 'o:populationTotal' in data:
                population = data.get('fr:population',
                                      data.get('o:populationTotal'))
                if population:
                    try:
                        metadata['population'] = int(population)
                    except ValueError:
                        pass
            if 'fr:superficie' in data or 'o:area' in data:
                area = data.get('fr:superficie', data.get('o:area'))
                if area:
                    try:
                        metadata['area'] = int(round(float(area)))
                    except ValueError:
                        pass
            if metadata:
                # Set metadata on latest zone
                zone = db.find_one({'dbpedia': uri},
                                   sort=[('validity.end', DESCENDING)])
                result = db.update_one({'_id': zone['_id']},
                                       {'$set': metadata})
                count['population'] += 1

    success('Processed data from {0} DBPedia URIs', count['uri'])
    success('Updated logos on {0} zones', count['logos'])
    success('Updated area and/orpopulation on {0} zones', count['population'])


@commune.postprocessor()
def compute_commune_with_districts_population(db):
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
def attach_counties_to_subcountries(db):
    info('Attaching French Metropolitan counties')
    ids = [departement['_id'] for departement in departements_metropole(db)]
    result = db.update_many(
        {'$or': [{'_id': {'$in': ids}}, {'parents': {'$in': ids}}]},
        {'$addToSet': {'parents': 'country-subset:fr:metro'}}
    )
    success('Attached {0} French Metropolitan children', result.modified_count)

    info('Attaching French DROM counties')
    ids = [departement['_id'] for departement in droms(db)]
    result = db.update_many(
        {'$or': [{'_id': {'$in': ids}}, {'parents': {'$in': ids}}]},
        {'$addToSet': {'parents': 'country-subset:fr:drom'}}
    )
    success('Attached {0} French DROM children', result.modified_count)


@canton.postprocessor()
def attach_canton_parents(db):
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
def attach_and_clean_iris(db):
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


@arrondissement.postprocessor()
def compute_district_population(db):
    info('Computing french district population by aggregation')
    processed = 0
    pipeline = [
        {'$match': {'level': commune.id}},
        {'$unwind': '$parents'},
        {'$match': {'parents': {'$regex': arrondissement.id}}},
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
def compute_departement_area_and_population(db):
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


@departement.postprocessor()  # Needs departement population to be computed
def compute_region_population(db):
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


@commune.postprocessor()  # Needs communes population, geometry and area to be processed
def attach_epci(db):
    '''
    Attach EPCI towns to their EPCI from
    and build EPCI geometry when available
    '''
    info('Processing EPCI town list')
    count = Counter()
    for zone in progress(db.find({'level': epci.id}), 'Attaching EPCIs members'):
        towns = [
            db.zone(commune.id, code.lower().zfill(5), zone['validity']['start'])
            for code in zone['_towns']
        ]
        ids = [t['_id'] for t in towns]

        # Attach EPCI as towns parent
        result = db.update_many(
            {'_id': {'$in': ids}},
            {'$addToSet': {'parents': zone['_id']}})
        count['processed'] += result.modified_count

        # Try to construct geometries and compute areas
        geoms, areas = zip(*[
            (t.get('geom'), t.get('area'))
            for t in db.find({'_id': {'$in': ids}})
        ])
        if all(geoms):
            try:
                polygons = [geom_to_multipolygon(geom) for geom in geoms]
                aggregated = aggregate_multipolygons(polygons)
                zone['geom'] = aggregated.__geo_interface__
                db.find_one_and_replace({'_id': zone['_id']}, zone)
                count['geometries'] += 1
            except Exception as e:
                warning('Unable to process geometry for "{0}": {1}',
                        zone['_id'], str(e))
        if all(areas):
            try:
                area = sum(areas)
                db.update_one({'_id': zone['_id']}, {'$set': {'area': area}})
                count['areas'] += 1
            except Exception as e:
                warning('Unable to process area for "{0}": {1}',
                        zone['_id'], str(e))

    success('Attached {0} french towns to their EPCI', count['processed'])
    success('Constructed {0} french EPCI geometry', count['geometries'])
    success('Computed {0} french EPCI areas', count['areas'])


SIREN_RDF_PROPERTIES = (
    'fr:logo',
    'fr:blason',
    'o:area',
    'o:blazon',
    'o:flag',
    'o:emblem',
)

SPARQL_BY_SIREN = '''{prefixes}
SELECT ?uri ?siren ?property ?value WHERE {{
    VALUES ?siren {{ {sirens} }}
    VALUES ?property {{ {properties} }}
    ?uri fr:siren ?siren ;
             ?property ?value.
}}
'''


def reduce_siren_row(row):
    prop = row['property']['value']
    for prefix, url in RDF_PREFIXES.items():
        prop = prop.replace(url, prefix)
    return {
        'uri': row['uri']['value'],
        'siren': row['siren']['value'],
        'property': prop,
        'value': row['value']['value'],
    }


def group_by_siren(results):
    reduced = map(reduce_siren_row, results)
    ordered = sorted(reduced, key=lambda r: r['siren'])
    for (siren, uri), rows in groupby(ordered,
                                      lambda r: (r['siren'], r['uri'])):
        yield siren, uri, dict(
            (row['property'], row['value']) for row in rows
        )


@epci.postprocessor()
def fetch_epci_data_from_dbpedia(db):
    info('Fetching EPCI DBPedia data from their SIREN')
    count = Counter()
    # Aggregate and iter over EPCI's SIRENs
    pipeline = [
        {'$match': {'level': 'fr:epci'}},  # Only takes EPCI
        {'$group': {'_id': '$code'}},     # Groups SIREN together
    ]

    sirens = map(lambda r: r['_id'], db.aggregate_with_progress(pipeline))

    for sirens in chunker(sirens, SPARQL_CHUNK_SIZE):

        query = SPARQL_BY_SIREN.format(
            prefixes=SPARQL_PREFIXES,
            sirens=' '.join(map(
                lambda s: '"{0}"^^xsd:integer'.format(s),
                sirens
            )),
            properties=' '.join(SIREN_RDF_PROPERTIES)
        )

        for siren, uri, data in group_by_siren(execute_sparql_query(query)):
            # DBPedia URI, logo, flag, blazon are (almost) valid at any time
            metadata = {'dbpedia': uri, 'wikipedia': dbpedia_to_wikipedia(uri)}
            if 'fr:logo' in data:
                metadata['logo'] = data['fr:logo'].replace(' ', '_')
            if 'o:flag' in data:
                metadata['flag'] = data['o:flag'].replace(' ', '_')
            if 'o:blazon' in data:
                metadata['blazon'] = data['o:blazon'].replace(' ', '_')

            result = db.update_many({'code': siren}, {'$set': metadata})
            count['siren'] += 1
            count['processed'] += result.modified_count

    success('Updated {0[processed]} EPCIs from DBPedia '
            'using {0[siren]} SIREN numbers', count)
