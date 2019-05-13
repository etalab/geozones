from collections import Counter

from .. import wiki
from ..model import country_subset
from ..tools import info, success, warning, error, progress
from ..tools import aggregate_multipolygons, geom_to_multipolygon, chunker

from .model import canton, departement, epci, commune, arrondissement, iris, region, collectivite
from .model import droms, departements_metropole, decoupage_etalab
from .model import PARIS_DISTRICTS, LYON_DISTRICTS, MARSEILLE_DISTRICTS, WIKIDATA_FLAG_OF_FRANCE
from .model import COMMUNES_START

'''
WARNING:
    In this file, order matters. First declared is first processed (by level)
'''


COUNTRY_SUBSETS_SPARQL = '''
SELECT DISTINCT ?subset ?subsetLabel ?population ?area ?siren ?geonames ?flag
                ?blazon ?logo ?site ?wikipedia ?osm
WHERE {
  VALUES ?subset { {ids} }
  OPTIONAL {?subset wdt:P1566 ?geonames.}
  OPTIONAL {?subset wdt:P1616 ?siren.}
  OPTIONAL {?subset wdt:P2046 ?area.}
  OPTIONAL {?subset wdt:P1082 ?population.}
  OPTIONAL {?subset wdt:P41 ?flag. FILTER (?flag != <http://commons.wikimedia.org/wiki/Special:FilePath/Flag%20of%20France.svg>)}
  OPTIONAL {?subset wdt:P94 ?blazon.}
  OPTIONAL {?subset wdt:P154 ?logo.}
  OPTIONAL {?subset wdt:P856 ?site.}
  OPTIONAL {?subset wdt:P402 ?osm.}

  OPTIONAL {
    ?wikipedia schema:about ?subset;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }

  SERVICE wikibase:label { bd:serviceParam wikibase:language 'fr'. }
}
'''  # NOQA: E501


@country_subset.postprocessor()
def fetch_french_country_subset_wikidata_metadata(db):
    info('Fetching french country subsets wikidata metadata')
    subsets = list(db.level(country_subset.id, code={'$regex': 'fr:.+'}, wikidata={'$exists': True}))
    ids = {subset['wikidata']: subset['_id'] for subset in subsets}

    wdids = ' '.join(f'wd:{id}' for id in ids.keys())
    query = COUNTRY_SUBSETS_SPARQL.replace('{ids}', wdids)
    results = wiki.data_sparql_query(query)
    results = wiki.data_reduce_result(results, 'subset')

    for row in results:
        uri = row['subset']
        wdid = wiki.data_uri_to_id(uri)
        zone_id = ids.get(wdid)
        if zone_id:
            db.find_one_and_update({'_id': zone_id}, {
                '$set': {k: v for k, v in {
                    'wikidata': wdid,
                    'wikipedia': wiki.wikipedia_url_to_id(row['wikipedia']),
                    'dbpedia': wiki.wikipedia_to_dbpedia(row['wikipedia']),
                    'website': row.get('site'),
                    'flag': wiki.media_url_to_path(row.get('flag')),
                    'area': float(row.get('area', 0)) or None,
                    'population': int(row.get('population', 0)) or None,
                    'keys.osm': row.get('osm'),
                    'keys.geonames': row.get('geonames'),
                }.items() if v is not None}
            })


@commune.postprocessor('https://unpkg.com/codes-postaux@3.2.0/codes-postaux-full.json')
def fr_postal_codes(db, data):
    '''
    Extract postal codes from:
    https://www.data.gouv.fr/fr/datasets/codes-postaux/

    codePostal : code postal
    libelleAcheminement : libellé d'acheminement (en majuscules non accentuées à la norme postale)
    codeCommune : code INSEE de la commune
    nomCommune : nom de la commune
    Liste des champs supplémentaires (full)
    Tous les champs suivants sont optionnels.

    codeVoie : code FANTOIR de la voie concernée
    codeAncienneCommune : code INSEE de l'ancienne commune à laquelle est rattaché le codeVoie
    codeParite : code de parité (0 pour numéros pairs, 1 pour numéros impairs)
    borneInferieure : plus petit numéro du tronçon
    borneSuperieure : plus grand numéro du tronçon
    '''
    processed = 0
    for row in progress(data, 'Processing french postal code'):
        match = db.update_zone(commune.id, row['codeCommune'], ops={
            '$addToSet': {'keys.postal': row['codePostal']}
        })
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


@commune.postprocessor(decoupage_etalab('v0.5.0', 'communes'))
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


@commune.postprocessor()
def commune_with_districts(db):
    info('Attaching Paris town districts')
    paris = db.find_one({'_id': 'fr:commune:75056@{0}'.format(COMMUNES_START)})
    parents = paris['parents']
    parents.append(paris['_id'])
    result = db.update_many(
        {'_id': {'$in': PARIS_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Paris', result.modified_count)

    info('Attaching Marseille town districts')
    marseille = db.find_one({'_id': 'fr:commune:13055@{0}'.format(COMMUNES_START)})
    parents = marseille['parents']
    parents.append(marseille['_id'])
    result = db.update_many(
        {'_id': {'$in': MARSEILLE_DISTRICTS}},
        {'$addToSet': {'parents': {'$each': parents}}})
    success('Attached {0} districts to Marseille', result.modified_count)

    info('Attaching Lyon town districts')
    lyon = db.find_one({'_id': 'fr:commune:69123@{0}'.format(COMMUNES_START)})
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


COMMUNES_SPARQL_QUERY = f'''
SELECT DISTINCT ?commune ?insee ?population ?area ?siren ?geonames ?flag
                ?blazon ?logo ?site ?wikipedia ?osm
WHERE {{
  VALUES ?insee {{ {{codes}} }}
  ?commune wdt:P374 ?insee
  OPTIONAL {{?commune wdt:P1566 ?geonames.}}
  OPTIONAL {{?commune wdt:P1616 ?siren.}}
  OPTIONAL {{?commune wdt:P2046 ?area.}}
  OPTIONAL {{?commune wdt:P1082 ?population.}}
  OPTIONAL {{?commune wdt:P41 ?flag. FILTER (?flag != <{WIKIDATA_FLAG_OF_FRANCE}>)}}
  OPTIONAL {{?commune wdt:P94 ?blazon.}}
  OPTIONAL {{?commune wdt:P154 ?logo.}}
  OPTIONAL {{?commune wdt:P856 ?site.}}
  OPTIONAL {{?commune wdt:P402 ?osm.}}
  OPTIONAL {{
    ?wikipedia schema:about ?commune;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }}
}}
'''


@commune.postprocessor()
def fetch_communes_data_from_wikidata(db):
    info('Fetching french communes metadata from wikidata')
    processed = 0

    pipeline = [
        {'$match': {
            'level': commune.id,
            '$or': [
                {'population': None},
                {'area': None},
                {'flag': None, 'blazon': None, 'logo': None},
            ]
        }},
        {'$group': {'_id': '$code'}},
    ]

    codes = map(lambda r: r['_id'], db.aggregate_with_progress(pipeline))

    for codes in chunker(codes, SPARQL_CHUNK_SIZE):
        query = COMMUNES_SPARQL_QUERY.replace('{codes}', ' '.join('"{}"'.format(s) for s in codes))
        results = wiki.data_sparql_query(query)
        results = wiki.data_reduce_result(results, 'commune')
        for row in results:
            insee = row['insee'].lower()
            db.update_zone(commune.id, insee, db.TODAY,  ops={
                '$set': {k: v for k, v in {
                    'wikidata': wiki.data_uri_to_id(row['commune']),
                    'wikipedia': wiki.wikipedia_url_to_id(row.get('wikipedia')),
                    'dbpedia': wiki.wikipedia_to_dbpedia(row.get('wikipedia')),
                    'website': row.get('site'),
                    'flag': wiki.media_url_to_path(row.get('flag')),
                    'blazon': wiki.media_url_to_path(row.get('blazon')),
                    'logo': wiki.media_url_to_path(row.get('logo')),
                    'area': float(row.get('area', 0)) or None,
                    'population': int(row.get('population', 0)) or None,
                    'keys.osm': row.get('osm'),
                    # Siren have no end-time but is sequential (keep last)
                    'keys.siren': row.get('siren'),
                    'keys.geonames': row.get('geonames'),
                }.items() if v is not None}
            })
            processed += 1

    success('Fetched {0} french communes metadata from wikidata', processed)


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


REGIONS_SPARQL_QUERY = f'''
SELECT DISTINCT ?region ?insee ?capital ?capitalInsee ?population ?area
                ?iso2 ?siren ?fips ?nuts2 ?geonames ?flag ?blazon ?logo
                ?site ?wikipedia ?osm
WHERE {{
  ?region wdt:P31 wd:Q36784;
          wdt:P2585 ?insee;
          wdt:P36 ?capital.
  OPTIONAL {{?capital wdt:P374 ?capitalInsee.}}
  OPTIONAL {{?region p:P300 ?iso2Stmt.
             ?iso2Stmt ps:P300 ?iso2.
             FILTER NOT EXISTS {{ ?iso2Stmt pq:P582 [] }} .
             }}
  OPTIONAL {{?region wdt:P901 ?fips.}}
  OPTIONAL {{?region wdt:P1566 ?geonames.}}
  OPTIONAL {{?region p:P605 ?nutsStmt.
            ?nutsStmt ps:P605 ?nuts2.
            FILTER (regex(?nuts2, '^FR\\\\d{{2}}$')) .
            FILTER NOT EXISTS {{ ?nutsStmt pq:P582 [] }} .
            }}
  OPTIONAL {{?region wdt:P1616 ?siren.}}
  OPTIONAL {{?region wdt:P2046 ?area.}}
  OPTIONAL {{?region wdt:P1082 ?population.}}
  OPTIONAL {{?region wdt:P41 ?flag. FILTER (?flag != <{WIKIDATA_FLAG_OF_FRANCE}>)}}
  OPTIONAL {{?region wdt:P94 ?blazon.}}
  OPTIONAL {{?region wdt:P154 ?logo.}}
  OPTIONAL {{?region wdt:P856 ?site.}}
  OPTIONAL {{?region wdt:P402 ?osm.}}
  OPTIONAL {{
    ?wikipedia schema:about ?region;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }}
}}
'''


@region.postprocessor()
def fetch_region_data_from_wikidata(db):
    info('Fetching french regions wikidata metadata')
    results = wiki.data_sparql_query(REGIONS_SPARQL_QUERY)
    results = wiki.data_reduce_result(results, 'region', 'geonames', 'siren')
    for row in progress(results):
        insee = row['insee'].lower()
        db.update_zone(region.id, insee, db.TODAY,  ops={
            '$set': {k: v for k, v in {
                'wikidata': wiki.data_uri_to_id(row['region']),
                'wikipedia': wiki.wikipedia_url_to_id(row['wikipedia']),
                'dbpedia': wiki.wikipedia_to_dbpedia(row['wikipedia']),
                'website': row.get('site'),
                'flag': wiki.media_url_to_path(row.get('flag')),
                'blazon': wiki.media_url_to_path(row.get('blazon')),
                'logo': wiki.media_url_to_path(row.get('logo')),
                'area': float(row['area']),
                'population': int(row['population']),
                'keys.iso2': row.get('iso2', '').lower() or None,
                'keys.nuts2': row.get('nuts2', '').lower() or None,
                'keys.osm': row.get('osm'),
                'keys.fips': row.get('fips', '').lower() or None,
                # Siren have no end-time but is sequential (keep last)
                'keys.siren': next((s for s in sorted(row['siren'], reverse=True)), None),
                'keys.geonames': row['geonames'],
            }.items() if v is not None}
        })


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


DEPARTEMENT_SPARQL_QUERY = f'''
SELECT DISTINCT ?dpt ?dptLabel ?insee ?capital ?capitalInsee ?population ?area
                ?iso2 ?siren ?fips ?nuts3 ?geonames ?flag ?blazon ?logo
                ?site ?wikipedia ?osm
WHERE {{
  ?dpt wdt:P31 wd:Q6465;
       wdt:P2586 ?insee;
       wdt:P36 ?capital.
  OPTIONAL {{?capital wdt:P374 ?capitalInsee.}}
  OPTIONAL {{?dpt p:P300 ?iso2Stmt.
             ?iso2Stmt ps:P300 ?iso2.
             FILTER NOT EXISTS {{ ?iso2Stmt pq:P582 [] }} .
             }}
  OPTIONAL {{?dpt wdt:P901 ?fips.}}
  OPTIONAL {{?dpt wdt:P1566 ?geonames.}}
  OPTIONAL {{?dpt p:P605 ?nutsStmt.
            ?nutsStmt ps:P605 ?nuts3.
            FILTER (regex(?nuts3, '^FR\\\\d{{3}}$')) .
            FILTER NOT EXISTS {{ ?nutsStmt pq:P582 [] }} .
            }}
  OPTIONAL {{?dpt wdt:P1616 ?siren.}}
  OPTIONAL {{?dpt wdt:P2046 ?area.}}
  OPTIONAL {{?dpt wdt:P1082 ?population.}}
  OPTIONAL {{?dpt wdt:P41 ?flag. FILTER (?flag != <{WIKIDATA_FLAG_OF_FRANCE}>)}}
  OPTIONAL {{?dpt wdt:P94 ?blazon.}}
  OPTIONAL {{?dpt wdt:P154 ?logo.}}
  OPTIONAL {{?dpt wdt:P856 ?site.}}
  OPTIONAL {{?dpt wdt:P402 ?osm.}}
  OPTIONAL {{
    ?wikipedia schema:about ?dpt;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language 'fr'. }}
}}
'''


@departement.postprocessor()
def fetch_departement_data_from_wikidata(db):
    info('Fetching french departement wikidata metadata')
    results = wiki.data_sparql_query(DEPARTEMENT_SPARQL_QUERY)
    results = wiki.data_reduce_result(results, 'dpt', 'geonames', 'siren')
    for row in progress(results):
        insee = row['insee'].lower()
        db.update_zone(departement.id, insee, db.TODAY,  ops={
            '$set': {k: v for k, v in {
                'wikidata': wiki.data_uri_to_id(row['dpt']),
                'wikipedia': wiki.wikipedia_url_to_id(row['wikipedia']),
                'dbpedia': wiki.wikipedia_to_dbpedia(row['wikipedia']),
                'website': row.get('site'),
                'flag': wiki.media_url_to_path(row.get('flag')),
                'blazon': wiki.media_url_to_path(row.get('blazon')),
                'logo': wiki.media_url_to_path(row.get('logo')),
                'area': float(row['area']),
                'population': int(row['population']),
                'keys.iso2': row.get('iso2', '').lower() or None,
                'keys.nuts3': row.get('nuts3', '').lower() or None,
                'keys.osm': row.get('osm'),
                'keys.fips': row.get('fips', '').lower() or None,
                # Siren have no end-time but is sequential (keep last)
                'keys.siren': next((s for s in sorted(row['siren'], reverse=True)), None),
                'keys.geonames': row['geonames'],
            }.items() if v is not None}
        })


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


EPCI_SPARQL_QUERY = f'''
SELECT DISTINCT ?epci ?epciLabel ?siren ?population ?area
                ?flag ?blazon ?logo ?site ?wikipedia ?osm
WHERE {{
  VALUES ?siren {{ {{sirens}} }}
  ?epci wdt:P1616 ?siren.
  OPTIONAL {{?epci wdt:P2046 ?area.}}
  OPTIONAL {{?epci wdt:P1082 ?population.}}
  OPTIONAL {{?epci wdt:P41 ?flag. FILTER (?flag != <{WIKIDATA_FLAG_OF_FRANCE}>)}}
  OPTIONAL {{?epci wdt:P94 ?blazon.}}
  OPTIONAL {{?epci wdt:P154 ?logo.}}
  OPTIONAL {{?epci wdt:P856 ?site.}}
  OPTIONAL {{?epci wdt:P402 ?osm.}}
  OPTIONAL {{
    ?wikipedia schema:about ?epci;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language 'fr'. }}
}}
'''


@epci.postprocessor()
def fetch_epci_data_from_wikidata(db):
    info('Fetching french EPCIs wikidata metadata')

    count = Counter()
    # Aggregate and iter over EPCI's SIRENs
    pipeline = [
        {'$match': {'level': epci.id}},  # Only takes EPCI
        {'$group': {'_id': '$code'}},     # Groups SIREN together
    ]

    sirens = map(lambda r: r['_id'], db.aggregate_with_progress(pipeline))

    for sirens in chunker(sirens, SPARQL_CHUNK_SIZE):
        query = EPCI_SPARQL_QUERY.replace('{sirens}', ' '.join('"{}"'.format(s) for s in sirens))
        results = wiki.data_sparql_query(query)
        results = wiki.data_reduce_result(results, 'siren')
        for row in results:
            siren = row['siren']
            r = db.update_zones(epci.id, siren, db.TODAY,  ops={
                '$set': {k: v for k, v in {
                    'wikidata': wiki.data_uri_to_id(row['epci']),
                    'wikipedia': wiki.wikipedia_url_to_id(row.get('wikipedia')),
                    'dbpedia': wiki.wikipedia_to_dbpedia(row.get('wikipedia')),
                    'website': row.get('site'),
                    'flag': wiki.media_url_to_path(row.get('flag')),
                    'blazon': wiki.media_url_to_path(row.get('blazon')),
                    'logo': wiki.media_url_to_path(row.get('logo')),
                    'area': float(row.get('area', 0)) or None,
                    'population': int(row.get('population', 0)) or None,
                    'keys.osm': row.get('osm'),
                }.items() if v is not None}
            })

            count['siren'] += 1
            count['processed'] += r.modified_count

    success('Updated {0[processed]} EPCIs from DBPedia '
            'using {0[siren]} SIREN numbers', count)
