from . import wiki
from .model import Level, country
from .tools import info, progress

_ = lambda s: s  # noqa: E731

district = Level('lu:district', _('Luxembourguish district'), 40, country)
canton = Level('lu:canton', _('Luxembourguish canton'), 60, district)
commune = Level('lu:commune', _('Luxembourguish commune'), 80, canton)

# See: https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
GEOJSON = 'https://download.data.public.lu/resources/limites-administratives-du-grand-duche-de-luxembourg/20180913-143737/lu-limites-administratives-2018.geojson'


@district.extractor(GEOJSON, layer='districts')  # NOQA
def extract_lu_district(db, polygon):
    '''
    Extract a luxembourgish district informations from a MultiPolygon.
    Based on data from:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
    '''
    props = polygon['properties']
    code = props['ISO'].lower()
    return {
        'code': code,
        'name': props['NOM'],
        'area': float(props['SUPERFICIE'].replace(',', '.')),
        'population': int(props['POPULATION']),
        'density': float(props['DENSITE'].replace(',', '.')),
        'parents': ['country:lu', 'country-group:ue', 'country-group:world'],
        'keys': {
            'iso': code,
        },
        'validity': {
            'start': props['CREATED'],
            'end': props['DELETED'],
        }
    }


DISTRICTS_SPARQL = '''
SELECT DISTINCT ?district ?iso ?population ?area ?geonames ?flag
                ?blazon ?logo ?site ?wikipedia ?osm
WHERE {
  ?district wdt:P31 wd:Q216888;
           wdt:P300 ?iso.
  OPTIONAL {?district wdt:P1566 ?geonames.}
  OPTIONAL {?district wdt:P2046 ?area.}
  OPTIONAL {?district wdt:P1082 ?population.}
  OPTIONAL {?district wdt:P41 ?flag.}
  OPTIONAL {?district wdt:P94 ?blazon.}
  OPTIONAL {?district wdt:P154 ?logo.}
  OPTIONAL {?district wdt:P856 ?site.}
  OPTIONAL {?district wdt:P402 ?osm.}

  OPTIONAL {
    ?wikipedia schema:about ?district;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }
}
'''


@district.postprocessor()
def fetch_districts_data_from_wikidata(db):
    info('Fetching luxembourguish districts wikidata metadata')
    results = wiki.data_sparql_query(DISTRICTS_SPARQL)
    results = wiki.data_reduce_result(results, 'district')
    for row in progress(results):
        iso = row['iso'].lower()
        db.update_zone(district.id, iso, db.TODAY,  ops={
            '$set': {k: v for k, v in {
                'wikidata': wiki.data_uri_to_id(row['district']),
                'wikipedia': wiki.wikipedia_url_to_id(row['wikipedia']),
                'dbpedia': wiki.wikipedia_to_dbpedia(row['wikipedia']),
                'website': row.get('site'),
                'flag': wiki.media_url_to_path(row.get('flag')),
                'blazon': wiki.media_url_to_path(row.get('blazon')),
                'logo': wiki.media_url_to_path(row.get('logo')),
                'area': float(row['area']),
                'population': int(row['population']),
                'keys.osm': row.get('osm'),
                'keys.geonames': row['geonames'],
            }.items() if v is not None}
        })


@canton.extractor(GEOJSON, layer='cantons')  # NOQA
def extract_lu_canton(db, polygon):
    '''
    Extract a luxembourgish canton informations from a MultiPolygon.
    Based on data from:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
    '''
    props = polygon['properties']
    code = props['ISO'].lower()

    parents = {'country:lu', 'country-group:ue', 'country-group:world'}

    district = db.find_one({'level': 'lu:district', 'name': props['DISTRICT']})

    if district:
        parents.add(district['_id'])
        parents |= set(district['parents'])

    return {
        'code': code,
        'name': props['NOM'],
        'area': float(props['SUPERFICIE'].replace(',', '.')),
        'population': int(props['POPULATION']),
        'density': float(props['DENSITE'].replace(',', '.')),
        'parents': list(parents),
        'keys': {
            'iso': code,
        }
    }


CANTONS_SPARQL = '''
SELECT DISTINCT ?canton ?iso ?population ?area ?geonames ?flag
                ?blazon ?logo ?site ?wikipedia ?osm
WHERE {
  ?canton wdt:P31 wd:Q1146429;
           wdt:P300 ?iso.
  OPTIONAL {?canton wdt:P1566 ?geonames.}
  OPTIONAL {?canton wdt:P2046 ?area.}
  OPTIONAL {?canton wdt:P1082 ?population.}
  OPTIONAL {?canton wdt:P41 ?flag.}
  OPTIONAL {?canton wdt:P94 ?blazon.}
  OPTIONAL {?canton wdt:P154 ?logo.}
  OPTIONAL {?canton wdt:P856 ?site.}
  OPTIONAL {?canton wdt:P402 ?osm.}
  OPTIONAL {
    ?wikipedia schema:about ?canton;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }
}
'''


@canton.postprocessor()
def fetch_cantons_data_from_wikidata(db):
    info('Fetching luxembourguish cantons wikidata metadata')
    results = wiki.data_sparql_query(CANTONS_SPARQL)
    results = wiki.data_reduce_result(results, 'canton')
    for row in progress(results):
        iso = row['iso'].lower()
        db.update_zone(canton.id, iso, db.TODAY,  ops={
            '$set': {k: v for k, v in {
                'wikidata': wiki.data_uri_to_id(row['canton']),
                'wikipedia': wiki.wikipedia_url_to_id(row['wikipedia']),
                'dbpedia': wiki.wikipedia_to_dbpedia(row['wikipedia']),
                'website': row.get('site'),
                'flag': wiki.media_url_to_path(row.get('flag')),
                'blazon': wiki.media_url_to_path(row.get('blazon')),
                'logo': wiki.media_url_to_path(row.get('logo')),
                'area': float(row['area']),
                'population': int(row['population']),
                'keys.osm': row.get('osm'),
                'keys.geonames': row['geonames'],
            }.items() if v is not None}
        })

@commune.extractor(GEOJSON, layer='communes')  # NOQA
def extract_lu_commune(db, polygon):
    '''
    Extract a luxembourgish commune informations from a MultiPolygon.
    Based on data from:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
    '''
    props = polygon['properties']
    code = props['LAU2']

    parents = {'country:lu', 'country-group:ue', 'country-group:world'}

    canton = db.find_one({'level': 'lu:canton', 'name': props['CANTON']})

    if canton:
        parents.add(canton['_id'])
        parents |= set(canton['parents'])

    return {
        'code': code,
        'name': props['COMMUNE'],
        'parents': list(parents),
        'keys': {
            'lau2': code,
        }
    }


COMMUNES_SPARQL = '''
SELECT DISTINCT ?commune ?communeLabel ?lau ?population ?area ?geonames ?flag
                ?blazon ?logo ?site ?wikipedia ?osm
WHERE {
  ?commune wdt:P31 wd:Q2919801;
           wdt:P782 ?lau.
  OPTIONAL {?commune wdt:P1566 ?geonames.}
  OPTIONAL {?commune wdt:P2046 ?area.}
  OPTIONAL {?commune wdt:P1082 ?population.}
  OPTIONAL {?commune wdt:P41 ?flag.}
  OPTIONAL {?commune wdt:P94 ?blazon.}
  OPTIONAL {?commune wdt:P154 ?logo.}
  OPTIONAL {?commune wdt:P856 ?site.}
  OPTIONAL {?commune wdt:P402 ?osm.}

  OPTIONAL {
    ?wikipedia schema:about ?commune;
               schema:inLanguage 'fr';
               schema:isPartOf <https://fr.wikipedia.org/>.
  }

  SERVICE wikibase:label { bd:serviceParam wikibase:language 'fr'. }
}
'''


@commune.postprocessor()
def fetch_communes_data_from_wikidata(db):
    info('Fetching luxembourguish communes wikidata metadata')
    results = wiki.data_sparql_query(COMMUNES_SPARQL)
    results = wiki.data_reduce_result(results, 'commune')
    for row in progress(results):
        lau = row['lau'].lower()
        db.update_zone(commune.id, lau, db.TODAY,  ops={
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
                'keys.geonames': row.get('geonames'),
            }.items() if v is not None}
        })
