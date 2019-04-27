from ..dbpedia import extract_uri
from ..tools import convert_from

from .histo import (
    retrieve_current_departement, retrieve_current_collectivite
)
from .model import canton, collectivite, departement, region, arrondissement, commune, iris


@arrondissement.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/arrondissements-20131220-100m-shp.zip')  # NOQA
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
        'dbpedia': extract_uri(props['wikipedia']),
        'parents': ['country:fr', 'country-group:ue', 'country-group:world'],
        'keys': {
            'insee': code,
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
    zone = db.fetch_zone(departement.id, code_insee, after='2016-01-01')
    if not zone:
        return
    zone['keys']['nuts3'] = props['nuts3']
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    return zone


@region.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20140306-100m-shp.zip')  # NOQA
def extract_2014_french_region(db, polygon):
    '''
    Extract a french region informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/contours-des-regions-francaises-sur-openstreetmap/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(region.id, props['code_insee'], before='2014-12-31', after='2014-01-01')
    if not zone:
        return
    zone['keys']['nuts2'] = props['nuts2']
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    return zone


@region.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20161121-shp.zip', simplify=0.01)  # NOQA
def extract_2016_french_region(db, polygon):
    '''
    Extract new french region informations from a MultiPolygon.
    Based on data from:
    https://www.data.gouv.fr/fr/datasets/projet-de-redecoupages-des-regions/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(region.id, props['code_insee'], before='2016-12-31', after='2016-01-01')
    if not zone:
        return
    zone['keys']['nuts2'] = props['nuts2']
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    return zone


@region.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20170102-shp.zip', simplify=0.01)  # NOQA
def extract_2017_french_region(db, polygon):
    '''
    Extract new french region informations from a MultiPolygon.
    Based on data from:
    https://www.data.gouv.fr/fr/datasets/projet-de-redecoupages-des-regions/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(region.id, props['insee'], before='2017-12-31', after='2017-01-01')
    if not zone:
        return
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    zone['wikidata'] = convert_from(props['wikidata'], 'latin-1')
    return zone


@region.extractor('http://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2018/geojson/regions-100m.geojson.gz',
                  filename='regions-100m-2018.geojson.gz')
def extract_2018_french_regions(db, polygon):
    props = polygon['properties']
    return {
        'code': props['code'],
        'name': props['nom'],
        'parents': ['country:fr', 'country-group:ue', 'country-group:world'],
    }


@region.extractor('http://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2019/geojson/regions-100m.geojson.gz',
                  filename='regions-100m-2019.geojson.gz')
def extract_2019_french_regions(db, polygon):
    props = polygon['properties']
    return {
        'code': props['code'],
        'name': props['nom'],
        'parents': ['country:fr', 'country-group:ue', 'country-group:world'],
    }


@commune.extractor('http://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2019/geojson/communes-100m.geojson.gz',
                   filename='communes-100m-2019.geojson.gz')
def extract_2019_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    parents = ['country:fr', 'country-group:ue', 'country-group:world']
    # region = retrieve_current_collectivite
    # departement = retrieve_current_departement(db, props['departement'])
    # if departement:
    #     parents.append(departement['_id'])
    # wikipedia = convert_from(props['wikipedia'], 'latin-1')
    return {
        'code': props['code'],
        'name': props['nom'],
        'parents': parents,
        # 'keys': {
        #     'ref': code,
        #     'jorf': props['jorf']
        # }
    }


@commune.extractor('http://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2018/geojson/communes-100m.geojson.gz',
                   filename='communes-100m-2018.geojson.gz')
def extract_2018_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    parents = ['country:fr', 'country-group:ue', 'country-group:world']
    # region = retrieve_current_collectivite
    # departement = retrieve_current_departement(db, props['departement'])
    # if departement:
    #     parents.append(departement['_id'])
    # wikipedia = convert_from(props['wikipedia'], 'latin-1')
    return {
        'code': props['code'],
        'name': props['nom'],
        'parents': parents,
        # 'keys': {
        #     'ref': code,
        #     'jorf': props['jorf']
        # }
    }


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20170111-shp.zip', simplify=0.0005)  # NOQA
def extract_2017_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(commune.id, props['insee'], '9999-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_ha'])
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20160119-shp.zip', simplify=0.0005)  # NOQA
def extract_2016_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(commune.id, props['insee'], '2016-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_ha'])
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20150101-100m-shp.zip')  # NOQA
def extract_2015_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(commune.id, props['insee'], '2015-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_m2']) / 10**6
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20131220-100m-shp.zip')  # NOQA
def extract_2014_french_commune(db, polygon):
    '''
    Extract a french town informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(commune.id, props['insee'], '2014-12-31')
    if not zone:
        return
    zone['area'] = int(props['surf_m2']) / 10**6
    zone['wikipedia'] = convert_from(props['wikipedia'], 'latin-1')
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
    return zone


@commune.extractor('http://osm13.openstreetmap.fr/~cquest/openfla/export/arrondissements-municipaux-20160128-shp.zip')  # NOQA
def extract_french_arrondissements(db, polygon):
    '''
    Extract a french arrondissements informations from a MultiPolygon.
    Based on data from:
    http://www.data.gouv.fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    '''
    props = polygon['properties']
    zone = db.fetch_zone(commune.id, props['insee'])
    print('zone', zone)
    zone['wikipedia'] = props['wikipedia']
    zone['dbpedia'] = extract_uri(zone['wikipedia'])
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
    wikipedia = convert_from(props['wikipedia'], 'latin-1')
    return {
        'code': code,
        'name': convert_from(props['nom'], 'latin-1'),
        'population': props['population'],
        'wikipedia': wikipedia,
        'dbpedia': extract_uri(wikipedia),
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
    commune = db.fetch_zone('fr:commune', props['DEPCOM'], after='2013-01-01')
    if commune:
        parents.append(commune['_id'])
    name = props['NOM_IRIS'].title()
    return {
        'code': code,
        'name': name,
        'parents': parents,
        '_type': props['TYP_IRIS'],
        'keys': {
            'iris': code
        },

    }
