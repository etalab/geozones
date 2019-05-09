from ..tools import success, progress
from .model import departement, epci, commune, collectivite, region

DEBUT = '1943-01-01'
GEOHISTO_EOT = '9999-12-31'
GEOHISTO_BASE = 'https://github.com/etalab/geohisto/raw/master/'


def geohisto_datetime(datetime):
    '''Sanitize a geohisto datetime into a simple ISO date'''
    date = datetime.split(' ')[0]
    return None if date == GEOHISTO_EOT else date


def geohisto_list(row, field):
    value = row.get(field, '').strip()
    if not value:
        return []
    return value.split(';')


def histo_id(id):
    return id.replace('COM-', 'fr:commune:')


def _ancestors(row, pivots):
    if 'predecesseur' in row:
        return [histo_id(row['predecesseur'])]
    ids = row.get('membres', [])
    ancestors = (pivots.get(id, {}).get('predecesseur') for id in ids)
    return [
        histo_id(ancestor) for ancestor in ancestors
        if ancestor and ancestor[:4] not in ('COMA', 'COMD')
    ]


def _successors(row, pivots):
    successor = row.get('successeur')
    if not successor:
        return []
    if successor.startswith('COM-'):
        return [histo_id(successor)]
    comp = pivots.get(successor)
    return [histo_id(comp['pole'])] if comp else []


@commune.preprocessor('https://github.com/etalab/decoupage-administratif/releases/download/v0.5.0/historique-communes.json')
def load_communes_history(db, data):
    '''Load french communes from history'''
    rows = list(data)
    pivots = {row['id']: row for row in rows if row['type'] in ('COMP', 'COMA', 'COMD')}

    count = db.safe_bulk_insert({
        '_id': 'fr:commune:{0}@{1}'.format(row['code'].lower(), row.get('dateDebut', DEBUT)),
        'code': row['code'].lower(),
        'level': commune.id,
        'name': row['nom'],
        'parents': [
            'country:fr', 'country-group:ue', 'country-group:world'
        ],
        'keys': {
            'insee': row['code'].lower(),
            'histo': row['id'],
        },
        'successors': _successors(row, pivots),
        'ancestors': _ancestors(row, pivots),
        'validity': {
            'start': row.get('dateDebut'),
            'end': row.get('dateFin'),
        }
    } for row in progress(rows, 'Loading french communes history') if row['type'] == 'COM')
    success('Loaded {} french communes(s)', count)


@departement.preprocessor(GEOHISTO_BASE + 'exports/departements/departements.csv')
def load_departements(db, data):
    '''Load departements from GeoHisto.'''
    count = db.safe_bulk_insert({
        '_id': row['id'].lower(),
        'code': row['insee_code'].lower(),
        'level': departement.id,
        'name': row['name'],
        'parents': (['country:fr', 'country-group:ue', 'country-group:world'] +
                    row['parents'].split(';')),
        'keys': {
            'insee': row['insee_code'].lower(),
        },
        'successors': geohisto_list(row, 'successors'),
        'ancestors': geohisto_list(row, 'ancestors'),
        'validity': {
            'start': geohisto_datetime(row['start_datetime']),
            'end': geohisto_datetime(row['end_datetime']),
        }
    } for row in progress(data, 'Loading french departements history'))
    success('Loaded {} french departement(s)', count)


@collectivite.preprocessor(GEOHISTO_BASE + 'exports/collectivites/collectivites.csv')
def load_collectivites(db, data):
    '''Load collectivites from GeoHisto.'''
    count = db.safe_bulk_insert({
        '_id': '{0}:{1}@{2}'.format(collectivite.id, row['insee_code'].lower(), geohisto_datetime(row['start_datetime'])),
        'code': row['insee_code'].lower(),
        'level': collectivite.id,
        'name': row['name'],
        'parents': (['country:fr', 'country-group:ue', 'country-group:world'] +
                    row['parents'].split(';')),
        'keys': {
            'insee': row['insee_code'].lower(),
            'iso2': row['iso2'].lower(),
        },
        'successors': geohisto_list(row, 'successors'),
        'ancestors': geohisto_list(row, 'ancestors'),
        'validity': {
            'start': geohisto_datetime(row['start_datetime']),
            'end': geohisto_datetime(row['end_datetime']),
        }
    } for row in progress(data, 'Loading french oversea collectivities history'))
    success('Loaded {} french oversea collectivities', count)


@region.preprocessor(GEOHISTO_BASE + 'exports/regions/regions.csv')
def load_regions(db, data):
    '''Load regions from GeoHisto.'''
    count = db.safe_bulk_insert({
        '_id': row['id'].lower(),
        'code': row['insee_code'].lower(),
        'level': region.id,
        'name': row['name'],
        'area': int(row['surface']),
        'population': int(row['population']),
        'wikipedia': row['wikipedia'],
        'parents': [
            'country:fr', 'country-group:ue', 'country-group:world'
        ],
        'keys': {
            'insee': row['insee_code'].lower(),
            'nuts2': row['nuts_code'].lower()
        },
        'successors': geohisto_list(row, 'successors'),
        'ancestors': geohisto_list(row, 'ancestors'),
        'validity': {
            'start': geohisto_datetime(row['start_datetime']),
            'end': geohisto_datetime(row['end_datetime']),
        }
    } for row in progress(data, 'Loading french regions history'))
    success('Loaded {} french region(s)', count)


@epci.preprocessor('https://static.data.gouv.fr/resources/liste-et-historique-des-epci-a-fiscalite-propre/20190430-114320/historique-epcis.json')
def load_epcis_history(db, data):
    '''Load EPCIs history'''
    count = db.safe_bulk_insert({
        '_id': 'fr:epci:{}'.format(row['id']),
        'code': row['siren'],
        'level': epci.id,
        'name': row['nom'],
        'population': int(row['population']),
        'parents': [
            'country:fr', 'country-group:ue', 'country-group:world'
        ],
        'keys': {
            'siren': row['siren'],
        },
        'successors': row.get('successeurs', []),
        'ancestors': row.get('predecesseurs', []),
        '_towns': row['membres'],
        'validity': {
            'start': row.get('dateDebut'),
            'end': row.get('dateFin'),
        }
    } for row in progress(data, 'Loading French EPCIs history'))
    success('Loaded {} EPCI(s)', count)
