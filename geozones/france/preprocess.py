from ..tools import success, progress
from .model import departement, epci, commune, collectivite, region

DEBUT = '1942-01-01'
GEOHISTO_EOT = '9999-12-31'
GEOHISTO_BASE = 'https://github.com/etalab/geohisto/raw/master/'

def geohisto_datetime(datetime):
    '''Sanitize a geohisto datetime into a simple ISO date'''
    date = datetime.split(' ')[0]
    return None if date == GEOHISTO_EOT else date


def _link(row, field):
    value = row.get(field)
    if not value:
        return []
    return [value.replace('COM-', 'fr:commune')]



@commune.preprocessor('https://github.com/etalab/decoupage-administratif/releases/download/v0.5.0/historique-communes.json')
def load_communes_history(db, data):
    '''Load french communes from history'''
    count = db.safe_bulk_insert({
        '_id': 'fr:commune:{0}@{1}'.format(row['code'], row.get('dateDebut', DEBUT)),
        'code': row['code'],
        'level': 'fr:commune',
        'name': row['nom'],
        # 'population': int(row['population']),
        'parents': [
            'country:fr', 'country-group:ue', 'country-group:world'
        ],
        'keys': {
            'insee': row['code'],
            'histo': row['id'],
        },
        'successors': _link(row, 'successeur'),
        'ancestors': _link(row, 'predecesseur'),
        'validity': {
            'start': row.get('dateDebut'),
            'end': row.get('dateFin'),
        }
    } for row in progress(data, 'Loading french communes history') if row['type'] == 'COM')
    success('Loaded {} french communes(s)', count)


@departement.preprocessor(GEOHISTO_BASE + 'exports/departements/departements.csv')
def load_departements(db, data):
    '''Load departements from GeoHisto.'''
    count = db.safe_bulk_insert({
        '_id': row['id'],
        'code': row['insee_code'],
        'level': 'fr:departement',
        'name': row['name'],
        'parents': (['country:fr', 'country-group:ue', 'country-group:world'] +
                    row['parents'].split(';')),
        'keys': {
            'insee': row['insee_code'],
        },
        'successors': row['successors'].split(';'),
        'ancestors': row['ancestors'].split(';'),
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
        '_id': row['id'],
        'code': row['insee_code'],
        'level': 'fr:collectivite-outre-mer',
        'name': row['name'],
        'iso2': row['iso2'],
        'parents': (['country:fr', 'country-group:ue', 'country-group:world'] +
                    row['parents'].split(';')),
        'keys': {
            'insee': row['insee_code'],
        },
        'successors': row['successors'].split(';'),
        'ancestors': row['ancestors'].split(';'),
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
        '_id': row['id'],
        'code': row['insee_code'],
        'level': 'fr:region',
        'name': row['name'],
        'area': int(row['surface']),
        'population': int(row['population']),
        'wikipedia': row['wikipedia'],
        'parents': [
            'country:fr', 'country-group:ue', 'country-group:world'
        ],
        'keys': {
            'insee': row['insee_code'],
            'nuts2': row['nuts_code']
        },
        'successors': row['successors'].split(';'),
        'ancestors': row['ancestors'].split(';'),
        'validity': {
            'start': geohisto_datetime(row['start_datetime']),
            'end': geohisto_datetime(row['end_datetime']),
        }
    } for row in progress(data, 'Loading french regions history'))
    success('Loaded {} french region(s)', count)


@epci.preprocessor('https://static.data.gouv.fr/resources/liste-et-historique-des-epci-a-fiscalite-propre/20190419-175019/historique-epcis.json')
def load_epcis_history(db, data):
    '''Load EPCIs history'''
    count = db.safe_bulk_insert({
        '_id': 'fr:epci:{}'.format(row['id']),
        'code': row['siren'],
        'level': 'fr:epci',
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
